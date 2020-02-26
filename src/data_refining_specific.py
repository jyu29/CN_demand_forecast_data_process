from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *

import pyspark.sql.functions as F
import sys
import time

import utils as ut

# ----------------------------------------------------------------------------------

## Spark Configs
spark = SparkSession.builder \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------------------------------------------------

## Run Configs
conf = ut.ProgramConfiguration(sys.argv[1], sys.argv[2])
only_last = eval(sys.argv[3])
s3_path_refine_global = conf.get_s3_path_refine_global()
s3_path_refine_specific = conf.get_s3_path_refine_specific()
filter_type, filter_val = conf.get_filter_type(), conf.get_filter_val()
scope = conf.get_scope()
first_test_cutoff = conf.get_first_test_cutoff()


# ----------------------------------------------------------------------------------

## Read and cache data
def read_clean_data():
    actual_sales = ut.read_parquet_s3(spark, s3_path_refine_global, 'actual_sales/')
    actual_sales.persist(StorageLevel.MEMORY_ONLY)

    active_sales = ut.read_parquet_s3(spark, s3_path_refine_global, 'active_sales/')
    active_sales.persist(StorageLevel.MEMORY_ONLY)

    model_info = ut.read_parquet_s3(spark, s3_path_refine_global, 'model_info/')
    model_info.persist(StorageLevel.MEMORY_ONLY)

    return actual_sales, active_sales, model_info


# ----------------------------------------------------------------------------------

## Filter on scope
def filter_data_scope(actual_sales, active_sales, model_info):
    print('Filtering data depending on scope...')

    if scope != 'full_scope':
        actual_sales = actual_sales \
            .join(model_info.select(model_info['model'], model_info[filter_type]),
                  on='model',
                  how='left')

        actual_sales = actual_sales \
            .filter(actual_sales[filter_type].isin(filter_val)) \
            .drop(filter_type)

        active_sales = active_sales \
            .join(model_info.select(model_info['model'], model_info[filter_type]),
                  on='model',
                  how='left')

        active_sales = active_sales \
            .filter(active_sales[filter_type].isin(filter_val)) \
            .drop(filter_type)

        active_sales.persist(StorageLevel.MEMORY_ONLY)
        actual_sales.persist(StorageLevel.MEMORY_ONLY)

    return actual_sales, active_sales


# ----------------------------------------------------------------------------------

## Reconstruction function
def reconstruct_history(train_data_cutoff, actual_sales, model_info,
                        cluster_keys=['product_nature', 'family'], min_ts_len=160):
    last_week = train_data_cutoff.agg(F.max('week_id').alias('last_week'))
    model_to_keep = train_data_cutoff.groupBy('model').agg(F.max('week_id').alias('last_active_week'))
    model_to_keep = model_to_keep.join(last_week,
                                       on=last_week.last_week == model_to_keep.last_active_week,
                                       how='inner').select(model_to_keep.model)

    train_data_cutoff = train_data_cutoff.join(model_to_keep, on='model', how='inner')

    df_date = actual_sales.select(['week_id', 'date']).drop_duplicates()
    y_not_null = train_data_cutoff.filter(train_data_cutoff.y.isNotNull())

    max_week = train_data_cutoff.select(F.max('week_id')).collect()[0][0]
    min_week = train_data_cutoff.select(F.min('week_id')).collect()[0][0]

    list_weeks = ut.find_weeks(min_week, max_week)
    list_weeks = spark.createDataFrame(list_weeks, IntegerType()).selectExpr('value as week_id')
    list_models = train_data_cutoff.select(train_data_cutoff.model).distinct()

    full = list_weeks.crossJoin(list_models)
    full_actives_sales = full.join(train_data_cutoff, ['week_id', 'model'], how='left')

    # Add cluster infos
    mdl_inf = model_info.select(['model'] + cluster_keys)

    complete_ts = full_actives_sales.join(mdl_inf, on='model', how='left').drop('date')
    complete_ts = complete_ts.join(df_date, on='week_id', how='inner')

    # Calculate the average sales per cluster and week from actual_sales
    all_sales = actual_sales.join(mdl_inf, 'model', how='left')
    all_sales = all_sales.dropna()
    join_key = ['week_id', 'date'] + cluster_keys
    all_sales = all_sales.groupBy(join_key).agg(F.mean('y').alias('mean_cluster_y'))

    # Add it to complete_ts
    complete_ts = complete_ts.join(all_sales,
                                   on=['week_id', 'date', 'product_nature', 'family'],
                                   how='left')

    # Scale factor
    complete_ts = complete_ts.withColumn('row_scale_factor',
                                         complete_ts.y / complete_ts.mean_cluster_y)

    model_scale_factor = complete_ts \
        .groupBy('model') \
        .agg(F.mean('row_scale_factor').alias('model_scale_factor'))

    complete_ts = complete_ts.join(model_scale_factor, on='model', how='left')

    assert complete_ts.where(complete_ts.model_scale_factor.isNull()).count() == 0

    # Compute fake Y
    complete_ts = complete_ts \
        .withColumn('fake_y',
                    (complete_ts.mean_cluster_y * complete_ts.model_scale_factor).cast('int'))
    complete_ts = complete_ts.fillna(0, subset=['fake_y'])

    start_end = y_not_null \
        .groupBy('model') \
        .agg(F.min('date').alias('start_date'), F.max('date').alias('end_date'))

    complete_ts = complete_ts.join(start_end, 'model', how='left')

    complete_ts = complete_ts \
        .withColumn('age', (F.datediff(F.col('date'), F.col('start_date'))) / (7) + 1) \
        .withColumn('length', (F.datediff(F.col('end_date'), F.col('date'))) / (7) + 1) \
        .withColumn('is_y_sup', F.when(complete_ts.y.isNull(), 'false') \
                    .when(complete_ts.y > complete_ts.fake_y, 'true') \
                    .otherwise('false'))

    end_impl_period = complete_ts \
        .filter(complete_ts.is_y_sup == True) \
        .select(['model', 'age']).groupBy('model') \
        .agg(F.min('age').alias('end_impl_period'))

    complete_ts = complete_ts.join(end_impl_period, on=['model'], how='left')

    complete_ts = complete_ts \
        .withColumn('y',
                    F.when(((complete_ts.age <= 0) & (complete_ts.length <= min_ts_len)) | \
                           ((complete_ts.age > 0) & (complete_ts.age < complete_ts.end_impl_period)),
                           complete_ts.fake_y.cast('int')) \
                    .otherwise(complete_ts.y).cast('int'))

    complete_ts = complete_ts \
        .select(['week_id', 'date', 'model', 'y']) \
        .dropna(subset=('week_id', 'date', 'model', 'y')) \
        .orderBy(['week_id', 'model'])

    return complete_ts


# ----------------------------------------------------------------------------------

# Generate training data used to forecast validation & test cutoffs
def generate_cutoff_train_data(actual_sales, active_sales, model_info, only_last):
    current_cutoff = ut.get_next_week_id(actual_sales.select(F.max('week_id')).collect()[0][0])

    if only_last:
        l_cutoff_week_id = [current_cutoff]
    else:
        cutoff_week_test = active_sales \
            .filter(active_sales.week_id >= first_test_cutoff) \
            .select(active_sales.week_id) \
            .drop_duplicates() \
            .orderBy('week_id')

        nRow = spark.createDataFrame([[current_cutoff]])
        iterate_week = cutoff_week_test.union(nRow)
        l_cutoff_week_id = [row.week_id for row in iterate_week.collect()]

    # loop generate cutoffs
    for cutoff_week_id in sorted(l_cutoff_week_id):
        t0 = time.time()
        print('Generating train data for cutoff', str(cutoff_week_id))

        train_data_cutoff = active_sales.filter(active_sales.week_id < cutoff_week_id)

        model_sold = train_data_cutoff.select(['model', 'y']) \
            .groupBy('model') \
            .agg(F.sum(train_data_cutoff.y).alias('qty_sold')) \
            .orderBy('model')

        model_sold = model_sold.filter(model_sold.qty_sold > 0).select('model').orderBy('model')

        last_week = train_data_cutoff.agg(F.max('week_id').alias('last_week'))

        model_active = train_data_cutoff.groupBy('model').agg(F.max('week_id').alias('last_active_week'))

        model_active = model_active \
            .join(last_week,
                  on=last_week.last_week == model_active.last_active_week,
                  how='inner') \
            .select(model_active.model)

        model_to_keep = model_active.join(model_sold, 'model', 'inner')

        train_data_cutoff = train_data_cutoff.join(model_to_keep, on='model', how='inner')

        # Reconstruct a fake history
        train_data_cutoff = reconstruct_history(train_data_cutoff, actual_sales, model_info)

        path_cutoff = '{}/train_data_cutoff/train_data_cutoff_{}'.format(scope, str(cutoff_week_id))

        ut.write_parquet_s3(train_data_cutoff, s3_path_refine_specific, path_cutoff)

        t1 = time.time()
        total = t1 - t0
        print('Loop time {} {}:'.format(str(cutoff_week_id), total))


# ----------------------------------------------------------------------------------

print('scope: ', scope)
print('filter_type: ', filter_type)
print('filter_val: ', filter_val)
print('only_last: ', only_last)

actual_sales, active_sales, model_info = read_clean_data()

print("====> Counting(cache) [actual_sales] took ")
start = time.time()
actual_sales_count = actual_sales.count()
ut.get_timer(starting_time=start)
print("Count actual_sales = {}".format(actual_sales_count))

print("====> Counting(cache) actual_sales took ")
start = time.time()
active_sales_count = active_sales.count()
ut.get_timer(starting_time=start)
print("Count active_sales = {}".format(active_sales_count))

print("====> Counting(cache) actual_sales took ")
start = time.time()
model_info_count = model_info.count()
ut.get_timer(starting_time=start)
print("Count model_info = {}".format(model_info_count))


actual_sales, active_sales = filter_data_scope(actual_sales, active_sales, model_info)

generate_cutoff_train_data(actual_sales, active_sales, model_info, only_last=only_last)

spark.stop()
