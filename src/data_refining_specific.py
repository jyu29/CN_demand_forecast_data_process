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

## Reconstruction function
def reconstruct_history(train_data_cutoff, actual_sales, model_info,
                        cluster_keys=['product_nature', 'family'], min_ts_len=160):
    # Create a complete TS dataframe
    max_week = train_data_cutoff.select(F.max('week_id')).collect()[0][0]

    all_model = train_data_cutoff.select('model').orderBy('model').drop_duplicates()
    all_week = actual_sales \
        .filter(actual_sales.week_id <= max_week) \
        .select('week_id') \
        .orderBy('week_id') \
        .drop_duplicates()

    complete_ts = all_model.crossJoin(all_week)

    # Add corresponding date
    complete_ts = complete_ts \
        .join(actual_sales.select(['week_id', 'date']).drop_duplicates(),
              on=['week_id'],
              how='inner')

    # Add cluster_keys info from model_info
    # /!\ drop_na because in very rare cases, the models are too old or too recent
    #     and do not have descriptions in d_sku

    cluster_info = model_info.select(['model'] + cluster_keys)

    complete_ts = complete_ts \
        .join(cluster_info, on='model', how="left") \
        .dropna(subset=cluster_keys)

    # Add active sales from train_data_cutoff
    complete_ts = complete_ts.join(train_data_cutoff,
                                   on=['model', 'week_id', 'date'],
                                   how="left")

    # Calculate the average sales per cluster and week from actual_sales
    all_sales = actual_sales \
        .join(cluster_info, on='model', how='left') \
        .dropna() \
        .groupBy(['week_id', 'date'] + cluster_keys) \
        .agg(F.mean('y').alias('mean_cluster_y'))

    # Add it to complete_ts
    complete_ts = complete_ts.join(all_sales,
                                   on=['week_id', 'date', 'product_nature', 'family'],
                                   how='left')

    # Compute the scale factor by row
    complete_ts = complete_ts \
        .withColumn('row_scale_factor', complete_ts.y / complete_ts.mean_cluster_y)

    # Compute the scale factor by model
    model_scale_factor = complete_ts \
        .groupBy('model') \
        .agg(F.mean('row_scale_factor').alias('model_scale_factor'))

    complete_ts = complete_ts.join(model_scale_factor, on='model', how='left')

    # have each model a scale factor?
    assert complete_ts.filter(complete_ts.model_scale_factor.isNull()).count() == 0

    # Compute a fake Y by row (if unknow fill by 0)
    complete_ts = complete_ts \
        .withColumn('fake_y',
                    (complete_ts.mean_cluster_y * complete_ts.model_scale_factor).cast('int'))
    complete_ts = complete_ts.fillna(0, subset=['fake_y'])

    # Calculate real age & total length of each TS
    # And estimate the implementation period: while fake y > y
    ts_start_end_date = complete_ts \
        .filter(complete_ts.y.isNotNull()) \
        .groupBy('model') \
        .agg(F.min('date').alias('start_date'), F.max('date').alias('end_date'))

    complete_ts = complete_ts.join(ts_start_end_date, on='model', how='left')

    complete_ts = complete_ts \
        .withColumn('age', (F.datediff(F.col('date'), F.col('start_date')) / 7) + 1) \
        .withColumn('length', (F.datediff(F.col('end_date'), F.col('date')) / 7) + 1) \
        .withColumn('is_y_sup', F.when(complete_ts.y.isNull(), 'false') \
                    .when(complete_ts.y > complete_ts.fake_y, 'true') \
                    .otherwise('false'))

    end_impl_period = complete_ts \
        .filter(complete_ts.is_y_sup == True) \
        .groupBy('model') \
        .agg(F.min('age').alias('end_impl_period'))

    complete_ts = complete_ts.join(end_impl_period, on='model', how='left')

    # Update y from "min_ts_len" weeks ago to the end of the implementation period
    complete_ts = complete_ts \
        .withColumn('y',
                    F.when(((complete_ts.age <= 0) & (complete_ts.length <= min_ts_len)) | \
                           ((complete_ts.age > 0) & (complete_ts.age < complete_ts.end_impl_period)),
                           complete_ts.fake_y.cast('int')) \
                    .otherwise(complete_ts.y).cast('int'))

    complete_ts = complete_ts \
        .select(['week_id', 'date', 'model', 'y']) \
        .dropna() \
        .orderBy(['week_id', 'model'])

    return complete_ts


# ----------------------------------------------------------------------------------

# Generate training data used to forecast validation & test cutoffs
def generate_cutoff_train_data(actual_sales, active_sales, model_info, only_last):
    current_cutoff = ut.get_current_week_id()

    if only_last:
        l_cutoff_week_id = [current_cutoff]
    else:
        cutoff_week_id = active_sales \
            .filter(active_sales.week_id >= first_test_cutoff) \
            .select('week_id') \
            .drop_duplicates() \
            .orderBy('week_id')

        l_cutoff_week_id = [row['week_id'] for row in cutoff_week_id.collect()] + [current_cutoff]

    # loop generate cutoffs
    for cutoff_week_id in l_cutoff_week_id:
        t0 = time.time()

        print('Generating train data for cutoff', str(cutoff_week_id))

        train_data_cutoff = active_sales.filter(active_sales.week_id < cutoff_week_id)

        # Models sold at least once before the cutoff
        model_sold = train_data_cutoff \
            .groupBy('model') \
            .agg(F.sum('y').alias('qty_sold')) \
            .filter(F.col('qty_sold') > 0) \
            .select('model')

        # Active the last week before the cutoff
        last_week = train_data_cutoff.agg(F.max('week_id').alias('last_week'))

        model_active = train_data_cutoff \
            .groupBy('model') \
            .agg(F.max('week_id').alias('last_active_week'))

        model_active = model_active \
            .join(last_week,
                  on=last_week.last_week == model_active.last_active_week,
                  how='inner') \
            .select('model')

        # Keep only sold & active models
        model_to_keep = model_active.join(model_sold, 'model', 'inner')
        train_data_cutoff = train_data_cutoff.join(model_to_keep, on='model', how='inner')

        # Reconstruct a fake history
        train_data_cutoff = reconstruct_history(train_data_cutoff, actual_sales, model_info)

        cutoff_path = 'train_data_cutoff/train_data_cutoff_{}'.format(str(cutoff_week_id))

        ut.write_parquet_s3(train_data_cutoff, s3_path_refine_specific, cutoff_path)

        t1 = time.time()
        total = t1 - t0
        print('Loop time {} {}:'.format(str(cutoff_week_id), total))


# ----------------------------------------------------------------------------------
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

assert actual_sales.count() > 0
assert active_sales.count() > 0
assert model_info.count() > 0

generate_cutoff_train_data(actual_sales, active_sales, model_info, only_last=only_last)

spark.stop()
