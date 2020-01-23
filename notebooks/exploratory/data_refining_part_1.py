from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer

import pyspark.sql.functions as F
import sys

def read_parquet_s3(app, s3_path):
    
    df = app.read.parquet(s3_path)
    path_sinature = ">> Parquet file read from " + s3_path
        
    return df


def write_parquet_s3(spark_df, bucket, file_path):
    """Writing spark Dataframe into s3 as a parquet file.
    
    Parameters:
    spark_df (pyspark.sql.dataframe): the spark Dataframe.
    bucket (string): the s3 bucket name.
    file_path (string) : the table name or directory.
    
    Returns:
    """
    s3_path = 's3://{}/{}'.format(bucket, file_path)
    spark_df.write.parquet(s3_path, mode="overwrite")

first_week_id = 201501
purch_org = 'Z001'
sales_org = 'Z002'
bucket = 's3://fcst-clean-dev/'


tdt = read_parquet_s3(spark, bucket + 'f_transaction_detail/*/')
dyd = read_parquet_s3(spark, bucket + 'f_delivery_detail/*/')

sku = read_parquet_s3(spark, bucket + 'd_sku/')
bu = read_parquet_s3(spark, bucket + 'd_business_unit/')

sapb = read_parquet_s3(spark, bucket + 'sites_attribut_0plant_branches_h/')
sdm = read_parquet_s3(spark, bucket + 'd_sales_data_material_h/')

day = read_parquet_s3(spark, bucket + 'd_day/')
week = read_parquet_s3(spark, bucket + 'd_week/')


actual_sales_offline = tdt \
    .join(day,
          on=F.to_date(tdt.tdt_date_to_ordered, 'yyyy-MM-dd') == day.day_id_day,
          how='inner') \
    .join(week, 
          on=day.wee_id_week == week.wee_id_week, 
          how='inner') \
    .join(sku,
          on=tdt.sku_idr_sku == sku.sku_idr_sku, 
          how='inner') \
    .join(bu, 
          on=tdt.but_idr_business_unit == bu.but_idr_business_unit, 
          how='inner') \
    .join(sapb,
          on=bu.but_num_business_unit.cast('string') == \
             F.regexp_replace(sapb.plant_id, '^0*|\s',''),
          how='inner') \
    .filter(tdt.the_to_type == 'offline') \
    .filter(week.wee_id_week >= first_week_id) \
    .filter(~sku.unv_num_univers.isin([0, 14, 89, 90])) \
    .filter(sku.mdl_num_model_r3.isNotNull()) \
    .filter(sapb.purch_org == purch_org) \
    .filter(sapb.sapsrc == 'PRT') \
    .filter(F.current_timestamp().between(sapb.date_begin, sapb.date_end)) \
    .select(week.wee_id_week.cast('int').alias('week_id'),
            week.day_first_day_week.alias('date'),
            sku.mdl_num_model_r3.alias('model'),
            tdt.f_qty_item)

actual_sales_online = dyd \
    .join(day,
          on=F.to_date(dyd.tdt_date_to_ordered, 'yyyy-MM-dd') == day.day_id_day,
          how='inner') \
    .join(week, 
          on=day.wee_id_week == week.wee_id_week, 
          how='inner') \
    .join(sku, 
          on=dyd.sku_idr_sku == sku.sku_idr_sku, 
          how='inner') \
    .join(bu, 
          on=dyd.but_idr_business_unit_economical == bu.but_idr_business_unit, 
          how='inner') \
    .join(sapb,
          on=bu.but_num_business_unit.cast('string') == \
             F.regexp_replace(sapb.plant_id, '^0*|\s',''),
          how='inner') \
    .filter(dyd.the_to_type == 'online') \
    .filter(week.wee_id_week >= first_week_id) \
    .filter(~sku.unv_num_univers.isin([0, 14, 89, 90])) \
    .filter(sku.mdl_num_model_r3.isNotNull()) \
    .filter(sapb.purch_org == purch_org) \
    .filter(sapb.sapsrc == 'PRT') \
    .filter(F.current_timestamp().between(sapb.date_begin, sapb.date_end)) \
    .select(week.wee_id_week.cast('int').alias('week_id'),
            week.day_first_day_week.alias('date'),
            sku.mdl_num_model_r3.alias('model'),
            dyd.f_qty_item)

actual_sales = actual_sales_offline.union(actual_sales_online) \
    .groupby(['week_id', 'date', 'model']) \
    .agg(F.sum('f_qty_item').alias('y')) \
    .filter(F.col('y') > 0) \
    .repartition(200)

actual_sales.cache()


lifestage_update = sdm \
    .join(sku, 
          on=F.regexp_replace(sdm.material_id, '^0*|\s','') == \
             sku.mdl_num_model_r3.cast('string'),
          how='inner') \
    .filter(sdm.sales_org == sales_org) \
    .filter(sdm.sap_source == 'PRT') \
    .filter(sdm.lifestage != '') \
    .filter(sdm.distrib_channel == '02') \
    .filter(sku.mdl_num_model_r3.isNotNull()) \
    .filter(~sku.unv_num_univers.isin([0, 14, 89, 90])) \
    .filter(F.current_timestamp().between(sku.sku_date_begin, sku.sku_date_end)) \
    .withColumn("date_end",
                F.when(sdm.date_end == '2999-12-31',
                       F.to_date(F.lit('2100-12-31'), 'yyyy-MM-dd')) \
                       .otherwise(sdm.date_end)) \
    .select(sku.mdl_num_model_r3.alias('model'), 
            sdm.date_begin,
            "date_end",
            sdm.lifestage.cast('int').alias('lifestage')) \
    .drop_duplicates() \
    .repartition(200)

lifestage_update.cache()


model_info = sku \
    .filter(sku.mdl_num_model_r3.isNotNull()) \
    .filter(~sku.unv_num_univers.isin([0, 14, 89, 90])) \
    .filter(F.current_timestamp().between(sku.sku_date_begin, sku.sku_date_end)) \
    .select(sku.mdl_num_model_r3.alias('model'),
            sku.mdl_label.alias('model_label'),
            sku.fam_num_family.alias('family'),
            sku.family_label.alias('family_label'),
            sku.sdp_num_sub_department.alias('sub_department'),
            sku.sdp_label.alias('sub_department_label'),
            sku.dpt_num_department.alias('department'),
            sku.unv_label.alias('department_label'),
            sku.unv_num_univers.alias('univers'),
            sku.mdl_label.alias('univers_label'),
            sku.pnt_num_product_nature.alias('product_nature'),
            sku.product_nature_label.alias('product_nature_label'),
            sku.category_label.alias('category_label')) \
    .drop_duplicates() \
    .repartition(200)

model_info.cache()


max_week_id = actual_sales.select(F.max('week_id')).collect()[0][0]

actual_sales = actual_sales \
    .filter(actual_sales.week_id < max_week_id) \
    .orderBy('model', 'date')


lifestage_update = lifestage_update.join(actual_sales.select('model').drop_duplicates(), 
                                         on='model', how='inner')



# Calculates all possible date/model combinations associated with a life stage update
min_date = lifestage_update.select(F.min('date_begin')).collect()[0][0]

all_lifestage_date = actual_sales \
    .filter(actual_sales.date >= min_date) \
    .select('date') \
    .drop_duplicates() \
    .orderBy('date')

all_lifestage_model = lifestage_update.select('model').drop_duplicates().orderBy('model')

date_model = all_lifestage_date.crossJoin(all_lifestage_model)

# Calculate lifestage by date
model_lifestage = date_model.join(lifestage_update, on='model', how='left')
model_lifestage = model_lifestage \
    .filter((model_lifestage.date >= model_lifestage.date_begin) &
            (model_lifestage.date <= model_lifestage.date_end)) \
    .drop('date_begin', 'date_end')

# The previous filter removes combinations that do not match the update dates.
# But sometimes the update dates do not cover all periods, 
# which causes some dates to disappear, even during the model's activity periods.
# To avoid this problem, we must merge again with all combinations to be sure 
# not to lose anything.
model_lifestage = date_model.join(model_lifestage, on=['date', 'model'], how='left')



model_lifestage = model_lifestage \
    .groupby(['date', 'model']) \
    .agg(F.min('lifestage').alias('lifestage'))

# This is a ffil by group in pyspark ==> OMG
window = Window.partitionBy('model')\
               .orderBy('date')\
               .rowsBetween(-sys.maxsize, 0)

ffilled_lifestage = F.last(model_lifestage['lifestage'], ignorenulls=True).over(window)

model_lifestage = model_lifestage.withColumn('lifestage', ffilled_lifestage)


model_lifestage = model_lifestage \
    .withColumn('lifestage_shift', 
                F.lag(model_lifestage['lifestage']) \
                      .over(Window.partitionBy("model").orderBy(F.desc('date'))))

model_lifestage = model_lifestage \
    .withColumn('diff_shift', model_lifestage['lifestage'] - \
                              model_lifestage['lifestage_shift'])

df_cut_date = model_lifestage.filter(model_lifestage.diff_shift > 0)

df_cut_date = df_cut_date \
    .groupBy('model') \
    .agg(F.max('date').alias('cut_date'))

model_lifestage = model_lifestage.join(df_cut_date, on=['model'], how='left')

# if no cut_date, fill by an old one
model_lifestage = model_lifestage \
    .withColumn('cut_date', F.when(F.col('cut_date').isNull(),
                                   F.to_date(F.lit('1993-04-15'), 'yyyy-MM-dd')) \
                                   .otherwise(F.col('cut_date')))

model_lifestage = model_lifestage \
    .filter(model_lifestage.date >= model_lifestage.cut_date) \
    .select(['date', 'model', 'lifestage'])


# Calculates all possible date/model combinations from actual sales
all_sales_model = actual_sales.select('model').orderBy('model').drop_duplicates()
all_sales_date = actual_sales.select('date').orderBy('date').drop_duplicates()

date_model = all_sales_model.crossJoin(all_sales_date)

# Add corresponding week id
date_model = date_model.join(actual_sales.select(['date', 'week_id']).drop_duplicates(), 
                             on=['date'], how='inner')

# Add actual sales
complete_ts = date_model.join(actual_sales, on=['date', 'model', 'week_id'], how='left')
complete_ts = complete_ts.select(actual_sales.columns)

# Fill NaN (no sales recorded) by 0
complete_ts = complete_ts.fillna(0, subset=['y'])


complete_ts = complete_ts.join(model_lifestage, ['date', 'model'], how='left')



def add_column_index(df, col_name): 
    new_schema = StructType(df.schema.fields + [StructField(col_name, LongType(), False),])
    return df.rdd.zipWithIndex().map(lambda row: row[0] + (row[1], )).toDF(schema=new_schema)
# find models respecting the first condition
w = Window.partitionBy('model').orderBy('date')

first_lifestage = complete_ts.filter(complete_ts.lifestage.isNotNull()) \
                             .withColumn('rn', F.row_number().over(w))

first_lifestage = first_lifestage.filter(first_lifestage.rn == 1).drop('rn')


first_lifestage = first_lifestage \
    .filter(first_lifestage.lifestage == 1) \
    .select(first_lifestage.model, 
            first_lifestage.date.alias('first_lifestage_date'))

# Create the mask (rows to be completed) for theses models
complete_ts = add_column_index(complete_ts, 'idx') # save original indexes
complete_ts.cache()

mask = complete_ts

# keep only models respecting the first condition
mask = mask.join(first_lifestage, on='model', how='inner')

# Look only before the first historized lifestage date
mask = mask.filter(mask.date <= mask.first_lifestage_date)

w = Window.partitionBy('model').orderBy(F.desc('date'))

mask = mask \
    .withColumn('cumsum_y', F.sum('y').over(w)) \
    .withColumn('lag_cumsum_y', F.lag('cumsum_y').over(w)) \
    .fillna(0, subset=['lag_cumsum_y']) \
    .withColumn('is_active', F.col('cumsum_y') > F.col('lag_cumsum_y'))

ts_start_date = mask \
    .filter(mask.is_active == False) \
    .withColumn('rn', F.row_number().over(w)) \
    .filter(F.col('rn') == 1) \
    .select('model', F.col('date').alias('start_date'))

mask = mask.join(ts_start_date, on='model', how='left')

# Case model start date unknown (older than first week recorded here)
# ==> fill by an old date
mask = mask \
    .withColumn('start_date', F.when(F.col('start_date').isNull(),
                                     F.to_date(F.lit('1993-04-15'), 'yyyy-MM-dd')) \
                                     .otherwise(F.col('start_date'))) \
    .withColumn('is_model_start', F.col('date') > F.col('start_date')) \
    .withColumn('to_fill', F.col('is_active') & \
                           F.col('is_model_start') & \
                           F.col('lifestage').isNull())

mask = mask.filter(mask.to_fill == True).select(['idx', 'to_fill'])

# Fill the eligible rows under all conditions
complete_ts = complete_ts.join(mask, on='idx', how='left')
complete_ts = complete_ts \
    .withColumn('lifestage', 
                F.when(F.col('to_fill') == True, F.lit(1)).otherwise(F.col('lifestage')))

complete_ts = complete_ts.select(['week_id', 'date', 'model', 'y', 'lifestage'])


w = Window.partitionBy('model').orderBy('date')

model_start_date = actual_sales.withColumn('rn', F.row_number().over(w))

model_start_date = model_start_date \
    .filter(model_start_date.rn == 1) \
    .drop('rn', 'week_id', 'y') \
    .select(F.col("model"), F.col("date").alias("first_date"))


active_sales = complete_ts \
    .filter(complete_ts.lifestage == 1) \
    .join(model_start_date, on='model', how='inner') \
    .filter(complete_ts.date >= model_start_date.first_date) \
    .drop('lifestage', 'first_date')


model_info = model_info \
    .withColumn('category_label', 
                F.when(model_info.category_label == 'SOUS RAYON POUB', F.lit(None)) \
                .otherwise(model_info.category_label)) \
    .fillna('UNKNOWN')

# Due to a discrepant seasonal behaviour between LOW SOCKS and HIGH SOCKS, we chose to split
# the product nature 'SOCKS' into two different product natures 'LOW SOCKS' and 'HIGH SOCKS'
model_info = model_info \
    .withColumn('product_nature_label', 
                F.when((model_info.product_nature_label == 'SOCKS') & \
                       (model_info.model_label.contains(' LOW')), 
                       F.lit('LOW SOCKS')) \
                 .when((model_info.product_nature_label == 'SOCKS') & \
                       (model_info.model_label.contains(' MID')), 
                       F.lit('MID SOCKS')) \
                 .when((model_info.product_nature_label == 'SOCKS') & \
                       (model_info.model_label.contains(' HIGH')), 
                       F.lit('HIGH SOCKS')) \
                 .otherwise(model_info.product_nature_label)) \
    .drop('product_nature')
    
indexer = StringIndexer(inputCol='product_nature_label', outputCol='product_nature')
model_info = indexer \
    .fit(model_info) \
    .transform(model_info) \
    .withColumn('product_nature', F.col('product_nature').cast('integer'))


# Check duplicates rows
assert active_sales.groupBy(['date', 'model']).count().select(F.max("count")).collect()[0][0] == 1
assert model_info.count() == model_info.select('model').drop_duplicates().count()


write_parquet_s3(model_info, 'fcst-refined-demand-forecast-dev', 'part_1/model_info')
write_parquet_s3(actual_sales, 'fcst-refined-demand-forecast-dev', 'part_1/actual_sales')
write_parquet_s3(active_sales, 'fcst-refined-demand-forecast-dev', 'part_1/active_sales')


spark.stop()
