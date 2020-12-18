## Requirements

import sys
import time
import utils as ut
from functools import reduce
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ---------------------------------------------------------------------------------------------------------------------

## Get Params

print('Getting parameters...')
params = ut.read_yml(sys.argv[1])
ut.pretty_print_dict(params)

bucket_clean = params['buckets']['clean']
bucket_refined = params['buckets']['refined']

path_clean_datalake = params['paths']['clean_datalake']
path_refined_global = params['paths']['refined_global']

first_historical_week = params['functional_parameters']['first_historical_week']
first_backtesting_cutoff = params['functional_parameters']['first_backtesting_cutoff']
list_puch_org = params['functional_parameters']['list_puch_org']

current_week = ut.get_current_week()

print('Current week: {}'.format(current_week))
print('==> Refined data will be uploaded up to this week (excluded).')

# ---------------------------------------------------------------------------------------------------------------------

## Set up Spark Session

print('Setting up Spark Session...')

list_conf = list(params['technical_parameters']['spark_conf'].items())
spark_conf = SparkConf().setAll(list_conf)

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
spark.sparkContext.setLogLevel('ERROR') # Output only Spark's ERROR.

# ---------------------------------------------------------------------------------------------------------------------

## Load all needed clean data

tdt = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'f_transaction_detail/*/')
dyd = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'f_delivery_detail/*/')
cex = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'f_currency_exchange')

sku = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_sku/')
sku_h = read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_sku_h/')
but = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_business_unit/')

sapb = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'sites_attribut_0plant_branches_h/')
gdw = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_general_data_warehouse_h/')
gdc = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_general_data_customer/')

day = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_day/')
week = ut.read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_week/')

# ---------------------------------------------------------------------------------------------------------------------

## Create model_week_sales

# Get current CRE exchange rate
# /!\ TO DO: get a dynamic exchange rate when the right data source is identified
cer = cex \
    .filter(cex['cpt_idr_cur_price'] == 6) \
    .filter(cex['cur_idr_currency_restit'] == 32) \
    .filter(current_timestamp().between(cex['hde_effect_date'], cex['hde_end_date'])) \
    .select(cex['cur_idr_currency_base'], 
            cex['cur_idr_currency_restit'],
            cex['hde_share_price']) \
    .groupby(cex['cur_idr_currency_base'], 
             cex['cur_idr_currency_restit']) \
    .agg(mean(cex['hde_share_price']).alias('exchange_rate'))

# Get offline sales
model_week_sales_offline = tdt \
    .join(day, on=to_date(tdt['tdt_date_to_ordered'], 'yyyy-MM-dd') == day['day_id_day'], how='inner') \
    .join(week, on=day['wee_id_week'] == week['wee_id_week'], how='inner') \
    .join(sku, on=tdt['sku_idr_sku'] == sku['sku_idr_sku'], how='inner') \
    .join(but, on=tdt['but_idr_business_unit'] == but['but_idr_business_unit'], how='inner') \
    .join(cer, on=tdt['cur_idr_currency'] == cer['cur_idr_currency_base'], how='inner') \
    .join(sapb,
          on=but['but_num_business_unit'].cast('string') == regexp_replace(sapb['plant_id'], '^0*|\s', ''),
          how='inner') \
    .filter(tdt['the_to_type'] == 'offline') \
    .filter(day['wee_id_week'] >= first_historical_week) \
    .filter(day['wee_id_week'] < current_week) \
    .filter(~sku['unv_num_univers'].isin([0, 14, 89, 90])) \
    .filter(sku['mdl_num_model_r3'].isNotNull()) \
    .filter(but['but_num_typ_but'] == 7) \
    .filter(sapb['sapsrc'] == 'PRT') \
    .filter(sapb['purch_org'].isin(list_puch_org)) \
    .filter(current_timestamp().between(sapb['date_begin'], sapb['date_end'])) \
    .select(sku['mdl_num_model_r3'].alias('model_id'),
            day['wee_id_week'].cast('int').alias('week_id'),
            week['day_first_day_week'].alias('date'),
            tdt['f_qty_item'],
            tdt['f_pri_regular_sales_unit'],
            tdt['f_to_tax_in'],
            cer['exchange_rate'])

# Get online sales
model_week_sales_online = dyd \
    .join(day, on=to_date(dyd['tdt_date_to_ordered'], 'yyyy-MM-dd') == day['day_id_day'], how='inner') \
    .join(week, on=day['wee_id_week'] == week['wee_id_week'], how='inner') \
    .join(sku, on=dyd['sku_idr_sku'] == sku['sku_idr_sku'], how='inner') \
    .join(but, on=dyd['but_idr_business_unit_sender'] == but['but_idr_business_unit'], how='inner') \
    .join(gdc, on=but['but_code_international'] == concat(gdc['ean_1'], gdc['ean_2'], gdc['ean_3']), how='inner') \
    .join(cer, on=dyd['cur_idr_currency'] == cer['cur_idr_currency_base'], how='inner') \
    .join(sapb, on=gdc['plant_id'] == sapb['plant_id'], how='inner') \
    .filter(dyd['the_to_type'] == 'online') \
    .filter(dyd['tdt_type_detail'] == 'sale') \
    .filter(day['wee_id_week'] >= first_historical_week) \
    .filter(day['wee_id_week'] < current_week) \
    .filter(~sku['unv_num_univers'].isin([0, 14, 89, 90])) \
    .filter(sku['mdl_num_model_r3'].isNotNull()) \
    .filter(sapb['sapsrc'] == 'PRT') \
    .filter(sapb['purch_org'].isin(list_puch_org)) \
    .filter(current_timestamp().between(sapb['date_begin'], sapb['date_end'])) \
    .select(sku['mdl_num_model_r3'].alias('model_id'),
            day['wee_id_week'].cast('int').alias('week_id'),
            week['day_first_day_week'].alias('date'),
            dyd['f_qty_item'],
            dyd['f_tdt_pri_regular_sales_unit'].alias('f_pri_regular_sales_unit'),
            dyd['f_to_tax_in'],
            cer['exchange_rate'])

# Create model week sales
model_week_sales = model_week_sales_offline.union(model_week_sales_online) \
    .groupby(['model_id', 'week_id', 'date']) \
    .agg(sum('f_qty_item').alias('sales_quantity'),
         mean(col('f_pri_regular_sales_unit') * col('exchange_rate')).alias('average_price'),
         sum(col('f_to_tax_in') * col('exchange_rate')).alias('sum_turnover')) \
    .filter(col('sales_quantity') > 0) \
    .filter(col('average_price') > 0) \
    .filter(col('sum_turnover') > 0) \
    .orderBy('model_id', 'week_id') \
    .cache()

print('====> counting(cache) [model_week_sales] took ')
start = time.time()
model_week_sales_count = model_week_sales.count()
ut.get_timer(starting_time=start)
print('[model_week_sales] length:', model_week_sales_count)

# ---------------------------------------------------------------------------------------------------------------------

## Create model_week_tree

model_week_tree = sku_h \
    .join(week, on=week['day_first_day_week'].between(sku_h['sku_date_begin'], sku_h['sku_date_end']), how='inner') \
    .filter(sku_h['sku_num_sku_r3'].isNotNull()) \
    .filter(sku_h['mdl_num_model_r3'].isNotNull()) \
    .filter(sku_h['fam_num_family'].isNotNull()) \
    .filter(sku_h['sdp_num_sub_department'].isNotNull()) \
    .filter(sku_h['dpt_num_department'].isNotNull()) \
    .filter(sku_h['unv_num_univers'].isNotNull()) \
    .filter(sku_h['pnt_num_product_nature'].isNotNull()) \
    .filter(~sku_h['unv_num_univers'].isin([0, 14, 89, 90])) \
    .filter(week['wee_id_week'] >= first_backtesting_cutoff) \
    .filter(week['wee_id_week'] <= current_week) \
    .groupBy(week['wee_id_week'].cast('int').alias('week_id'),
             sku_h['mdl_num_model_r3'].alias('model_id')) \
    .agg(max(sku_h['fam_num_family']).alias('family_id'),
         max(sku_h['sdp_num_sub_department']).alias('sub_department_id'),
         max(sku_h['dpt_num_department']).alias('department_id'),
         max(sku_h['unv_num_univers']).alias('univers_id'),
         max(sku_h['pnt_num_product_nature']).alias('product_nature_id'),
         max(when(sku_h['mdl_label'].isNull(), 'UNKNOWN').otherwise(sku_h['mdl_label'])).alias('model_label'),
         max(sku_h['family_label']).alias('family_label'),
         max(sku_h['sdp_label']).alias('sub_department_label'),
         max(sku_h['dpt_label']).alias('department_label'),
         max(sku_h['unv_label']).alias('univers_label'),
         max(when(sku_h['product_nature_label'].isNull(), 
                  'UNDEFINED').otherwise(sku_h['product_nature_label'])).alias('product_nature_label'),
         max(sku_h['brd_label_brand']).alias('brand_label'),
         max(sku_h['brd_type_brand_libelle']).alias('brand_type')) \
    .orderBy('week_id', 'model_id') \
    .cache()

print('====> counting(cache) [model_week_tree] took ')
start = time.time()
model_week_tree_count = model_week_tree.count()
ut.get_timer(starting_time=start)
print('[model_week_tree] length:', model_week_tree_count)

# ---------------------------------------------------------------------------------------------------------------------

## Create model_week_mrp

# get sku mrp update
smu = gdw \
    .join(sapb, on=gdw['sdw_plant_id'] == sapb['plant_id'], how='inner') \
    .join(sku, on=sku['sku_num_sku_r3'] == regexp_replace(gdw['sdw_material_id'], '^0*|\s', ''), how='inner') \
    .filter(gdw['sdw_sap_source'] == 'PRT') \
    .filter(gdw['sdw_material_mrp'] != '    ') \
    .filter(sapb['sapsrc'] == 'PRT') \
    .filter(sapb['purch_org'].isin(list_puch_org)) \
    .filter(current_timestamp().between(sapb['date_begin'], sapb['date_end'])) \
    .filter(sku['mdl_num_model_r3'].isNotNull()) \
    .filter(~sku['unv_num_univers'].isin([0, 14, 89, 90])) \
    .filter(current_timestamp().between(sku['sku_date_begin'], sku['sku_date_end'])) \
    .select(gdw['date_begin'],
            gdw['date_end'],
            sku['sku_num_sku_r3'].alias('sku_id'),
            sku['mdl_num_model_r3'].alias('model_id'),
            gdw['sdw_material_mrp'].cast('int').alias('mrp')) \
    .drop_duplicates()

# calculate model week mrp
model_week_mrp = smu \
    .join(day, on=day['day_id_day'].between(smu['date_begin'], smu['date_end']), how='inner') \
    .filter(day['wee_id_week'] >= '201939') \
    .filter(day['wee_id_week'] <= current_week) \
    .groupBy(day['wee_id_week'].cast('int').alias('week_id'),
             smu['model_id']) \
    .agg(max(when(smu['mrp'].isin(2, 5), True).otherwise(False)).alias('is_mrp_active')) \
    .orderBy('model_id', 'week_id') \
    .cache()

print('====> counting(cache) [model_week_mrp] took ')
start = time.time()
model_week_mrp_count = model_week_mrp.count()
ut.get_timer(starting_time=start)
print('[model_week_mrp] length:', model_week_mrp_count)

# ---------------------------------------------------------------------------------------------------------------------

## Reduce tables according to the models found in model_week_sales

print('====> Reducing tables according to the models found in model_week_sales...')

model_week_tree = model_week_tree.join(model_week_sales.select('model_id').drop_duplicates(), 
                                       on='model_id',  
                                       how='inner')

model_week_mrp = model_week_mrp.join(model_week_sales.select('model_id').drop_duplicates(), 
                                     on='model_id',  
                                     how='inner')

print('[model_week_tree] (new) length:', model_week_tree.count())
print('[model_week_mrp] (new) length:', model_week_mrp.count())

# ---------------------------------------------------------------------------------------------------------------------

## Fill missing MRP

# MRP are available since 201939 only.  
# We have to fill weeks between 201924 and 201938 using the 201939 values.
print('====> Filling missing MRP...')

model_week_mrp_201939 = model_week_mrp.filter(model_week_mrp['week_id'] == 201939)

l_df = []
for w in range(201924, 201939):
    df = model_week_mrp_201939.withColumn('week_id', lit(w))
    l_df.append(df)
l_df.append(model_week_mrp)

def unionAll(dfs):
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

model_week_mrp = unionAll(l_df) \
    .coalesce(int(spark.conf.get('spark.sql.shuffle.partitions'))) \
    .cache()

print('[model_week_mrp] (new) length:', model_week_mrp.count())

# ---------------------------------------------------------------------------------------------------------------------

## Split sales, price & turnover into 3 tables
print('====> Spliting sales, price & turnover into 3 tables...')

model_week_price = model_week_sales.select(['model_id', 'week_id', 'date', 'average_price'])
model_week_turnover = model_week_sales.select(['model_id', 'week_id', 'date', 'sum_turnover'])
model_week_sales = model_week_sales.select(['model_id', 'week_id', 'date', 'sales_quantity'])

print('Done')

# ---------------------------------------------------------------------------------------------------------------------

## Save refined global tables

# Check duplicates rows
assert model_week_sales.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
assert model_week_price.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
assert model_week_turnover.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
assert model_week_tree.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1
assert model_week_mrp.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1

# Write
print('====> Writing table [model_week_sales]')
start = time.time()
ut.write_parquet_s3(model_week_sales, bucket_refined, path_refined_global + 'model_week_sales')
ut.get_timer(starting_time=start)

print('====> Writing table [model_week_price]')
start = time.time()
ut.write_parquet_s3(model_week_price, bucket_refined, path_refined_global + 'model_week_price')
ut.get_timer(starting_time=start)

print('====> Writing table [model_week_turnover]')
start = time.time()
ut.write_parquet_s3(model_week_turnover, bucket_refined, path_refined_global + 'model_week_turnover')
ut.get_timer(starting_time=start)

print('====> Writing table [model_week_tree]')
start = time.time()
ut.write_parquet_s3(model_week_tree, bucket_refined, path_refined_global + 'model_week_tree')
ut.get_timer(starting_time=start)

print('====> Writing table [model_week_mrp]')
start = time.time()
ut.write_parquet_s3(model_week_mrp, bucket_refined, path_refined_global + 'model_week_mrp')
ut.get_timer(starting_time=start)


spark.stop()