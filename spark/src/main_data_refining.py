# -*- coding: utf-8 -*-
import sys
import time
from tools import get_config as conf, utils as ut
import prepare_data as prep
import sales as sales
import model_week_mrp as mrp
import model_week_tree as mwt

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main(args):
    ######### Get Params
    print('Getting parameters...')
    params = conf.Configuration(args)
    params.pretty_print_dict()

    current_week = ut.get_current_week()
    print('Current week: {}'.format(current_week))
    print('==> Refined data will be uploaded up to this week (excluded).')

    ######### Set up Spark Session
    print('Setting up Spark Session...')
    spark_conf = SparkConf().setAll(params.list_conf)
    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    ######### Load all needed clean data
    # Todo: use the uncomment code after the fix of connection spark metastore is done
    """
    transactions_df = spark.table(params.transactions_table)\
        .where(col("month") >= str(params.first_historical_week)[:4]) # get all years data from 2015
    deliveries_df = spark.table(params.deliveries_table)\
        .where(col("month") >= str(params.first_historical_week)[:4])
    """
    transactions_df = read_parquet_table(spark, params, 'f_transaction_detail/*/')
    deliveries_df = read_parquet_table(spark, params, 'f_delivery_detail/*/')
    currency_exchange_df = read_parquet_table(spark, params, 'f_currency_exchange/')
    sku = read_parquet_table(spark, params, 'd_sku/')
    sku_h = read_parquet_table(spark, params, 'd_sku_h/')
    but = read_parquet_table(spark, params, 'd_business_unit/')
    sapb = read_parquet_table(spark, params, 'sites_attribut_0plant_branches_h/')
    gdw = read_parquet_table(spark, params, 'd_general_data_warehouse_h/')
    gdc = read_parquet_table(spark, params, 'd_general_data_customer/')
    day = read_parquet_table(spark, params, 'd_day/')
    week = read_parquet_table(spark, params, 'd_week/')

    ######### Create model_week_sales
    cur_exch_df = prep.get_current_exchange(currency_exchange_df)
    day_df = prep.get_days(day, params.first_historical_week).where(col('wee_id_week') < current_week)
    fltr_sku_df = prep.filter_sku(sku)
    fltr_sapb_df = prep.filter_sap(sapb, params.list_puch_org)

    # Get offline sales
    offline_sales_df = sales.get_offline_sales(transactions_df, day_df, week, fltr_sku_df, but, cur_exch_df, fltr_sapb_df)
    # Get online sales
    online_sales_df = sales.get_online_sales(deliveries_df, day_df, week, fltr_sku_df, but, gdc, cur_exch_df, fltr_sapb_df)

    # Create model week sales
    model_week_sales = sales.union_sales(offline_sales_df, online_sales_df)
    model_week_sales.persist()

    print('====> counting(cache) [model_week_sales] took ')
    start = time.time()
    model_week_sales_count = model_week_sales.count()
    ut.get_timer(starting_time=start)
    print('[model_week_sales] length:', model_week_sales_count)

    ######### Create model_week_tree
    weeks_df = prep.get_weeks(week, params.first_backtesting_cutoff, current_week)
    model_week_tree = mwt.get_model_week_tree(sku_h, weeks_df)\
        .cache()

    print('====> counting(cache) [model_week_tree] took ')
    start = time.time()
    model_week_tree_count = model_week_tree.count()
    ut.get_timer(starting_time=start)
    print('[model_week_tree] length:', model_week_tree_count)

    ######### Create model_week_mrp
    mrp_day_df = prep.get_days(day, params.first_historical_week).where(col('wee_id_week') <= current_week)
    smu = mrp.get_sku_mrp_update(gdw, fltr_sapb_df, fltr_sku_df)
    model_week_mrp = mrp.get_model_week_mrp(smu, mrp_day_df)
    model_week_mrp.cache()

    print('====> counting(cache) [model_week_mrp] took ')
    start = time.time()
    model_week_mrp_count = model_week_mrp.count()
    ut.get_timer(starting_time=start)
    print('[model_week_mrp] length:', model_week_mrp_count)

    ######### Reduce tables according to the models found in model_week_sales
    print('====> Reducing tables according to the models found in model_week_sales...')
    list_models_df = model_week_sales.select('model_id').drop_duplicates()
    fltr_model_week_tree = model_week_tree.join(list_models_df, on='model_id', how='inner')
    fltr_model_week_mrp = model_week_mrp.join(list_models_df, on='model_id', how='inner')

    print('[model_week_tree] (new) length:', fltr_model_week_tree.count())
    print('[model_week_mrp] (new) length:', fltr_model_week_mrp.count())

    ######### Fill missing MRP
    print('====> Filling missing MRP...')
    final_model_week_mrp = mrp.fill_missing_mrp(fltr_model_week_mrp) \
        .coalesce(int(spark.conf.get('spark.sql.shuffle.partitions'))) \
        .cache()

    print('[model_week_mrp] (final after fill missing MRP) length:', final_model_week_mrp.count())

    print('====> Spliting sales, price & turnover into 3 tables...')
    model_week_price = model_week_sales.select(['model_id', 'week_id', 'date', 'average_price'])
    model_week_turnover = model_week_sales.select(['model_id', 'week_id', 'date', 'sum_turnover'])
    model_week_sales_qty = model_week_sales.select(['model_id', 'week_id', 'date', 'sales_quantity'])

    # Todo check with Antoine if this assert is needed ??
    assert model_week_sales_qty.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert model_week_price.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert model_week_turnover.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert fltr_model_week_tree.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1
    assert final_model_week_mrp.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1

    write_result(model_week_sales_qty, params, 'model_week_sales')
    write_result(model_week_price, params, 'model_week_price')
    write_result(model_week_turnover, params, 'model_week_turnover')
    write_result(fltr_model_week_tree, params, 'model_week_tree')
    write_result(final_model_week_mrp, params, 'model_week_mrp')
    spark.stop()


def read_parquet_table(spark, params, path):
    return ut.read_parquet_s3(spark, params.bucket_clean, params.path_clean_datalake + path)


def write_result(towrite_df, params, path):
    """
      Save refined global tables
    """
    start = time.time()
    ut.write_parquet_s3(towrite_df.repartition(10), params.bucket_refined, params.path_refined_global + path)
    ut.get_timer(starting_time=start)


if __name__ == '__main__':
    main(sys.argv[1])
