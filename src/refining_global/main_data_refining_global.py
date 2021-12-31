# -*- coding: utf-8 -*-
import time

import src.tools.utils as ut
import src.tools.get_config as conf
import src.tools.parse_config as parse_config

import generic_filter as gf
import model_week_sales as sales
import model_week_tree as tree
import model_week_mrp as mrp
import check_functions as check

from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Get params
    print('Getting parameters...')
    args = parse_config.basic_parse_args()
    config_file = vars(args)['configfile']
    params = conf.Configuration(config_file)
    params.pretty_print_dict()
    # params.pretty_print_list()

    current_week = ut.get_current_week_id()
    print('Current week: {}'.format(current_week))
    print('==> Global refined data will be uploaded up to this week (excluded).')

    # Set up Spark Session
    print('Setting up Spark Session...')
    spark_conf = SparkConf().setAll(params.list_conf)
    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Load all needed clean data
    print('Load data from clean bucket.')
    bucket_clean = params.bucket_clean
    path_clean_datalake = params.path_clean_datalake
    tdt = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'f_transaction_detail/')
    dyd = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'f_delivery_detail/')
    cex = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'f_currency_exchange/')
    sku = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_sku/')
    sku_h = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_sku_h/')
    but = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_business_unit/')
    sapb = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'sites_attribut_0plant_branches_h/')
    gdw = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_general_data_warehouse_h/')
    gdc = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_general_data_customer/')
    day = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_day/')
    week = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'd_week/')
    sms = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'apo_sku_mrp_status_h/')
    zep = ut.spark_read_parquet_s3(spark, bucket_clean, path_clean_datalake + 'ecc_zaa_extplan/')

    # Apply global filters
    print('Make global filter.')
    cex = gf.filter_current_exchange(cex)
    sku = gf.filter_sku(sku)
    sku_h = gf.filter_sku(sku_h)
    day = gf.filter_day(day, params.first_historical_week, current_week)
    week = gf.filter_week(week, params.first_historical_week, current_week)
    sapb = gf.filter_sapb(sapb, params.list_purch_org)
    gdw = gf.filter_gdw(gdw)
    dyd = gf.filter_dyd(dyd)
    channel = gf.filter_channel(but)

    # Create model_week_sales
    model_week_sales = sales.get_model_week_sales(tdt, dyd, day, week, sku, but, cex, sapb, gdc, current_week, params.black_list, channel)
    model_week_sales.persist(StorageLevel.MEMORY_ONLY)
    print('====> counting(cache) [model_week_sales] took ')
    start = time.time()
    model_week_sales_count = model_week_sales.count()
    ut.get_timer(starting_time=start)
    print('[model_week_sales] length:', model_week_sales_count)

    # Create model_week_tree
    model_week_tree = tree.get_model_week_tree(sku_h, week, params.first_backtesting_cutoff)
    model_week_tree.persist(StorageLevel.MEMORY_ONLY)
    print('====> counting(cache) [model_week_tree] took ')
    start = time.time()
    model_week_tree_count = model_week_tree.count()
    ut.get_timer(starting_time=start)
    print('[model_week_tree] length:', model_week_tree_count)

    # Create model_week_mrp
    model_week_mrp = mrp.get_model_week_mrp(gdw, sapb, sku, day, sms, zep, week, params.white_list, params.first_backtesting_cutoff)
    model_week_mrp.persist(StorageLevel.MEMORY_ONLY)
    print('====> counting(cache) [model_week_mrp] took ')
    start = time.time()
    model_week_mrp_count = model_week_mrp.count()
    ut.get_timer(starting_time=start)
    print('[model_week_mrp] length:', model_week_mrp_count)

    # Reduce tables according to the models found in model_week_sales
    print('====> Reducing tables according to the models found in model_week_sales...')
    l_model_id = model_week_sales.select('model_id').drop_duplicates()
    model_week_tree = model_week_tree.join(l_model_id, on='model_id', how='inner')
    model_week_mrp = model_week_mrp.join(l_model_id, on='model_id', how='inner')

    print('[model_week_tree] (new) length:', model_week_tree.count())
    print('[model_week_mrp] (new) length:', model_week_mrp.count())

    # Split model_week_sales into 3 tables
    print('====> Splitting sales, price & turnover into 3 tables...')
    model_week_price = model_week_sales.select(['model_id', 'week_id', 'date', 'channel', 'average_price'])
    model_week_turnover = model_week_sales.select(['model_id', 'week_id', 'date', 'channel', 'sum_turnover'])
    model_week_sales = model_week_sales.select(['model_id', 'week_id', 'date', 'channel', 'sales_quantity'])

    # Data checks & assertions
    check.check_d_sku(sku)
    check.check_d_business_unit(but)
    check.check_sales_stability(model_week_sales, current_week)
    check.check_duplicate_by_keys(model_week_sales, ['model_id', 'week_id', 'date', 'channel'])
    check.check_duplicate_by_keys(model_week_price, ['model_id', 'week_id', 'date', 'channel'])
    check.check_duplicate_by_keys(model_week_turnover, ['model_id', 'week_id', 'date', 'channel'])
    check.check_duplicate_by_keys(model_week_tree, ['model_id', 'week_id'])
    check.check_duplicate_by_keys(model_week_mrp, ['model_id', 'week_id'])

    # Write results
    bucket_refined = params.bucket_refined
    path_refined_global = params.path_refined_global
    ut.spark_write_parquet_s3(model_week_sales, bucket_refined, path_refined_global + 'model_week_sales')
    ut.spark_write_parquet_s3(model_week_price, bucket_refined, path_refined_global + 'model_week_price')
    ut.spark_write_parquet_s3(model_week_turnover, bucket_refined, path_refined_global + 'model_week_turnover')
    ut.spark_write_parquet_s3(model_week_tree, bucket_refined, path_refined_global + 'model_week_tree')
    ut.spark_write_parquet_s3(model_week_mrp, bucket_refined, path_refined_global + 'model_week_mrp')

    spark.stop()
