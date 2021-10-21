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
    
    ## Get params
    print('Getting parameters...')
    args = parse_config.basic_parse_args()
    config_file = vars(args)['configfile']
    params = conf.Configuration(config_file)
    params.pretty_print_dict()

    current_week = ut.get_current_week()
    print('Current week: {}'.format(current_week))
    print('==> Global refined data will be uploaded up to this week (excluded).')

    ## Set up Spark Session
    print('Setting up Spark Session...')
    spark_conf = SparkConf().setAll(params.list_conf)
    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    ## Load all needed clean data
    tdt = ut.read_parquet_table(spark, params, 'f_transaction_detail/')
    dyd = ut.read_parquet_table(spark, params, 'f_delivery_detail/')
    cex = ut.read_parquet_table(spark, params, 'f_currency_exchange/')
    sku = ut.read_parquet_table(spark, params, 'd_sku/')
    sku_h = ut.read_parquet_table(spark, params, 'd_sku_h/')
    but = ut.read_parquet_table(spark, params, 'd_business_unit/')
    sapb = ut.read_parquet_table(spark, params, 'sites_attribut_0plant_branches_h/')
    gdw = ut.read_parquet_table(spark, params, 'd_general_data_warehouse_h/')
    gdc = ut.read_parquet_table(spark, params, 'd_general_data_customer/')
    day = ut.read_parquet_table(spark, params, 'd_day/')
    week = ut.read_parquet_table(spark, params, 'd_week/')
    sms = ut.read_parquet_table(spark, params, 'apo_sku_mrp_status_h/')
    zep = ut.read_parquet_table(spark, params, 'ecc_zaa_extplan/')

    ## Apply global filters
    cex = gf.filter_current_exchange(cex)
    sku = gf.filter_sku(sku)
    sku_h = gf.filter_sku(sku_h)
    day = gf.filter_days(day, params.first_historical_week, current_week)
    week = gf.filter_weeks(week, params.first_historical_week, current_week)
    sapb = gf.filter_sapb(sapb, params.list_purch_org)
    gdw = gf.filter_gdw(gdw)

    ## Create model_week_sales
    model_week_sales = sales.get_model_week_sales(tdt, dyd, day, week, sku, but, cex, sapb, gdc)
    model_week_sales.persist(StorageLevel.MEMORY_ONLY)
    print('====> counting(cache) [model_week_sales] took ')
    start = time.time()
    model_week_sales_count = model_week_sales.count()
    ut.get_timer(starting_time=start)
    print('[model_week_sales] length:', model_week_sales_count)

    ## Create model_week_tree
    model_week_tree = tree.get_model_week_tree(sku_h, week)
    model_week_tree.persist(StorageLevel.MEMORY_ONLY)
    print('====> counting(cache) [model_week_tree] took ')
    start = time.time()
    model_week_tree_count = model_week_tree.count()
    ut.get_timer(starting_time=start)
    print('[model_week_tree] length:', model_week_tree_count)

    ## Create model_week_mrp
    model_week_mrp = mrp.get_model_week_mrp(gdw, sapb, sku, day, sms, zep, week)
    model_week_mrp.persist(StorageLevel.MEMORY_ONLY)
    print('====> counting(cache) [model_week_mrp] took ')
    start = time.time()
    model_week_mrp_count = model_week_mrp.count()
    ut.get_timer(starting_time=start)
    print('[model_week_mrp] length:', model_week_mrp_count)

    ## Reduce tables according to the models found in model_week_sales
    print('====> Reducing tables according to the models found in model_week_sales...')
    l_model_id = model_week_sales.select('model_id').drop_duplicates()
    model_week_tree = model_week_tree.join(l_model_id, on='model_id', how='inner')
    model_week_mrp = model_week_mrp.join(l_model_id, on='model_id', how='inner')

    print('[model_week_tree] (new) length:', model_week_tree.count())
    print('[model_week_mrp] (new) length:', model_week_mrp.count())

    ## Split model_week_sales into 3 tables
    print('====> Splitting sales, price & turnover into 3 tables...')
    model_week_price = model_week_sales.select(['model_id', 'week_id', 'date', 'average_price'])
    model_week_turnover = model_week_sales.select(['model_id', 'week_id', 'date', 'sum_turnover'])
    model_week_sales = model_week_sales.select(['model_id', 'week_id', 'date', 'sales_quantity'])

    ## Data checks & assertions
    check.check_d_week(week, current_week)
    check.check_d_day(day, current_week)
    check.check_d_sku(sku)
    check.check_d_business_unit(but)
    check.check_sales(model_week_sales, current_week)

    assert model_week_sales.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert model_week_price.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert model_week_turnover.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert model_week_tree.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1
    assert model_week_mrp.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1

    ## Write results
    ut.write_result(model_week_sales, params, 'model_week_sales')
    ut.write_result(model_week_price, params, 'model_week_price')
    ut.write_result(model_week_turnover, params, 'model_week_turnover')
    ut.write_result(model_week_tree, params, 'model_week_tree')
    ut.write_result(model_week_mrp, params, 'model_week_mrp')


    spark.stop()