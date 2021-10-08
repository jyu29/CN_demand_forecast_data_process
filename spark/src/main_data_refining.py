# -*- coding: utf-8 -*-
import time
import tools.get_config as conf
import tools.utils as ut
import tools.date_tools as dt
import tools.parse_config as parse_config
import prepare_data as prep
import sales as sales
import model_week_mrp as mrp
import model_week_tree as mwt
import check_functions as check
#import stocks_retail
#import store_picking as sp

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == '__main__':
    args = parse_config.basic_parse_args()
    # Getting the scope of the data we need to process.
    config_file = vars(args)['configfile']
    scope = vars(args)['scope']

    print('Getting parameters...')
    params = conf.Configuration(config_file)
    params.pretty_print_dict()

    current_week = dt.get_current_week()
    print('Current week: {}'.format(current_week))
    print('==> Refined data will be uploaded up to this week (excluded).')

    ######### Set up Spark Session
    print('Setting up Spark Session...')
    spark_conf = SparkConf().setAll(params.list_conf)
    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    ######### Load all needed clean data

    tdt = spark.table(params.schema_table+'.f_transaction_detail')
    cex = spark.table(params.schema_table+'.f_currency_exchange')
    sku = spark.table(params.schema_table+'.d_sku')
    sku_h = spark.table(params.schema_table+'.d_sku_h')

#    tdt = spark.table(params.transactions_table)\
#        .where(col('month') >= str(params.first_historical_week)[:4]) # get all years data from first_historical_week
#    dyd = spark.table(params.deliveries_table)\
#        .where(col('month') >= str(params.first_historical_week)[:4])

#    params.bucket_clean
#   cex = ut.read_parquet_table(spark, params, 'f_currency_exchange/')
#   sku = ut.read_parquet_table(spark, params, 'd_sku/')
#   sku_h = ut.read_parquet_table(spark, params, 'd_sku_h/')
#   but = ut.read_parquet_table(spark, params, 'd_business_unit/')
#   sapb = ut.read_parquet_table(spark, params, 'sites_attribut_0plant_branches_h/')
#   gdw = ut.read_parquet_table(spark, params, 'd_general_data_warehouse_h/')
#   gdc = ut.read_parquet_table(spark, params, 'd_general_data_customer/')
#   day = ut.read_parquet_table(spark, params, 'd_day/')
#   week = ut.read_parquet_table(spark, params, 'd_week/')
#   dtm = ut.read_parquet_table(spark, params, 'd_sales_data_material_h/')
#   rgc = ut.read_parquet_table(spark, params, 'f_range_choice/')
#   lga = ut.read_parquet_table(spark, params, 'd_listing_assortment/')
#   sms = ut.read_parquet_table(spark, params, 'apo_sku_mrp_status_h/')
#   lps = ut.read_parquet_table(spark, params, 'd_link_purchorg_system/')
#   zep = ut.read_parquet_table(spark, params, 'ecc_zaa_extplan/')
#   stocks = spark.table(params.stocks_pict_table)


#   ######### Global filter
#   cex = prep.filter_current_exchange(cex)
#   sku = prep.filter_sku(sku)
#   sku_h = prep.filter_sku(sku_h)
#   day = prep.filter_days(day, params.first_historical_week, current_week)
#   week = prep.filter_weeks(week, params.first_historical_week, current_week)
#   sapb = prep.filter_sapb(sapb, params.list_puch_org)
#   gdw = prep.filter_gdw(gdw)


#    ######### model_week_sales
#    model_week_sales = sales.get_model_week_sales(tdt, dyd, day, week, sku, but, cex, sapb, gdc)
#    model_week_sales.persist()
#
#    print('====> counting(cache) [model_week_sales] took ')
#    start = time.time()
#    model_week_sales_count = model_week_sales.count()
#    ut.get_timer(starting_time=start)
#    print('[model_week_sales] length:', model_week_sales_count)
#

#    ######### Create model_week_tree
#    model_week_tree = mwt.get_model_week_tree(sku_h, week)
#    model_week_tree.cache()
#
#    print('====> counting(cache) [model_week_tree] took ')
#    start = time.time()
#    model_week_tree_count = model_week_tree.count()
#    ut.get_timer(starting_time=start)
#    print('[model_week_tree] length:', model_week_tree_count)
#
#
#    ######### Create model_week_mrp
#
#    model_week_mrp = mrp.main_model_week_mrp(gdw, sapb, sku, day, sms, zep, week)
#    model_week_mrp.persist()
#
#    print('====> counting(cache) [model_week_mrp] took ')
#    start = time.time()
#    model_week_mrp_count = model_week_mrp.count()
#    ut.get_timer(starting_time=start)
#    print('[model_week_mrp] length:', model_week_mrp_count)
#
#
#
#    ######### Reduce tables according to the models found in model_week_sales
#    print('====> Reducing tables according to the models found in model_week_sales...')
#    l_model_id = model_week_sales.select('model_id').drop_duplicates()
#    model_week_tree = model_week_tree.join(l_model_id, on='model_id', how='inner')
#    model_week_mrp = model_week_mrp.join(l_model_id, on='model_id', how='inner')
#
#    print('[model_week_tree] (new) length:', reduce_model_week_tree.count())
#    print('[model_week_mrp] (new) length:', reduce_model_week_mrp.count())
#
#    print('====> Spliting sales, price & turnover into 3 tables...')
#    model_week_price = model_week_sales.select(['model_id', 'week_id', 'date', 'average_price'])
#    model_week_turnover = model_week_sales.select(['model_id', 'week_id', 'date', 'sum_turnover'])
#    model_week_sales_qty = model_week_sales.select(['model_id', 'week_id', 'date', 'sales_quantity'])
#
#    assert model_week_sales_qty.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
#    assert model_week_price.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
#    assert model_week_turnover.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
#    assert reduce_model_week_tree.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1
#    assert reduce_model_week_mrp.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1
#
#    check.check_d_week(week, current_week)
#    check.check_d_day(day, current_week)
#    check.check_d_sku(sku)
#    check.check_d_business_unit(but)
#    check.check_sales(model_week_sales_qty, current_week)


#    ut.write_result(model_week_sales_qty, params, 'model_week_sales')
#    ut.write_result(model_week_price, params, 'model_week_price')
#    ut.write_result(model_week_turnover, params, 'model_week_turnover')
#    ut.write_result(reduce_model_week_tree, params, 'model_week_tree')
#    ut.write_result(reduce_model_week_mrp, params, 'model_week_mrp')

    ################################################
    ################################################
    ################################################
    ################################################
    ################################################
    ################################################
    ################################################


#   if (shortage_history_update = False )

#   else
#       delta

#   puis le reste
#   Avec jointure des stocks
#   print('====> Spliting sales, price & turnover into 3 tables...')
#   model_week_price = model_week_sales.select(['model_id', 'week_id', 'date', 'average_price'])
#   model_week_turnover = model_week_sales.select(['model_id', 'week_id', 'date', 'sum_turnover'])
#   model_week_sales_qty = model_week_sales.select(['model_id', 'week_id', 'date', 'sales_quantity'])

#   assert model_week_sales_qty.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
#   assert model_week_price.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
#   assert model_week_turnover.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
#   assert fltr_model_week_tree.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1
#   assert final_model_week_mrp.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1

#   check.check_d_week(week, current_week)
#   check.check_d_day(day, current_week)
#   check.check_d_sku(sku)
#   check.check_d_business_unit(but)
#   check.check_sales(model_week_sales_qty, current_week)

#   sales.main_sales(params, transactions_df, deliveries_df, cex, sku, sku_h, but, sapb, gdw, gdc, day, week, sms, zex, current_week)

#   active_week = dt.get_previous_week_id(current_week)
#   week_id_min = dt.get_previous_n_week(active_week, 12)
#   stocks_retail.main_stock_retail(spark, params, stocks, sku, but, dtm, rgc, day, week_id_min, active_week)


#   if not is_valid_scope:
#       print('[error] ' + scope + ' is not a valid scope')

    spark.stop()

