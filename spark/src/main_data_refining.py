# -*- coding: utf-8 -*-
import time
from tools import get_config as conf, utils as ut, date_tools as dt
import prepare_data as prep
import sales as sales
import model_week_mrp as mrp
import model_week_tree as mwt
import check_functions as check
import stocks_retail
import mag_choices as mc

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import tools.parse_config as parse_config


def main_choices_magasins(params, choices_df, week, sapb):
    sapb_df = mc.filter_sap(sapb, params.list_puch_org)
    filtered_choices_df = choices_df.join(broadcast(sapb_df),
                                 on=sapb_df.ref_plant_id.cast('int') == choices_df.plant_id.cast('int'),
                                 how='inner')
    clean_data = mc.get_clean_data(filtered_choices_df)
    limit_week = dt.get_next_n_week(dt.get_current_week(), 104)  # TODO NGA verify with Antoine
    weeks = mc.get_weeks(week, params.first_backtesting_cutoff, limit_week)
    choices_per_week = mc.get_choices_per_week(clean_data, weeks)
    choices_per_week.persist()
    write_result(choices_per_week, params, 'b_choices_per_week')
    choices_per_country_df = mc.get_mag_choices_per_country(choices_per_week)
    write_result(choices_per_country_df, params, 'stores_choices_percountry')
    global_choices_df = mc.get_global_mag_choices(choices_per_week)
    write_result(global_choices_df, params, 'global_stores_choices')


def new_mrp_process(params, asms, ecc_zaa_extplan, week, sku):
    """
    apo_sku_mrp_status_h: contains MRP status for models
    ecc_zaa_extplan: contains all models migrated to the new process of MRP
    """
    #TODO Add all active status
    list_active_mrp = [20, 80]
    mrp_status = mrp.get_mrp_status(asms)
    models = mrp.get_mrp_models(ecc_zaa_extplan)
    sku_filter = mrp.filter_sku(sku)

    clean_data = models\
        .join(mrp_status, on=["sku_num_sku_r3"], how="inner")\
        .join(sku_filter, on=['sku_num_sku_r3'], how='inner')
    weeks = mrp.get_weeks(week, params.first_backtesting_cutoff, dt.get_current_week())
    mrp_per_week = mrp.get_mrp_status_per_week(clean_data, weeks)
    final_mrp_status = mrp_per_week\
        .select("purch_org", "model_id", "week_id", "mrp_status")\
        .withColumn("is_mrp_active", col("mrp_status").isin(list_active_mrp))\
        .groupBy("model_id", "week_id")\
        .agg(max(col("is_mrp_active")).alias("is_mrp_active"))
    return final_mrp_status


def main_sales(params, transactions_df, deliveries_df, currency_exchange_df, sku, sku_h, but, sapb, gdw, gdc, day, week, asms, ecc_zaa_extplan):
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
    filled_mrp_status = mrp.fill_missing_mrp(fltr_model_week_mrp)
    # get new process of mrp
    models_with_new_mrp = new_mrp_process(params, asms, ecc_zaa_extplan, week, sku)
    models_with_old_mrp = filled_mrp_status.join(models_with_new_mrp, on=["model_id", "week_id"], how="leftanti")
    final_model_week_mrp = models_with_new_mrp\
        .union(models_with_old_mrp)\
        .orderBy('model_id', 'week_id')
    final_model_week_mrp.persist()

    print('[model_week_mrp] (final after fill missing MRP) length:', final_model_week_mrp.count())

    print('====> Spliting sales, price & turnover into 3 tables...')
    model_week_price = model_week_sales.select(['model_id', 'week_id', 'date', 'average_price'])
    model_week_turnover = model_week_sales.select(['model_id', 'week_id', 'date', 'sum_turnover'])
    model_week_sales_qty = model_week_sales.select(['model_id', 'week_id', 'date', 'sales_quantity'])

    assert model_week_sales_qty.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert model_week_price.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert model_week_turnover.groupBy(['model_id', 'week_id', 'date']).count().select(max('count')).collect()[0][0] == 1
    assert fltr_model_week_tree.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1
    assert final_model_week_mrp.groupBy(['model_id', 'week_id']).count().select(max('count')).collect()[0][0] == 1

    check.check_d_week(week, current_week)
    check.check_d_day(day, current_week)
    check.check_d_sku(sku)
    check.check_d_business_unit(but)
    check.check_sales(model_week_sales_qty, current_week)


    write_result(model_week_sales_qty, params, 'model_week_sales')
    write_result(model_week_price, params, 'model_week_price')
    write_result(model_week_turnover, params, 'model_week_turnover')
    write_result(fltr_model_week_tree, params, 'model_week_tree')
    write_result(final_model_week_mrp, params, 'model_week_mrp')


def read_parquet_table(spark, params, path):
    return ut.read_parquet_s3(spark, params.bucket_clean, params.path_clean_datalake + path)


def write_result(towrite_df, params, path):
    """
      Save refined global tables
    """
    start = time.time()
    ut.write_parquet_s3(towrite_df.repartition(10), params.bucket_refined, params.path_refined_global + path)
    ut.get_timer(starting_time=start)


def write_partitioned_result(towrite_df, params, path, partition_col):
    """
      Save refined global tables
    """
    start = time.time()
    ut.write_partitionned_parquet_s3(
        towrite_df,
        params.bucket_refined,
        params.path_refined_global + path,
        partition_col
    )
    ut.get_timer(starting_time=start)


def main_stock_retail(spark, params, stocks, sku, but, dtm, rc, day, week_id_min, active_week):
    first_day_month = dt.get_first_day_month(week_id_min)
    last_day_week = dt.get_last_day_week(active_week)
    print("Refining stocks data for weeks from " + str(week_id_min) + " to " + str(active_week))
    print("--> Processing stock data between " + str(first_day_month) + " to " + str(last_day_week))

    filtered_stocks = stocks\
        .where(col("month") >= first_day_month.strftime("%Y%m"))\
        .where(col("month") <= last_day_week.strftime("%Y%m"))

    sku_df = stocks_retail.get_sku(sku)
    but_df = stocks_retail.get_but_open_store(but)
    sapb_df = stocks_retail.filter_sap(sapb, params.list_puch_org)
    dtm_df = stocks_retail.get_assortment_grade(dtm)
    day_df = stocks_retail.get_days(day, params.first_historical_week)
    rc_df = stocks_retail.get_range_choice(rc, params.first_historical_week)
    df_stock = stocks_retail.get_retail_stock(filtered_stocks, but_df, sku_df, sapb_df)
    df_stock = stocks_retail.add_lifestage_data(df_stock, dtm_df, active_week, params.lifestage_data_first_hist_week)
    all_days_df = stocks_retail.get_all_days_bu_df(spark, df_stock, first_day_month, last_day_week)
    stock_filled = stocks_retail.fill_empty_days(df_stock, all_days_df)
    stock_week = stocks_retail.enrich_with_data(stock_filled, day_df, rc_df, week_id_min, active_week,
                                                params.lifestage_data_first_hist_week)
    refined_stock = stocks_retail.refine_stock(stock_week)
    refined_stock = stocks_retail.keep_only_assigned_stock_for_old_stocks(
        refined_stock, week_id_min, params.lifestage_data_first_hist_week, params.max_nb_soldout_weeks)
    refined_stock.persist()
    stock_by_country = stocks_retail.get_stock_avail_by_country(refined_stock)
    write_partitioned_result(
        stock_by_country.withColumn('week', stock_by_country.week_id).repartition(col('week')),
        params, 'stock_by_country', 'week')
    global_stock = stocks_retail.get_stock_avail_for_all_countries(refined_stock)
    write_partitioned_result(
        global_stock.withColumn('week', global_stock.week_id).repartition(col('week')),
        params, 'global_stock', 'week')
    refined_stock.unpersist()


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
    transactions_df = spark.table(params.transactions_table)\
        .where(col("month") >= str(params.first_historical_week)[:4]) # get all years data from 2015
    deliveries_df = spark.table(params.deliveries_table)\
        .where(col("month") >= str(params.first_historical_week)[:4])

    cex = read_parquet_table(spark, params, 'f_currency_exchange/')
    sku = read_parquet_table(spark, params, 'd_sku/')
    sku_h = read_parquet_table(spark, params, 'd_sku_h/')
    but = read_parquet_table(spark, params, 'd_business_unit/')
    sapb = read_parquet_table(spark, params, 'sites_attribut_0plant_branches_h/')
    gdw = read_parquet_table(spark, params, 'd_general_data_warehouse_h/')
    gdc = read_parquet_table(spark, params, 'd_general_data_customer/')
    day = read_parquet_table(spark, params, 'd_day/')
    week = read_parquet_table(spark, params, 'd_week/')
    dtm = read_parquet_table(spark, params, 'd_sales_data_material_h/')
    rc = read_parquet_table(spark, params, 'f_range_choice/')
    choices_df = read_parquet_table(spark, params, "d_listing_assortment/")
    sku_mrp = read_parquet_table(spark, params, "apo_sku_mrp_status_h/")
    cz_purchorg_df = read_parquet_table(spark, params, "d_link_purchorg_system/")
    mrp_migration_df = read_parquet_table(spark, params, "ecc_zaa_extplan/")
    stocks = spark.table(params.stocks_pict_table)
    is_valid_scope = False

    if "choices" in scope.lower():
        is_valid_scope = True
        main_choices_magasins(params, choices_df, week, sapb)

    if "sales" in scope.lower():
        is_valid_scope = True
        main_sales(params, transactions_df, deliveries_df, cex, sku, sku_h, but, sapb, gdw, gdc, day, week, sku_mrp, mrp_migration_df)

    if "stocks_delta" in scope.lower():
        is_valid_scope = True
        active_week = dt.get_previous_week_id(dt.get_current_week())
        week_id_min = dt.get_previous_n_week(active_week, 12)
        main_stock_retail(spark, params, stocks, sku, but, dtm, rc, day, week_id_min, active_week)

    if "stocks_full" in scope.lower():
        is_valid_scope = True
        end_week = dt.get_previous_week_id(dt.get_current_week())
        while end_week > params.lifestage_data_first_hist_week:
            start_week = dt.get_previous_n_week(end_week, 12)
            main_stock_retail(spark, params, stocks, sku, but, dtm, rc, day, start_week, end_week)
            end_week = dt.get_previous_n_week(end_week, 13)

    if "historic_stocks" in scope.lower():
        is_valid_scope = True
        end_week = params.lifestage_data_first_hist_week
        while end_week > params.first_historical_week:
            start_week = dt.get_previous_n_week(end_week, 6)
            main_stock_retail(spark, params, stocks, sku, but, dtm, rc, day, start_week, end_week)
            end_week = dt.get_previous_n_week(end_week, 7)

    if not is_valid_scope:
        print('[error] ' + scope + ' is not a valid scope')

    spark.stop()
