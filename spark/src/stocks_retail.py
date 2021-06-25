from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

import datetime
import sys


def get_days(day, week_id_min):
    """
    Get matching Day --> Week
    """
    df_day = day \
        .filter(day.wee_id_week >= week_id_min) \
        .select(day.day_id_day.alias('day'),
                day.wee_id_week.cast('int').alias('week_id'))
    return df_day


def get_range_choice(rc, week_id_min):
    """
    Get range choice Data
    """
    select_rc = rc \
        .filter(rc.wee_id_week >= week_id_min) \
        .select(rc.but_idr_business_unit,
                rc.fam_idr_family,
                rc.range_level,
                rc.wee_id_week.cast('int'))\
        .drop_duplicates()
    return select_rc


def get_but_open_store(but):
    """
    Filter on:
        - open magasin
        - Physical magasin: but_num_typ_but == 7
    """
    select_bu = but \
        .filter(but.but_closed != 1) \
        .filter(but.but_num_typ_but == 7) \
        .select(but.but_num_business_unit, but.but_idr_business_unit, but.cnt_idr_country, but.cnt_country_code)
    return select_bu


def filter_sap(sapb, list_purch_org):
    """
      get SiteAttributePlant0Branch after filtering on:
      - sapsrc=PRT: all countries except brazil
      - list_push_org: EU countries
    """
    sap = sapb\
        .filter(sapb['sapsrc'] == 'PRT') \
        .filter(sapb['purch_org'].isin(list_purch_org))\
        .filter(current_timestamp().between(sapb['date_begin'], sapb['date_end']))\
        .select(sapb.plant_id, sapb.purch_org, sapb.sales_org)
    return sap


def get_sku(sku):
    """
    Get list of models after filtering on:
        - Models with univers=0 are used for test
        - Models with univers=14, 89 or 90 are not to sell directly to clients (ateliers, equipments ..)
    """
    select_sku = sku \
        .filter(~sku.unv_num_univers.isin([0, 14, 89, 90])) \
        .filter(sku.mdl_num_model_r3.isNotNull()) \
        .select(sku.sku_idr_sku, sku.sku_num_sku_r3, sku.mdl_num_model_r3, sku.fam_idr_family) \
        .drop_duplicates()
    return select_sku


def get_assortment_grade(dtm):
    """
    TODO: view filters with Benj
    Get Assortment Grade Data
    """
    select_dtm = dtm \
        .filter(dtm.lifestage == 1) \
        .filter(dtm.distrib_channel == '02') \
        .filter(dtm.assortment_grade.isin([1, 2, 3])) \
        .select(col('material_id').cast('int'),
                col('sales_org').alias('dtm_sales_org'),
                col('assortment_grade'),
                col('date_begin'),
                col('date_end')) \
        .drop_duplicates()
    return select_dtm


def get_retail_stock(stocks, but, sku, sapb):
    """
    Enrich Stocks with SKU, BUT, SAPB data
    """
    pick_stock = stocks \
        .filter(stocks.stt_idr_stock_type.isin(['67'])) \
        .join(sku,
              on=stocks.sku_idr_sku == sku.sku_idr_sku,
              how='inner') \
        .join(broadcast(but),
              on=stocks.but_idr_business_unit == but.but_idr_business_unit,
              how='inner') \
        .join(broadcast(sapb),
              on=sapb.plant_id.cast('int') == but.but_num_business_unit.cast('int'),
              how='inner') \
        .select(but.but_num_business_unit,
                but.but_idr_business_unit,
                sapb.purch_org,
                sapb.sales_org,
                sku.sku_num_sku_r3,
                sku.mdl_num_model_r3,
                sku.fam_idr_family,
                to_date(stocks.spr_date_stock, 'yyyy-MM-dd').alias('day'),
                stocks.f_quantity.cast('int'),
                stocks.month) \
        .drop_duplicates()
    return pick_stock


def add_lifestage_data(stock, dtm, week_end, lifestage_data_first_hist_week):
    """
    In order to optimize the code, I put This function before filling the dataframe
    By joinning with lifestage data before fulling empty lines, we gain 10min of time (20min instead of 30min)
    Enrich Stocks with Lifestage Data if week_start > lifestage_data_first_hist_week
    Else: we add a empty column that will not be used later
    """
    if week_end > lifestage_data_first_hist_week:
        pick_stock = stock \
            .join(dtm,
                  on=(stock.sku_num_sku_r3.cast('int') == dtm.material_id) &
                     (stock.sales_org == dtm.dtm_sales_org) &
                     (stock.day.between(dtm.date_begin, dtm.date_end)),
                  how='inner') \
            .select(
                stock.but_num_business_unit, stock.but_idr_business_unit, stock.purch_org, stock.sales_org,
                stock.sku_num_sku_r3, stock.mdl_num_model_r3, stock.fam_idr_family, stock.day, stock.f_quantity,
                dtm.assortment_grade, stock.month)
    else:
        pick_stock = stock.withColumn('assortment_grade', lit(None).cast(IntegerType()))

    return pick_stock


def get_all_days_bu_df(spark, df_stock, first_day, last_day):
    """
    Construct a dataframe containing all days for all (but, sku, model_r3, family, purch_org, sales_org)
    this dataframe is used to fill missing days on stocks data
    """
    delta = last_day - first_day

    list_day = [first_day + datetime.timedelta(days=i) for i in range(delta.days + 1)]
    list_day_add = spark.createDataFrame(list_day, DateType()).selectExpr('value as day')

    all_bu_sku = df_stock.select(['but_num_business_unit', 'sku_num_sku_r3', 'mdl_num_model_r3', 'fam_idr_family',
                                  'purch_org', 'sales_org', 'but_idr_business_unit']).drop_duplicates()
    df_bu_sku_day = list_day_add.crossJoin(all_bu_sku)
    return df_bu_sku_day


def fill_empty_days(df_stock, df_bu_sku_day):
    """
    Since we receive a full of stocks data on first day of month, and a delta for others days, we have to fill the
    dataframe by joining on the output of the fct get_all_days_bu_df.
    After joing we fill the fields  f_quantity, with the last value at each time
    """
    df_bu_sku_day_stock = df_bu_sku_day\
        .join(df_stock, how='left', on=['but_num_business_unit', 'sku_num_sku_r3', 'mdl_num_model_r3', 'fam_idr_family',
                                        'purch_org', 'sales_org', 'day', 'but_idr_business_unit']) \
        .orderBy(['but_num_business_unit', 'sku_num_sku_r3', 'mdl_num_model_r3', 'fam_idr_family', 'purch_org',
                  'sales_org', 'day', 'but_idr_business_unit'])

    # define the window
    window = Window.partitionBy(['but_num_business_unit', 'sku_num_sku_r3', 'mdl_num_model_r3', 'fam_idr_family',
                                 'purch_org', 'sales_org', 'but_idr_business_unit']) \
        .orderBy('day') \
        .rowsBetween(-sys.maxsize, 0)
    # define the forward-filled column
    stock_filled = df_bu_sku_day_stock\
        .withColumn('f_quantity', last(df_bu_sku_day_stock['f_quantity'], ignorenulls=True).over(window))\
        .withColumn('assortment_grade', last(df_bu_sku_day_stock['assortment_grade'], ignorenulls=True).over(window))\
        .fillna(0)
    return stock_filled


def enrich_with_data(stock_filled, df_week, rc_df, week_start, week_end, lifestage_data_first_hist_week):
    """
    Join on Day Table to get week_id
    Join with range choice if the period is after the start of historization of the table range_choice
    """
    stock_week = stock_filled\
        .join(df_week.select(['day', 'week_id']), how='inner', on='day')\
        .where(col('week_id').cast('int') >= week_start)\
        .where(col('week_id').cast('int') <= week_end)

    if week_end > lifestage_data_first_hist_week:
        stock_week = stock_week\
            .join(rc_df,
              on=(stock_week.but_idr_business_unit.cast('int') == rc_df.but_idr_business_unit.cast('int')) &
                 (stock_week.fam_idr_family == rc_df.fam_idr_family) &
                 (rc_df.range_level >= stock_week.assortment_grade) &
                 (rc_df.wee_id_week.cast('int') == df_week.week_id.cast('int')),
              how='inner')

    return stock_week.select(stock_week.but_num_business_unit, stock_week.mdl_num_model_r3, stock_week.day,
                             stock_week.week_id, stock_week.f_quantity, stock_week.purch_org, stock_week.sales_org)


def refine_stock(stock_week):
    """
    1. Aggregate on model: stocks(model) = sum (all skus of the model)
    2. Aggregate on business unit and determine if a model is in sold_out for each business unit
    A model is NOT considered as sold_out if it is available on mag stock for more than 5 days ( > 85%),
     elsewhere it is in soldout
    """
    refined = stock_week\
        .groupby(['but_num_business_unit', 'mdl_num_model_r3', 'day', 'week_id', 'purch_org', 'sales_org'])\
        .agg(
          sum(col('f_quantity')).alias('f_quantity')
        )\
        .withColumn('stock_null', when(col('f_quantity') <= 0, 1).otherwise(0))
    df_final_stock = refined\
        .groupby(['but_num_business_unit', 'mdl_num_model_r3', 'week_id', 'purch_org', 'sales_org']) \
        .agg(sum(col('stock_null')).cast('int').alias('nb_day_stock_null'), \
             avg(col('f_quantity')).alias('f_quantity_mean'), \
             max(col('f_quantity')).cast('int').alias('f_quantity_max'), \
             min(col('f_quantity')).cast('int').alias('f_quantity_min')) \
        .withColumn('f_quantity_mean', round(col('f_quantity_mean'), 2)) \
        .withColumn('week_id', col('week_id').cast('int'))\
        .withColumn('is_sold_out', when(col('nb_day_stock_null') >= 2, 1).otherwise(0))
    return df_final_stock


def keep_only_assigned_stock_for_old_stocks(stock, week_start, lifestage_data_first_hist_week, max_nb_soldout_weeks):
    """
    Since we don't have historic data of lifestage (before lifestage_data_first_hist_week in 2019),
    then we can't use range data to filter assignment model <--> Magasin.
    We have defined a rule to filter assignment model <--> Magasin
        - Use the same rule to define if a model is in sold out in a specific mag in a week
        - If a model is in sold out in a specific BU for more than max_nb_soldout_weeks=4 weeks, this model should be unassigned to the BU
    """
    session_window = Window.partitionBy(['but_num_business_unit', 'mdl_num_model_r3', 'purch_org', 'sales_org'])\
        .orderBy(col('week_id').asc())
    soldout_window = Window.partitionBy(['but_num_business_unit', 'mdl_num_model_r3', 'purch_org', 'sales_org', 'session'])\
        .orderBy(col('week_id').asc())
    if week_start < lifestage_data_first_hist_week:
        stock = stock\
            .withColumn("is_new_session", when(col('is_sold_out') != lag(col('is_sold_out'), 1, -1).over(session_window), 1).otherwise(0))\
            .withColumn("session", sum('is_new_session').over(session_window))\
            .withColumn("nb_weeks_soldout", row_number().over(soldout_window))\
            .withColumn("nb_weeks_soldout", when(col('is_sold_out') == 0, 0).otherwise(col("nb_weeks_soldout")))\
            .where(col("nb_weeks_soldout") <= max_nb_soldout_weeks)
    return stock


def get_stock_avail_by_country(df_stock):
    """
    Aggregate Data to compute stock metrics for each model by week by sales_org
    Metrics:
        - percent_sold_out
        - quantity_mean
        - quantity_min
        - quantity_max
    """
    country_stock = df_stock\
        .groupby(['mdl_num_model_r3', 'week_id', 'purch_org', 'sales_org']) \
        .agg(count(col('but_num_business_unit')).alias('nb_mag'),
             count(when(col('is_sold_out') == 1, col('but_num_business_unit'))).alias('nb_mag_sold_out'),
             sum(col('f_quantity_mean')).alias('quantity'),
             avg(col('f_quantity_mean')).alias('f_quantity_mean'),
             max(col('f_quantity_max')).alias('f_quantity_max'),
             min(col('f_quantity_min')).alias('f_quantity_min')
             )\
        .withColumn('percent_sold_out', col('nb_mag_sold_out')/col('nb_mag'))
    return country_stock


def get_stock_avail_for_all_countries(df_stock):
    """
    Aggregate Data to compute stock metrics for each model by week by custom zone
    """
    global_stock = df_stock\
        .groupby(['mdl_num_model_r3', 'week_id']) \
        .agg(count(col('but_num_business_unit')).alias('nb_mag'),
             count(when(col('is_sold_out') == 1, col('but_num_business_unit'))).alias('nb_mag_sold_out'),
             sum(col('f_quantity_mean')).alias('quantity'),
             avg(col('f_quantity_mean')).alias('f_quantity_mean'),
             max(col('f_quantity_max')).alias('f_quantity_max'),
             min(col('f_quantity_min')).alias('f_quantity_min')
             )\
        .withColumn('percent_sold_out', col('nb_mag_sold_out') / col('nb_mag'))
    return global_stock
