from pyspark.sql.functions import *


def filter_current_exchange(cex):
    """
      Get the current CRE exchange rate
        cex['cpt_idr_cur_price'] = 6 #exchange rate for sales price
        cex['cur_idr_currency_restit'] == 32 # 32 is the index of euro
      TODO: get a dynamic exchange rate when the right data source is identified
    """
    cex = cex \
        .filter(cex['cpt_idr_cur_price'] == 6) \
        .filter(cex['cur_idr_currency_restit'] == 32) \
        .filter(current_timestamp().between(cex['hde_effect_date'], cex['hde_end_date'])) \
        .select(col('cur_idr_currency_base').alias('cur_idr_currency'),
                col('hde_share_price')) \
        .groupby('cur_idr_currency') \
        .agg(mean(cex['hde_share_price']).alias('exchange_rate'))
    return cex


def filter_days(day, week_begin, week_end):
    """
    Filter on days more recent than first historical week
    """
    day = day \
        .filter(col('wee_id_week') >= week_begin) \
        .filter(col('wee_id_week') <= week_end)
    return day


def filter_weeks(weeks, week_begin, week_end):
    """
    Filter on weeks between first backtesting cutoff and current week
    """
    weeks = weeks \
        .filter(weeks['wee_id_week'] >= week_begin) \
        .filter(weeks['wee_id_week'] <= week_end)
    return weeks


def filter_sapb(sapb, list_purch_org):
    """
      get SiteAttributePlant0Branch after filtering on:
      - sapsrc=PRT: all countries except brazil
      - list_push_org: List Purchase Organization
    """
    sap = sapb \
        .filter(sapb['sapsrc'] == 'PRT') \
        .filter(sapb['purch_org'].isin(list_purch_org))\
        .filter(current_timestamp().between(sapb['date_begin'], sapb['date_end']))
    return sap


def filter_sku(sku):
    """
      Get list of models after filtering on:
        - Models with univers=0 are used for tests
        - Models with univers=14, 89 or 90 are not to sell directly to clients (workshop, services, store equipment...)
    """
    sku = sku \
        .filter(~sku['unv_num_univers'].isin([0, 14, 89, 90])) \
        .filter(sku['mdl_num_model_r3'].isNotNull()) \
        .filter(sku['sku_num_sku_r3'].isNotNull()) \
        .filter(sku['fam_num_family'].isNotNull()) \
        .filter(sku['sdp_num_sub_department'].isNotNull()) \
        .filter(sku['dpt_num_department'].isNotNull()) \
        .filter(sku['unv_num_univers'].isNotNull()) \
        .filter(sku['pnt_num_product_nature'].isNotNull())

    return sku


def filter_gdw(gdw):
    """
    Filter on PRT and wrong data quality
    """
    gdw = gdw \
        .filter(gdw['sdw_sap_source'] == 'PRT') \
        .filter(gdw['sdw_material_mrp'] != '    ')
    return gdw
