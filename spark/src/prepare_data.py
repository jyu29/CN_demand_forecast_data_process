from pyspark.sql.functions import *


def get_current_exchange(cex):
    """
      Get the current CRE exchange rate
        cex['cpt_idr_cur_price'] = 6 #taux change prix de vente
        cex['cur_idr_currency_restit'] == 32 # 32 is the index of euro
      TODO: get a dynamic exchange rate when the right data source is identified
    """
    cer = cex \
        .where(cex['cpt_idr_cur_price'] == 6) \
        .where(cex['cur_idr_currency_restit'] == 32) \
        .filter(current_timestamp().between(cex['hde_effect_date'], cex['hde_end_date'])) \
        .select('cur_idr_currency_base', 'cur_idr_currency_restit', 'hde_share_price') \
        .groupby('cur_idr_currency_base', 'cur_idr_currency_restit') \
        .agg(mean(cex['hde_share_price']).alias('exchange_rate'))
    return cer


def get_days(day_df, first_historical_week, current_week):
    """
    Filter on days between first historical week and the current date
    """
    day_df = day_df\
        .where(col('wee_id_week') >= first_historical_week)\
        .where(col('wee_id_week') < current_week)
    return day_df


def get_weeks(week, first_backtesting_cutoff, current_week):
    """
    Filter on weeks between first backtesting cutoff and current week
    """
    weeks_df = week.filter(week['wee_id_week'] >= first_backtesting_cutoff) \
        .filter(week['wee_id_week'] <= current_week)
    return weeks_df


def filter_sap(sapb, list_puch_org):
    """
      get SiteAttributePlant0Branch after filtering on:
      - sapsrc=PRT: all countries except brazil
      - list_push_org: EU countries
    """
    sap = sapb\
        .filter(sapb['sapsrc'] == 'PRT') \
        .filter(sapb['purch_org'].isin(list_puch_org))\
        .filter(current_timestamp().between(sapb['date_begin'], sapb['date_end']))
    return sap


def filter_sku(sku):
    """
      Get list of models after filtering on:
        - Models with univers=0 are used for test
        - Models with univers=14, 89 or 90 are not to sell directly to clients (ateliers, equipments ..)
    """
    sku = sku \
        .filter(~sku['unv_num_univers'].isin([0, 14, 89, 90])) \
        .filter(sku['mdl_num_model_r3'].isNotNull())
    return sku
