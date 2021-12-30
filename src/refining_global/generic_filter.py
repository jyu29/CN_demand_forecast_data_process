import pyspark.sql.functions as F


def filter_current_exchange(cex):
    """
    Get the current CRE exchange rate
        cex['cpt_idr_cur_price'] = 6 #exchange rate for sales price
        cex['cur_idr_currency_restit'] == 32 # 32 is the index of euro
        19 is CN, 37 is HK, 46 is JP, 49 is KN, 90 is TW
      TODO: get a dynamic exchange rate when the right data source is identified
    """
    cex = cex \
        .filter(cex['cpt_idr_cur_price'] == 6) \
        .filter(cex['cur_idr_currency_restit'].isin([19, 37])) \
        .filter(F.current_timestamp().between(cex['hde_effect_date'], cex['hde_end_date'])) \
        .select(cex['cur_idr_currency_base'].alias('cur_idr_currency'),
                cex['cur_idr_currency_restit'],
                cex['hde_share_price']) \
        .groupby('cur_idr_currency', 'cur_idr_currency_restit') \
        .agg(F.mean(cex['hde_share_price']).alias('exchange_rate'))

    cex = cex \
        .filter(~((cex['cur_idr_currency_restit'] == 37) & (cex['cur_idr_currency'] != 19))) \
        .withColumn('exchange_rate', F.when(cex['cur_idr_currency_restit'] == 37, 1.0).otherwise(cex['exchange_rate']))
    return cex

def filter_day(day, week_begin, week_end):
    """
    Filter on days more recent than first historical week
    """
    day = day \
        .filter(day['wee_id_week'] >= week_begin) \
        .filter(day['wee_id_week'] <= week_end)
    return day


def filter_week(week, week_begin, week_end):
    """
    Filter on weeks between first backtesting cutoff and current week
    """
    week = week \
        .filter(week['wee_id_week'] >= week_begin) \
        .filter(week['wee_id_week'] <= week_end)
    return week


def filter_sapb(sapb, list_purch_org):
    """
    Get SiteAttributePlant0Branch after filtering on:
        - sapsrc=PRT: all countries except brazil
        - list_push_org: List Purchase Organization
    """
    sap = sapb \
        .filter(sapb['sapsrc'] == 'PRT') \
        .filter(sapb['purch_org'].isin(list_purch_org)) \
        .filter(F.current_timestamp().between(sapb['date_begin'], sapb['date_end']))
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


def filter_channel(but):
    """
    Create a table for channel_name and channel_key of online data.
    """
    channel = but.select(F.concat(but['but_num_typ_but'].cast('string'),
                                  but['but_num_business_unit'].cast('string'),
                                  but['but_sub_num_but'].cast('string')).alias('channel_key'),
                         but['but_name_business_unit'].alias('channel_name')).distinct()
    return channel


def filter_dyd(dyd):
    """
    trun the column the_transaction_id in delivery table into the key used for join with channel_name.

    """
    dyd = dyd \
        .withColumn('channel_id_1', F.regexp_extract('the_transaction_id', r'(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)', 1))\
        .withColumn('channel_id_2', F.regexp_extract('the_transaction_id', r'(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)', 2))\
        .withColumn('channel_id_3', F.regexp_extract('the_transaction_id', r'(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)', 3))

    dyd = dyd.withColumn('the_transaction_id', F.concat(dyd.channel_id_1, dyd.channel_id_2, dyd.channel_id_3))

    return dyd
