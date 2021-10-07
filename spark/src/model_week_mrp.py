import time
import tools.utils as ut
from pyspark.sql.functions import *
from pyspark.sql.types import *


def get_sku_mrp_apo(gdw, sapb, sku):
    """
      get sku mrp update
    """
    smu = gdw \
        .join(sku, on=sku['sku_num_sku_r3'] == regexp_replace(gdw['sdw_material_id'], '^0*|\s', ''), how='inner') \
        .join(broadcast(sapb), on=gdw['sdw_plant_id'] == sapb['plant_id'], how='inner') \
        .filter(current_timestamp().between(sku['sku_date_begin'], sku['sku_date_end'])) \
        .select(gdw['date_begin'],
                gdw['date_end'],
                sku['sku_num_sku_r3'].alias('sku_id'),
                sku['mdl_num_model_r3'].alias('model_id'),
                gdw['sdw_material_mrp'].cast('int').alias('mrp')) \
        .drop_duplicates()
    return smu


def get_model_week_mrp_apo(gdw, sapb, sku, day):
    """
      calculate model week mrp
    """

    smu = get_sku_mrp_apo(gdw, sapb, sku)

    model_week_mrp_apo = smu \
        .join(broadcast(day), on=day['day_id_day'].between(smu['date_begin'], smu['date_end']), how='inner') \
        .filter(day['wee_id_week'] >= '201939') \
        .groupBy(day['wee_id_week'].cast('int').alias('week_id'), smu['model_id']) \
        .agg(max(when(smu['mrp'].isin(2, 5), True).otherwise(False)).alias('is_mrp_active')) \
        .orderBy('model_id', 'week_id')
    return model_week_mrp_apo


def fill_mrp_apo_before_201939(model_week_mrp_apo):
    """
    MRP from APO are available since 201939 only
    We have to fill weeks between 201924 and 201938 using the 201939 values.
    """
    model_week_mrp_apo_201939 = model_week_mrp_apo.filter(model_week_mrp_apo['week_id'] == 201939)

    l_df = []
    for w in range(201924, 201939):
        df = model_week_mrp_apo_201939.withColumn('week_id', lit(w))
        l_df.append(df)
    l_df.append(model_week_mrp_apo)

    model_week_mrp_apo_clean = ut.unionAll(l_df)
    return model_week_mrp_apo_clean


def get_mrp_status_pf(asms):
    """
      get mrp status data
      - filter on cz = 2002
    """
    mrp_pf = asms \
        .filter(asms['custom_zone'] == '2002') \
        .select(
          col('sku').cast(IntegerType()).alias('sku_num_sku_r3'),
          col('status').cast(IntegerType()).alias('mrp_status'),
          col('date_begin'),
          col('date_end'))\
        .distinct()
    return mrp_pf


def get_link_purchorg_system(lps):
    """
    get matching purch_org <-> custom_zone
    """
    return lps.select('stp_purch_org_legacy_id', 'stp_purch_org_highway_id').distinct()


def get_migrated_sku_pf(zex):
    """
    Get list of models in new mrp method
    """
    migrated_sku_pf = zex\
        .filter(upper(zex['mrp_pr']) == 'X')\
        .select(
          col('ekorg').alias('purch_org'),
          col('matnr').cast(IntegerType()).alias('sku_num_sku_r3')
        ).distinct()
    return migrated_sku_pf


#def get_weeks(week, first_backtesting_cutoff, limit_week):
#    """
#    Filter on weeks between first backtesting cutoff and limit_date in the future
#    """
#    weeks_df = week.filter(week['wee_id_week'] >= first_backtesting_cutoff) \
#        .filter(week['wee_id_week'] <= limit_week)
#    return weeks_df.select(col('wee_id_week').cast(IntegerType()).alias('week_id')).distinct()


def filter_sku(sku):
    """
      Get list of models after filtering on:
        - Models with univers=0 are used for test
        - Models with univers=14, 89 or 90 are not to sell directly to clients (ateliers, equipments ..)
    """
    sku = sku \
        .filter(~sku['unv_num_univers'].isin([0, 14, 89, 90])) \
        .filter(sku['mdl_num_model_r3'].isNotNull())\
        .select(
            col('sku_num_sku_r3').cast(IntegerType()),
            col('mdl_num_model_r3').cast(IntegerType()).alias('model_id'))
    return sku.distinct()


def get_sku_week_mrp_pf(sku_mrp_pf, week):
    """
    Get mrp data week by week
    """

    sku_week_mrp_pf = week \
        .join(sku_mrp_pf,
              on=week.week_id.between(col('week_from'), col('week_to')),
              how='inner')
    return sku_week_mrp_pf


def get_sku_mrp_pf(sku_migrated_pf, mrp_status_pf, sku):
    sku_mrp_pf = sku_migrated_pf \
        .join(mrp_status_pf, on=['sku_num_sku_r3'], how='inner') \
        .join(sku, on=['sku_num_sku_r3'], how='inner') \
        .withColumn('week_from', year(col('date_begin')) * 100 + weekofyear(col('date_begin'))) \
        .withColumn('week_to', year(col('date_end')) * 100 + weekofyear(col('date_end')))

    return sku_mrp_pf


def get_model_week_mrp_pf(sku_week_mrp_pf, list_active_mrp):
    model_week_mrp_pf = sku_week_mrp_pf \
        .select('purch_org',
                'model_id',
                'week_id',
                'mrp_status') \
        .withColumn('is_mrp_active', col('mrp_status').isin(list_active_mrp)) \
        .groupBy('model_id', 'week_id') \
        .agg(max(col('is_mrp_active')).alias('is_mrp_active'))

    return model_week_mrp_pf


def get_model_week_mrp_pf(sms, zep, week, sku):
    """
    apo_sku_mrp_status_h: contains MRP status for models
    ecc_zaa_extplan: contains all models migrated to the new process of MRP
    """
    #TODO Add all active status
    list_active_mrp = [20, 80]
    mrp_status_pf = get_mrp_status_pf(sms)
    sku_migrated_pf = get_migrated_sku_pf(zep)
    sku = filter_sku(sku)

    sku_mrp_pf = get_sku_mrp_pf(mrp_status_pf, sku_migrated_pf, sku)
    sku_week_mrp_pf = get_sku_week_mrp_pf(sku_mrp_pf, week)
    model_week_mrp_pf = get_model_week_mrp_pf(sku_week_mrp_pf, list_active_mrp)

    return model_week_mrp_pf


def main_model_week_mrp(gdw, sapb, sku, day, sms, zep, week):
    ######### Model MRP for APO
    model_week_mrp_apo = get_model_week_mrp_apo(gdw, sapb, sku, day)
    model_week_mrp_apo.cache()

    print('====> counting(cache) [model_week_mrp_apo] took ')
    start = time.time()
    model_week_mrp_apo_count = model_week_mrp_apo.count()
    ut.get_timer(starting_time=start)
    print('[model_week_mrp] length:', model_week_mrp_apo_count)

    ######### Fill missing MRP for APO
    print('====> Filling missing MRP for APO...')
    model_week_mrp_apo_clean = fill_mrp_apo_before_201939(model_week_mrp_apo)

    ######### Model MRP for Purchase Forecast
    models_week_mrp_pf = get_model_week_mrp_pf(sms, zep, week, sku)

    ######### Join between MRP APO and Purchase Forecast
    models_not_migrate_pf = model_week_mrp_apo_clean.join(models_week_mrp_pf, on=['model_id', 'week_id'], how='leftanti')
    model_week_mrp = models_week_mrp_pf\
        .union(models_not_migrate_pf)\
        .orderBy('model_id', 'week_id')
    #final_model_week_mrp.persist()

    return model_week_mrp
