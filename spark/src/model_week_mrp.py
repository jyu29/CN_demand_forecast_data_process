from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce


def get_sku_mrp_update(gdw, sapb, sku):
    """
      get sku mrp update
    """
    smu = gdw \
        .filter(gdw['sdw_sap_source'] == 'PRT') \
        .filter(gdw['sdw_material_mrp'] != '    ') \
        .join(broadcast(sapb), on=gdw['sdw_plant_id'] == sapb['plant_id'], how='inner') \
        .join(sku, on=sku['sku_num_sku_r3'] == regexp_replace(gdw['sdw_material_id'], '^0*|\s', ''), how='inner') \
        .filter(current_timestamp().between(sku['sku_date_begin'], sku['sku_date_end'])) \
        .select(gdw['date_begin'],
                gdw['date_end'],
                sku['sku_num_sku_r3'].alias('sku_id'),
                sku['mdl_num_model_r3'].alias('model_id'),
                gdw['sdw_material_mrp'].cast('int').alias('mrp')) \
        .drop_duplicates()
    return smu


def get_model_week_mrp(smu, day):
    """
      calculate model week mrp
    """
    model_week_mrp = smu \
        .join(broadcast(day), on=day['day_id_day'].between(smu['date_begin'], smu['date_end']), how='inner') \
        .filter(day['wee_id_week'] >= '201939') \
        .groupBy(day['wee_id_week'].cast('int').alias('week_id'), smu['model_id']) \
        .agg(max(when(smu['mrp'].isin(2, 5), True).otherwise(False)).alias('is_mrp_active')) \
        .orderBy('model_id', 'week_id')
    return model_week_mrp


def unionAll(dfs):
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


def fill_missing_mrp(model_week_mrp):
    """
    MRP are available since 201939 only
    We have to fill weeks between 201924 and 201938 using the 201939 values.
    """
    model_week_mrp_201939 = model_week_mrp.filter(model_week_mrp['week_id'] == 201939)

    l_df = []
    for w in range(201924, 201939):
        df = model_week_mrp_201939.withColumn('week_id', lit(w))
        l_df.append(df)
    l_df.append(model_week_mrp)

    model_week_mrp_filled = unionAll(l_df)
    return model_week_mrp_filled

# ********** New MRP Process **************


def get_mrp_status(asms):
    """
      get mrp status data
      - filter on cz = 2002
    """
    filtered_mrp = asms \
        .filter(asms['custom_zone'] == '2002') \
        .select(
          col("sku").cast(IntegerType()).alias("sku_num_sku_r3"),
          col("status").cast(IntegerType()).alias("mrp_status"),
          col("date_begin"),
          col("date_end"))\
        .distinct()
    return filtered_mrp


def get_link_purchorg_system(lps):
    """
    get matching purch_org <-> custom_zone
    """
    return lps.select('stp_purch_org_legacy_id', 'stp_purch_org_highway_id').distinct()


def get_mrp_models(ecc_zaa_extplan):
    """
    Get list of models in new mrp method
    """
    mrp_passage = ecc_zaa_extplan\
        .filter(upper(ecc_zaa_extplan['mrp_pr']) == 'X')\
        .select(
          col("ekorg").alias("purch_org"),
          col("matnr").cast(IntegerType()).alias("sku_num_sku_r3")
        ).distinct()
    return mrp_passage


def get_weeks(week, first_backtesting_cutoff, limit_week):
    """
    Filter on weeks between first backtesting cutoff and limit_date in the future
    """
    weeks_df = week.filter(week['wee_id_week'] >= first_backtesting_cutoff) \
        .filter(week['wee_id_week'] <= limit_week)
    return weeks_df.select(col('wee_id_week').cast(IntegerType()).alias('week_id')).distinct()


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
            col("sku_num_sku_r3").cast(IntegerType()),
            col("mdl_num_model_r3").cast(IntegerType()).alias("model_id"))
    return sku.distinct()


def get_mrp_status_per_week(clean_data, weeks):
    """
    Get mrp data week by week
    """
    mrp = clean_data\
        .withColumn("week_from", year(col("date_begin")) * 100 + weekofyear(col("date_begin")))\
        .withColumn("week_to", year(col("date_end")) * 100 + weekofyear(col("date_end")))
    mrp_per_week = weeks.join(mrp, on=weeks.week_id.between(col("week_from"), col("week_to")), how="inner")
    return mrp_per_week
