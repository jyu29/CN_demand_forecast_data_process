from pyspark.sql.functions import *
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

