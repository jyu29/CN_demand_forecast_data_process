import pyspark.sql.functions as F


def get_model_week_tree(sku_h, week):
    model_week_tree = sku_h \
        .join(F.broadcast(week),
              on=week['day_first_day_week'].between(sku_h['sku_date_begin'], sku_h['sku_date_end']),
              how='inner') \
        .groupBy(week['wee_id_week'].cast('int').alias('week_id'),
                 sku_h['mdl_num_model_r3'].alias('model_id')) \
        .agg(max(sku_h['fam_num_family']).alias('family_id'),
             max(sku_h['sdp_num_sub_department']).alias('sub_department_id'),
             max(sku_h['dpt_num_department']).alias('department_id'),
             max(sku_h['unv_num_univers']).alias('univers_id'),
             max(sku_h['pnt_num_product_nature']).alias('product_nature_id'),
             max(F.when(sku_h['mdl_label'].isNull(), 'UNKNOWN').otherwise(sku_h['mdl_label'])).alias('model_label'),
             max(sku_h['family_label']).alias('family_label'),
             max(sku_h['sdp_label']).alias('sub_department_label'),
             max(sku_h['dpt_label']).alias('department_label'),
             max(sku_h['unv_label']).alias('univers_label'),
             max(F.when(sku_h['product_nature_label'].isNull(), 'UNDEFINED').
                 otherwise(sku_h['product_nature_label'])).alias('product_nature_label'),
             max(sku_h['brd_label_brand']).alias('brand_label'),
             max(sku_h['brd_type_brand_libelle']).alias('brand_type')) \
        .orderBy('week_id', 'model_id')
    return model_week_tree
