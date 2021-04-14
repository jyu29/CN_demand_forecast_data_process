from pyspark.sql.functions import *


def get_offline_sales(tdt, day, week, sku, but, cer, sapb):
    """
    Get Offline sales from transactions data
    Filters:
      but['but_num_typ_but'] == 7 magasin physique
    """
    model_week_sales_offline = tdt \
        .filter(lower(tdt['the_to_type']) == 'offline') \
        .join(broadcast(day), on=to_date(tdt['tdt_date_to_ordered'], 'yyyy-MM-dd') == day['day_id_day'], how='inner') \
        .join(broadcast(week), on=day['wee_id_week'] == week['wee_id_week'], how='inner') \
        .join(sku, on=tdt['sku_idr_sku'] == sku['sku_idr_sku'], how='inner') \
        .join(broadcast(but.where(but['but_num_typ_but'] == 7)), on=tdt['but_idr_business_unit'] == but['but_idr_business_unit'], how='inner') \
        .join(broadcast(cer), on=tdt['cur_idr_currency'] == cer['cur_idr_currency_base'], how='inner') \
        .join(broadcast(sapb),
              on=but['but_num_business_unit'].cast('string') == regexp_replace(sapb['plant_id'], '^0*|\s', ''),
              how='inner') \
        .select(sku['mdl_num_model_r3'].alias('model_id'),
                day['wee_id_week'].cast('int').alias('week_id'),
                week['day_first_day_week'].alias('date'),
                tdt['f_qty_item'],
                tdt['f_pri_regular_sales_unit'],
                tdt['f_to_tax_in'],
                cer['exchange_rate'])
    return model_week_sales_offline


def get_online_sales(dyd, day, week, sku, but, gdc, cer, sapb):
    """
    Get online sales from delivery data
    """
    model_week_sales_online = dyd \
        .filter(lower(dyd['the_to_type']) == 'online') \
        .filter(lower(dyd['tdt_type_detail']) == 'sale') \
        .join(broadcast(day), on=to_date(dyd['tdt_date_to_ordered'], 'yyyy-MM-dd') == day['day_id_day'], how='inner') \
        .join(broadcast(week), on=day['wee_id_week'] == week['wee_id_week'], how='inner') \
        .join(sku, on=dyd['sku_idr_sku'] == sku['sku_idr_sku'], how='inner') \
        .join(broadcast(but), on=dyd['but_idr_business_unit_sender'] == but['but_idr_business_unit'], how='inner') \
        .join(broadcast(gdc), on=but['but_code_international'] == concat(gdc['ean_1'], gdc['ean_2'], gdc['ean_3']), how='inner') \
        .join(broadcast(cer), on=dyd['cur_idr_currency'] == cer['cur_idr_currency_base'], how='inner') \
        .join(broadcast(sapb), on=gdc['plant_id'] == sapb['plant_id'], how='inner') \
        .select(sku['mdl_num_model_r3'].alias('model_id'),
                day['wee_id_week'].cast('int').alias('week_id'),
                week['day_first_day_week'].alias('date'),
                dyd['f_qty_item'],
                dyd['f_tdt_pri_regular_sales_unit'].alias('f_pri_regular_sales_unit'),
                dyd['f_to_tax_in'],
                cer['exchange_rate'])
    return model_week_sales_online


def union_sales(offline_sales_df, online_sales_df):
    """
    union online and offline sales and compute metrics for each (model, date)
     - quantity: online quantity + offline quantities
     - average_price: mean of regular sales unit
     - turnover: sum taxes with exchange
    """
    sales = offline_sales_df.union(online_sales_df) \
        .groupby(['model_id', 'week_id', 'date']) \
        .agg(sum('f_qty_item').alias('sales_quantity'),
             mean(col('f_pri_regular_sales_unit') * col('exchange_rate')).alias('average_price'),
             sum(col('f_to_tax_in') * col('exchange_rate')).alias('sum_turnover')) \
        .filter(col('sales_quantity') > 0) \
        .filter(col('average_price') > 0) \
        .filter(col('sum_turnover') > 0) \
        .orderBy('model_id', 'week_id')
    return sales
