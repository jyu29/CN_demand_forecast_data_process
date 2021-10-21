from pyspark.sql.functions import *


def get_offline_sales(tdt, day, week, sku, but, cex, sapb):
    """
    Get Offline sales from transactions data
    Filters:
      but['but_num_typ_but'] == 7 physical store
      tdt['the_to_type']) == 'offline'
    """
    model_week_sales_offline = tdt \
        .join(broadcast(day),
              on=to_date(tdt['tdt_date_to_ordered'], 'yyyy-MM-dd') == day['day_id_day'],
              how='inner') \
        .join(broadcast(week),
              on='wee_id_week',
              how='inner') \
        .join(sku,
              on='sku_idr_sku',
              how='inner') \
        .join(broadcast(but.filter(but['but_num_typ_but'] == 7)),
              on='but_idr_business_unit',
              how='inner') \
        .join(broadcast(cex),
              on=tdt['cur_idr_currency'] == cex['cur_idr_currency'],
              how='inner') \
        .join(broadcast(sapb),
              on=but['but_num_business_unit'].cast('string') == regexp_replace(sapb['plant_id'], '^0*|\s', ''),
              how='inner') \
        .filter(lower(tdt['the_to_type']) == 'offline') \
        .select(sku['mdl_num_model_r3'].alias('model_id'),
                day['wee_id_week'].cast('int').alias('week_id'),
                week['day_first_day_week'].alias('date'),
                tdt['f_qty_item'],
                tdt['f_pri_regular_sales_unit'],
                tdt['f_to_tax_in'],
                cex['exchange_rate'])
    return model_week_sales_offline


def get_online_sales(dyd, day, week, sku, but, gdc, cex, sapb):
    """
    Get online sales from delivery data
    Filters:
      dyd['tdt_type_detail'] == 'sale'
      dyd['the_to_type'] == 'online'
    """
    model_week_sales_online = dyd \
        .join(broadcast(day),
              on=to_date(dyd['tdt_date_to_ordered'], 'yyyy-MM-dd') == day['day_id_day'],
              how='inner') \
        .join(broadcast(week),
              on='wee_id_week',
              how='inner') \
        .join(sku,
              on='sku_idr_sku',
              how='inner') \
        .join(broadcast(but),
              on=dyd['but_idr_business_unit_sender'] == but['but_idr_business_unit'],
              how='inner') \
        .join(broadcast(gdc),
              on=but['but_code_international'] == concat(gdc['ean_1'], gdc['ean_2'], gdc['ean_3']),
              how='inner') \
        .join(broadcast(cex),
              on='cur_idr_currency',
              how='inner') \
        .join(broadcast(sapb),
              on=gdc['plant_id'] == sapb['plant_id'],
              how='inner') \
        .filter(lower(dyd['the_to_type']) == 'online') \
        .filter(lower(dyd['tdt_type_detail']) == 'sale') \
        .select(sku['mdl_num_model_r3'].alias('model_id'),
                day['wee_id_week'].cast('int').alias('week_id'),
                week['day_first_day_week'].alias('date'),
                dyd['f_qty_item'],
                dyd['f_tdt_pri_regular_sales_unit'].alias('f_pri_regular_sales_unit'),
                dyd['f_to_tax_in'],
                cex['exchange_rate'])
    return model_week_sales_online


def union_sales(offline_sales, online_sales):
    """
    union online and offline sales and compute metrics for each (model, date)
     - quantity: online quantity + offline quantities
     - average_price: mean of regular sales unit
     - turnover: sum taxes with exchange
    """
    model_week_sales = offline_sales.union(online_sales) \
        .groupby(['model_id', 'week_id', 'date']) \
        .agg(sum('f_qty_item').alias('sales_quantity'),
             mean(col('f_pri_regular_sales_unit') * col('exchange_rate')).alias('average_price'),
             sum(col('f_to_tax_in') * col('exchange_rate')).alias('sum_turnover')) \
        .filter(col('sales_quantity') > 0) \
        .filter(col('average_price') > 0) \
        .filter(col('sum_turnover') > 0) \
        .orderBy('model_id', 'week_id')
    return model_week_sales


def get_model_week_sales(tdt, dyd, day, week, sku, but, cex, sapb, gdc):

    # Get offline sales
    model_week_sales_offline = get_offline_sales(tdt, day, week, sku, but, cex, sapb)

    # Get online sales
    model_week_sales_online = get_online_sales(dyd, day, week, sku, but, gdc, cex, sapb)

    # Create model week sales
    model_week_sales = union_sales(model_week_sales_offline, model_week_sales_online)

    return model_week_sales
