import pyspark.sql.functions as F


def get_offline_sales(tdt, day, week, sku, but, cex, sapb, taiwan):
    """
    1.Get Offline sales from transactions data
        but['but_num_typ_but'] == 7 physical store
        tdt['the_to_type']) == 'offline'
    2.Delete the product that taiwan by from other way:
        ~((sku['mdl_num_model_r3'].isin(taiwan)) & (sapb['purch_org'] == 'Z024'))
    3.Add a column for channel:
        .withColumn("channel", F.lit('offline'))
    """
    offline_sales = tdt \
        .join(F.broadcast(day),
              on=F.to_date(tdt['tdt_date_to_ordered'], 'yyyy-MM-dd') == day['day_id_day'],
              how='inner') \
        .join(F.broadcast(week),
              on=week['wee_id_week'] == day['wee_id_week'],
              how='inner') \
        .join(sku,
              on=sku['sku_idr_sku'] == tdt['sku_idr_sku'],
              how='inner') \
        .join(F.broadcast(but.filter(but['but_num_typ_but'] == 7)),
              on=but['but_idr_business_unit'] == tdt['but_idr_business_unit'],
              how='inner') \
        .join(F.broadcast(cex),
              on=tdt['cur_idr_currency'] == cex['cur_idr_currency'],
              how='left') \
        .join(F.broadcast(sapb),
              on=but['but_num_business_unit'].cast('string') == F.regexp_replace(sapb['plant_id'], '^0*|\s', ''),
              how='inner') \
        .filter(F.lower(tdt['the_to_type']) == 'offline') \
        .filter(~((sku['mdl_num_model_r3'].isin(taiwan)) & (sapb['purch_org'] == 'Z024'))) \
        .select(sku['mdl_num_model_r3'].alias('model_id'),
                day['wee_id_week'].cast('int').alias('week_id'),
                week['day_first_day_week'].alias('date'),
                tdt['f_qty_item'],
                tdt['f_pri_regular_sales_unit'],
                tdt['f_to_tax_in'],
                cex['exchange_rate']) \
        .withColumn("channel", F.lit('offline'))
    return offline_sales


def get_online_sales(dyd, day, week, sku, but, gdc, cex, sapb, channel, taiwan):
    """
    1.Get online sales from delivery data:
        .dyd['tdt_type_detail'] == 'sale'
        .dyd['the_to_type'] == 'online'
    2. Get data from non-canceled:
        . dyd['the_transaction_status'] != 'canceled'
    3.Delete the product that taiwan by from other way:
        ~((sku['mdl_num_model_r3'].isin(taiwan)) & (sapb['purch_org'] == 'Z024'))
    4.Add a column for channel:
        .withColumn("channel", F.lit('offline'))
    """
    online_sales = dyd \
        .join(F.broadcast(day),
              on=F.to_date(dyd['tdt_date_to_ordered'], 'yyyy-MM-dd') == day['day_id_day'],
              how='inner') \
        .join(F.broadcast(week),
              on=week['wee_id_week'] == day['wee_id_week'],
              how='inner') \
        .join(sku,
              on=sku['sku_idr_sku'] == dyd['sku_idr_sku'],
              how='inner') \
        .join(F.broadcast(but),
              on=dyd['but_idr_business_unit_stock_origin'] == but['but_idr_business_unit'],
              how='inner') \
        .join(F.broadcast(gdc),
              on=but['but_code_international'] == F.concat(gdc['ean_1'], gdc['ean_2'], gdc['ean_3']),
              how='inner') \
        .join(F.broadcast(cex),
              on=cex['cur_idr_currency'] == dyd['cur_idr_currency'],
              how='left') \
        .join(F.broadcast(sapb),
              on=sapb['plant_id'] == gdc['plant_id'],
              how='inner') \
        .filter(F.lower(dyd['the_to_type']) == 'online') \
        .filter(F.lower(dyd['tdt_type_detail']) == 'sale') \
        .filter(dyd['the_transaction_status'] != 'canceled') \
        .filter(~((sku['mdl_num_model_r3'].isin(taiwan)) & (sapb['purch_org'] == 'Z024'))) \
        .select(sku['mdl_num_model_r3'].alias('model_id'),
                day['wee_id_week'].cast('int').alias('week_id'),
                week['day_first_day_week'].alias('date'),
                dyd['f_qty_item'],
                dyd['f_tdt_pri_regular_sales_unit'].alias('f_pri_regular_sales_unit'),
                dyd['f_to_tax_in'],
                cex['exchange_rate']) \
        .withColumn("channel", F.lit('online'))
    return online_sales


def union_sales(offline_sales, online_sales, current_week):
    """
    union online and offline sales and compute metrics for each (model, date)
     - quantity: online quantity + offline quantities
     - average_price: mean of regular sales unit
     - turnover: sum taxes with exchange
    """
    model_week_sales = offline_sales.union(online_sales) \
        .groupby(['model_id', 'week_id', 'date', 'channel']) \
        .agg(F.sum('f_qty_item').alias('sales_quantity'),
             F.mean(F.col('f_pri_regular_sales_unit') * F.col('exchange_rate')).alias('average_price'),
             F.sum(F.col('f_to_tax_in') * F.col('exchange_rate')).alias('sum_turnover')) \
        .filter(F.col('sales_quantity') > 0) \
        .filter(F.col('average_price') > 0) \
        .filter(F.col('sum_turnover') > 0) \
        .filter(F.col('week_id') < current_week) \
        .orderBy('model_id', 'week_id')\
        .cache()
    return model_week_sales


def get_model_week_sales(tdt, dyd, day, week, sku, but, cex, sapb, gdc, current_week, taiwan, channel):
    # Get offline sales
    offline_sales = get_offline_sales(tdt, day, week, sku, but, cex, sapb, taiwan)

    # Get online sales
    online_sales = get_online_sales(dyd, day, week, sku, but, gdc, cex, sapb, channel, taiwan)

    # Create model week sales
    model_week_sales = union_sales(offline_sales, online_sales, current_week)

    return model_week_sales
