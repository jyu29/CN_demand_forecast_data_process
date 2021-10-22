import pyspark.sql.functions as F

from src.tools import utils as ut


def check_d_sku(sku):
    """
    Check duplicate of each sku_idr_sku
    Check if always date_begin < date_end

    Args:
        sku:

    """
    sku_last = sku \
        .filter(sku['sku_date_end'] == '2999-12-31 23:59:59') \
        .groupby('sku_idr_sku') \
        .agg((F.count(sku['sku_num_sku_r3'])).alias('count_nb'))
    sku_duplicate = sku_last.filter(sku_last.count_nb > 1)
    print(f'Sku duplicate : {sku_duplicate.count()}')

    sku_wrong_date = sku.filter(sku['sku_date_begin'] > sku['sku_date_end'])
    print(f'Sku date_begin > date_end : {sku_wrong_date.count()}')

    sku_max_tech_date = sku.agg(F.max('rs_technical_date')).collect()[0][0]
    print(f'Sku max technical_date : {sku_max_tech_date}')


def check_d_business_unit(but):
    """
    Check duplicate of each but_idr_business_unit

    Args:
        but:

    """
    but_count = but \
        .filter(but['but_num_typ_but'].isin({'7', '48', '50'})) \
        .groupby('but_idr_business_unit') \
        .agg((F.count(but['but_idr_business_unit'])).alias('count_nb'))
    but_duplicate = but_count.filter(but_count.count_nb > 1)
    print(f'Business Unit duplicate : {but_duplicate.count()}')

    but_max_tech_date = but.agg(F.max('rs_technical_date')).collect()[0][0]
    print(f'Business Unit max technical_date : {but_max_tech_date}')


def check_sales_stability(model_week_sales, current_week):
    """
    Check stability of sales for the current week.
    If average of percentage sales growth is more or less than 30% it crashes.

    Args:
        model_week_sales:
        current_week:

    """
    week_sales = model_week_sales \
        .groupby('week_id') \
        .agg(F.sum('sales_quantity').alias('sum_sales'))
    sales_w = week_sales \
        .filter(week_sales['week_id'] == ut.get_shift_n_week(current_week, -1)) \
        .select(week_sales['sum_sales'].alias('sum_sales_cur')).collect()[0][0]
    sales_w_1 = week_sales \
        .filter(week_sales['week_id'] == ut.get_shift_n_week(current_week, -2)) \
        .select(week_sales['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    sales_w_2 = week_sales \
        .filter(week_sales['week_id'] == ut.get_shift_n_week(current_week, -3)) \
        .select(week_sales['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    sales_w_3 = week_sales \
        .filter(week_sales['week_id'] == ut.get_shift_n_week(current_week, -4)) \
        .select(week_sales['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    sales_mean = (sales_w + sales_w_1 + sales_w_2 + sales_w_3) / 4
    sales_pct = ((sales_w - sales_mean) / sales_mean) * 100

    print(f'Sales quantity week-1 : {sales_w}')
    print(f'Sales quantity week-2 : {sales_w_1}')
    print(f'Sales quantity week-3 : {sales_w_2}')
    print(f'Sales quantity week-4 : {sales_w_3}')
    print(f'Sales percentage growth : {sales_pct}')

    assert abs(sales_pct) < 30, '---> ALERT: Absolute sales percentage growth >= 30%'


def check_duplicate_by_keys(df, keys):
    """
    Check data duplicate by keys.

    Args:
        df:
        keys:

    Returns:
        object:

    """
    assert df.groupBy(keys).count().select(F.max('count')).collect()[0][0] == 1
