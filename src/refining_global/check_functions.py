from pyspark.sql.functions import *
from src.tools import utils as ut


def check_d_sku(df):
    """
    Check unicity of each sku_idr_sku
    Check if always date_begin < date_end

    Args:
        df:

    """
    sku_last = df \
        .filter(col('sku_date_end') == '2999-12-31 23:59:59') \
        .groupby('sku_idr_sku') \
        .agg((count(col('sku_num_sku_r3'))).alias('count_nb'))
    sku_duplicate = sku_last.filter(sku_last.count_nb > 1)
    print(f'Sku duplicate : {sku_duplicate.count()}')

    sku_wrong_date = df.filter(col('sku_date_begin') > col('sku_date_end'))
    print(f'Sku date_begin > date_end : {sku_wrong_date.count()}')

    sku_max_tech_date = df.agg(max('rs_technical_date')).collect()[0][0]
    print(f'Sku max technical_date : {sku_max_tech_date}')


def check_d_business_unit(df):
    """
    Check unicity of each but_idr_business_unit

    Args:
        df:

    """
    but_count = df \
        .filter(col('but_num_typ_but').isin({'7', '48', '50'})) \
        .groupby('but_idr_business_unit') \
        .agg((count(col('but_idr_business_unit'))).alias('count_nb'))
    but_duplicate = but_count.filter(but_count.count_nb > 1)
    print(f'Business Unit duplicate : {but_duplicate.count()}')

    but_max_tech_date = df.agg(max('rs_technical_date')).collect()[0][0]
    print(f'Business Unit max technical_date : {but_max_tech_date}')


def check_sales_stability(df, current_week):
    """
    Check stability of sales for the current week.
    If average of percentage sales growth is more or less than 30% it crashes.

    Args:
        df:
        current_week:

    """
    sales_agg = df \
        .groupby('week_id') \
        .agg(sum('sales_quantity').alias('sum_sales'))
    sales_agg_w = sales_agg \
        .filter(sales_agg['week_id'] == ut.get_shift_n_week(current_week, -1)) \
        .select(sales_agg['sum_sales'].alias('sum_sales_cur')).collect()[0][0]
    sales_agg_w_1 = sales_agg \
        .filter(sales_agg['week_id'] == ut.get_shift_n_week(current_week, -2)) \
        .select(sales_agg['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    sales_agg_w_2 = sales_agg \
        .filter(sales_agg['week_id'] == ut.get_shift_n_week(current_week, -3)) \
        .select(sales_agg['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    sales_agg_w_3 = sales_agg \
        .filter(sales_agg['week_id'] == ut.get_shift_n_week(current_week, -4)) \
        .select(sales_agg['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    mean = (sales_agg_w + sales_agg_w_1 + sales_agg_w_2 + sales_agg_w_3) / 4
    sales_pct = ((sales_agg_w - mean) / mean) * 100

    print(f'Sales quantity week-1 : {sales_agg_w}')
    print(f'Sales quantity week-2 : {sales_agg_w_1}')
    print(f'Sales quantity week-3 : {sales_agg_w_2}')
    print(f'Sales quantity week-4 : {sales_agg_w_3}')
    print(f'Sales percentage growth : {sales_pct}')

    assert abs(sales_pct) < 30, '---> ALERT: Absolute sales percentage growth >= 30%'


def check_unicity_by_keys(df, keys):
    """
    Check data unicity by keys.

    Args:
        df:
        keys:

    Returns:
        object:

    """
    assert df.groupBy(keys).count().select(max('count')).collect()[0][0] == 1
