from pyspark.sql.functions import *
from tools import date_tools as dt
"""
f_transaction_detail
    tous les YYYYMM
f_delivery_detail
    tous les YYYYMM
sites_attribut_0plant_branches_h
    présence Z002/2002
    unicité
d_general_data_warehouse_h
    jointure sur les modèles uniques de f_transaction_detail & f_delivery_detail et vérifier que chaque modèle a un MRP status
d_general_data_customer
    présence Z002/2002
    unicité"""

def check_d_week(df, current_week):
    """
    Check if there is 104 weeks in the future

    """
    # Check des semaines
    df_control_week = df.where(col("wee_id_week") > current_week)
    df_count_week = df_control_week.agg((count(col("wee_id_week"))).alias("count")).collect()[0][0]
    assert df_count_week >= 104



def check_d_day(df, current_week):
    """
    Check if there is 104 weeks in the future, 728 days

    """
    # Check des jours
    df_control_day = df.where(col("wee_id_week") > current_week)
    df_count_day = df_control_day.agg((count(col("wee_id_week"))).alias("count")).collect()[0][0]
    assert df_count_day >= 728


def check_d_sku(df):
    """
    Check unicity of each sku_idr_sku
    Check if always date_begin < date_end

    """
    # Check de l'unicité de la d_sku
    sku_last = df.where(col("sku_date_end") == '2999-12-31 23:59:59'). \
        groupby('sku_idr_sku'). \
        agg((count(col("sku_num_sku_r3"))).alias("count_nb"))
    sku_duplicate = sku_last.filter(sku_last.count_nb > 1)
    print(sku_duplicate.count())

    sku_wrong_date = df.where(col("sku_date_begin") > col("sku_date_end"))
    print(sku_wrong_date.count())

    sku_max_tech_date = df.agg(max("rs_technical_date")).collect()[0][0]
    print(sku_max_tech_date)

def check_d_business_unit(df):
    """
    Check unicity of each but_idr_business_unit

    """
    # Check de l'unicité de la d_business_unit
    but_count = df.where(col("but_num_typ_but").isin({"7", "48", "50"})). \
        groupby('but_idr_business_unit'). \
        agg((count(col("but_idr_business_unit"))).alias("count_nb"))
    but_duplicate = but_count.filter(but_count.count_nb > 1)
    print(but_duplicate.count())

    but_max_tech_date = df.agg(max("rs_technical_date")).collect()[0][0]
    print(but_max_tech_date)


def check_sales(df, current_week):
    """
    Check stability of sales for the current week.
    If sales <30 %

    """
    sales_agg = df.groupby('week_id').agg(sum('sales_quantity').alias('sum_sales'))
    sales_agg_w = \
    sales_agg.filter(sales_agg['week_id'] == 202125).select(sales_agg['sum_sales'].alias('sum_sales_cur')).collect()[0][
        0]
    sales_agg_w_1 = sales_agg.filter(sales_agg['week_id'] == get_shift_n_week(current_week, -1))\
                            .select(sales_agg['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    sales_agg_w_2 = sales_agg.filter(sales_agg['week_id'] == get_shift_n_week(current_week, -2))\
                            .select(sales_agg['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    sales_agg_w_3 = sales_agg.filter(sales_agg['week_id'] == get_shift_n_week(current_week, -3))\
                            .select(sales_agg['sum_sales'].alias('sum_sales_last')).collect()[0][0]
    mean = (sales_agg_w + sales_agg_w_1 + sales_agg_w_1 + sales_agg_w_2 + sales_agg_w_3) / 4
    sales_pct = ((sales_agg_w - mean) / mean) * 100

    assert sales_pct > -30