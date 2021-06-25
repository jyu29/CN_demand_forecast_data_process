from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import ArrayType


def overlap_period(list_periods):
    res = [list_periods[0]]
    for a in list_periods[1:]:
        overlap = False
        for b in res:
            if a[0] < b[1] and b[0] < a[1]:
                overlap = True
                res[res.index(b)] = (min(a[0], b[0]), max(a[1], b[1]))
        if not overlap:
            res.append(a)
    return res


overlap_period_udf = udf(lambda list_periods: overlap_period(list_periods), ArrayType())


def get_last_change_data(selection_df):
    """
    plant_id, material_id, model_id, date_valid_from, date_valid_to, date_last_change
    """
    w = Window().partitionBy("plant_id", "material_id", "date_valid_from").orderBy(col("date_last_change").desc())
    last_change_df = selection_df \
        .withColumn("rn", row_number().over(w)) \
        .where(col("rn") == 1) \
        .withColumn("tupple_period", (col("date_valid_from"), col("date_valid_to"))) \
        .groupBy("plant_id", "material_id", "model_id")\
        .agg(collect_list(col("tupple_period").alias("tupple_periods")))

    df = last_change_df.withColumn("all_periods", overlap_period_udf(col("tupple_periods")))
    return last_change_df

