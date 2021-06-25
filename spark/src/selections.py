from pyspark.sql.functions import *
from pyspark.sql import Window


def get_last_change_data(selection_df):
    """
    plant_id, material_id, model_id, date_valid_from, date_valid_to, date_last_change
    """
    w = Window().partitionBy("plant_id", "material_id", "date_valid_from").orderBy(col("date_last_change").desc)
    last_change_df = selection_df \
        .withColumn("rn", row_number().over(w)) \
        .where(col("rn") == 1)
    return last_change_df

