from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *
from tools import date_tools as dt

def overlap_period(list_periods):
    res = [list_periods[0]]
    for a in list_periods[1:]:
        overlap = False
        for b in res:
            if a[0] < b[1] and b[0] < a[1]:
                overlap = True
                res[res.index(b)] = (a[0] if a[0] < b[0] else b[0], a[1] if a[1] > b[1] else b[1])
        if not overlap:
            res.append(a)
    return res


def recurse_overlap(x):
    y = overlap_period(x)
    if x == y:
        return x
    else:
        return recurse_overlap(y)


overlap_period_udf = udf(lambda list_periods: recurse_overlap(list_periods), ArrayType(ArrayType(StringType())))


def clean_data(choices_df):
    """
    Clean Data:
      - First, for all rows with same date_valid_from, keep raw with last update date
      - Then get the overlap period of all listed periods
    plant_id, material_id, model_id, date_valid_from, date_valid_to, date_last_change
    """
    w = Window().partitionBy("plant_id", "material_id", "date_valid_from").orderBy(col("date_last_change").desc())
    last_change_df = choices_df \
        .withColumn("rn", row_number().over(w)) \
        .where(col("rn") == 1) \
        .withColumn("dates_from_to", array(col("date_valid_from"), col("date_valid_to"))) \
        .groupBy("plant_id", "material_id", "model_id")\
        .agg(collect_list(col("dates_from_to")).alias("dates_from_to"))
    df = last_change_df\
        .withColumn("all_periods", overlap_period_udf(col("dates_from_to")))\
        .withColumn("period", explode(col("all_periods")))
    res_df = df.select(
        col("plant_id"),
        col("material_id"),
        col("model_id"),
        df['period'][0].alias("date_from"),
        df['period'][1].alias("date_to")
    )
    return res_df


get_week_id_udf = udf(lambda date: dt.get_week_id(date), StringType())


def get_weeks(week, first_backtesting_cutoff):
    """
    Filter on weeks between first backtesting cutoff and limit_date in the future
    """
    limit_week = dt.get_next_n_week(dt.get_current_week(), 104) #TODO NGA verify with Antoine
    weeks_df = week.filter(week['wee_id_week'] >= first_backtesting_cutoff) \
        .filter(week['wee_id_week'] <= limit_week)
    return weeks_df.select(col('wee_id_week').alias('week_id')).distinct()


def refine_mag_choices(clean_data, weeks):
    choices = clean_data\
        .withColumn("week_from", get_week_id_udf(col("date_from")))\
        .withColumn("week_to", get_week_id_udf(col("date_to")))
    choices_per_week = weeks.join(choices, on=weeks.week_id.between(col("week_from"), col("week_to")), how="inner")
    return choices_per_week


def refine_mag_choices(choices_per_week):
    agg_df = choices_per_week\
        .groupBy("model_id", "week_id")\
        .agg(countDistinct(col("plant_id")).alias("nb_mags"))
    return agg_df
