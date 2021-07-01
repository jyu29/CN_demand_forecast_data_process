from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *


def filter_sap(sapb, list_purch_org):
    """
      get SiteAttributePlant0Branch after filtering on:
      - sapsrc=PRT: all countries except brazil
      - list_push_org: EU countries
    """
    sap = sapb\
        .filter(sapb['sapsrc'] == 'PRT') \
        .filter(sapb['purch_org'].isin(list_purch_org))\
        .filter(current_timestamp().between(sapb['date_begin'], sapb['date_end']))\
        .select(col("plant_id").alias("ref_plant_id"), col("purch_org"), col("sales_org"))\
        .distinct()
    return sap


def get_weeks(week, first_backtesting_cutoff, limit_week):
    """
    Filter on weeks between first backtesting cutoff and limit_date in the future
    """
    weeks_df = week.filter(week['wee_id_week'] >= first_backtesting_cutoff) \
        .filter(week['wee_id_week'] <= limit_week)
    return weeks_df.select(col('wee_id_week').alias('week_id')).distinct()


def overlap_period(list_periods):
    res = [list_periods[0]]
    for a in list_periods[1:]:
        overlap = False
        for b in res:
            if a[0] <= b[1] and b[0] <= a[1]:
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


def get_clean_data(choices_df):
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


def get_choices_per_week(clean_data, weeks):
    choices = clean_data\
        .withColumn("date_from_cast", col("date_from").cast(StringType()))\
        .withColumn("date_from_1", to_date(col("date_from")))\
        .withColumn("date_to_1", to_date(col("date_to")).cast(StringType()))\
        .withColumn("week_from", year(col("date_from")) * 100 + weekofyear(col("date_from_1")))\
        .withColumn("week_to", year(col("date_to")) * 100 + weekofyear(col("date_to_1")))
    # .withColumn("date_from", when(dayofweek(col("date_from")) == 1, date_add(col("date_from"), 1)).otherwise(col("date_from")))\
    # .withColumn("date_to", when(dayofweek(col("date_to")) == 1, date_add(col("date_to"), 1)).otherwise(col("date_to")))\

    print("---------> before join Choices.show()")
    choices.show(truncate=False)
    choices_per_week = weeks.join(choices, on=weeks.week_id.between(col("week_from"), col("week_to")), how="inner")
    return choices_per_week


def refine_mag_choices(choices_per_week):
    agg_df = choices_per_week\
        .groupBy("model_id", "week_id", "purch_org", "sales_org")\
        .agg(countDistinct(col("plant_id")).alias("nb_mags"))
    return agg_df
