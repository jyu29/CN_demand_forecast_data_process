import time
from datetime import datetime, timedelta
from functools import reduce


def to_uri(bucket, key):
    """
    List all files under a S3 bucket

    Args:
        bucket (string): name of the S3 bucket
        key (string): S3 key

    Returns:
        object (string): URI format
    """
    return 's3://{}/{}'.format(bucket, key)


def read_parquet_s3(spark, bucket, key):
    """
    Read parquet files on s3 and return a spark dataframe

    Args:
        spark (SparkSession): spark app
        bucket (string): name of the S3 bucket
        key (string): S3 key

    Returns:
        object (SparkDataframe):
    """
    df = spark.read.parquet(to_uri(bucket, key))
    return df


def write_parquet_s3(df, bucket, key, mode='overwrite'):
    """
    Write a SparkDataframe to parquet files on a S3 bucket

    Args:
        df (SparkDataframe):
        bucket (string): name of the S3 bucket
        key (string):  S3 key
    """
    df.write.parquet(to_uri(bucket, key), mode=mode)


def write_partitionned_parquet_s3(df, bucket, key, partition_col, mode='overwrite'):
    """
    Write a SparkDataframe to parquet files on a S3 bucket
    :df: (SparkDataframe)
    :param bucket: (string) name of the S3 bucket
    :param key: (string) S3 key
    :param partition_col: (string) Partition Column

    Args:
        df:
        bucket:
        key:
        partition_col:
        mode:
    """
    df.write.partitionBy(partition_col).parquet(to_uri(bucket, key), mode=mode)


def get_timer(starting_time):
    """
    Displays the time that has elapsed between the input timer and the current time.
    :param starting_time: (timecode) timecode from Python 'time' package
    """
    end_time = time.time()
    minutes, seconds = divmod(int(end_time - starting_time), 60)
    print("{} minute(s) {} second(s)".format(int(minutes), seconds))


def read_parquet_table(spark, params, path):
    return read_parquet_s3(spark, params.bucket_clean, params.path_clean_datalake + path)


def write_result(towrite_df, params, path):
    """
      Save refined  tables
    """
    start = time.time()
    write_parquet_s3(towrite_df.repartition(10), params.bucket_refined, params.path_refined_global + path)
    get_timer(starting_time=start)


def write_partitioned_result(towrite_df, params, path, partition_col):
    """
      Save refined global tables
    """
    start = time.time()
    write_partitionned_parquet_s3(
        towrite_df,
        params.bucket_refined,
        params.path_refined_global + path,
        partition_col
    )
    get_timer(starting_time=start)


def unionAll(dfs):
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


def get_week_id(date):
    """
      return the week number of a date (ex: 202051)
       - year: str(date.isocalendar()[0])
       - week number: date.strftime("%V") (return the week number where week is starting from monday)
      If the day is sunday, I add one day to get the good day value in order to respect Decathlon regle:
        the fist day of week is sunday
    """
    day_of_week = date.strftime("%w")
    date = date if (day_of_week != '0') else date + timedelta(days=1)
    return int(str(date.isocalendar()[0]) + str(date.isocalendar()[1]).zfill(2))


def get_first_day_month(week_id):
    """
    Get the month of the first day of week
    ex: 202122 (30/05 -> 05/06) => result = 2021-05-01
    """
    fdm = (datetime.strptime(str(week_id) + '1', '%G%V%u') - timedelta(days=1)).replace(day=1)
    return fdm


def get_last_day_week(week_id):
    """
    Get the date of the last day of week (saturday)
    """
    ldw = datetime.strptime(str(week_id) + '6', '%G%V%u')
    return ldw


def get_current_week():
    """
    Return current week (international standard ISO 8601 - first day of week
    is Sunday, with format 'YYYYWW'
    :return current week (international standard ISO 8601) with format 'YYYYWW'
    """
    shifted_date = datetime.today()
    current_week_id = get_week_id(shifted_date)
    return current_week_id


def get_shift_n_week(week_id, nb_weeks):
    """
    Return shifted week (previous or next)
    """
    shifted_date = datetime.strptime(str(week_id) + '1', '%G%V%u') + timedelta(nb_weeks * 7)
    ret_week_id = get_week_id(shifted_date)
    return ret_week_id


def get_previous_n_week(week_id, nb_weeks):
    """
    Get previous week depending on nb_week
    @param nb_weeks should be positive
    """
    if nb_weeks < 0:
        raise ValueError('get_previous_n_week: nb_weeks argument should be positive')
    return get_shift_n_week(week_id, -nb_weeks)


def get_previous_week_id(week_id):
    """
    Get previous week: current week - 1
    """
    return get_previous_n_week(week_id, 1)


def get_next_n_week(week_id, nb_weeks):
    """
    Get next week depending on nb_week
    @param nb_weeks should be positive
    """
    if nb_weeks < 0:
        raise ValueError('get_next_n_week: nb_weeks argument should be positive')
    return get_shift_n_week(week_id, nb_weeks)


def get_next_week_id(week_id):
    """
    Get next week: current week + 1
    """
    return get_next_n_week(week_id, 1)


def get_time():
    return str(datetime.now())
