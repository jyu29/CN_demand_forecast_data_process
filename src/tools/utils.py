import time
from datetime import datetime, timedelta
from functools import reduce


def to_uri(bucket, key):
    """
    Transforms bucket & key strings into S3 URI

    Args:
        bucket (string): name of the S3 bucket
        key (string): S3 key

    Returns:
        object (string): URI format
    """
    return 's3://{}/{}'.format(bucket, key)


def spark_read_parquet_s3(spark, bucket, path):
    """
    Read parquet file(s) hosted on a S3 bucket, load and return as spark dataframe

    Args:
        spark (SparkSession): spark app
        bucket (string): S3 bucket
        path (string): full path to the parquet directory or file within the S3 bucket

    Returns:
        (SparkDataframe): data loaded
    """
    return spark.read.parquet(to_uri(bucket, path))


def spark_write_parquet_s3(df, bucket, dir_path, repartition=10, mode='overwrite'):
    """
    Write a in-memory SparkDataframe to parquet files on a S3 bucket

    Args:
        df (SparkDataframe): the data to save
        bucket (string): S3 bucket
        dir_path (string): full path to the parquet directory within the S3 bucket
        repartition (int): number of partitions files to write
        mode (string): writing mode
    """
    df.repartition(repartition).write.parquet(to_uri(bucket, dir_path), mode=mode)

def spark_write_csv_s3(df, bucket, dir_path, repartition=1, mode='overwrite'):
    """
    Write a in-memory SparkDataframe to parquet files on a S3 bucket

    Args:
        df (SparkDataframe): the data to save
        bucket (string): S3 bucket
        dir_path (string): full path to the parquet directory within the S3 bucket
        repartition (int): number of partitions files to write
        mode (string): writing mode
    """
    df.repartition(repartition).write.csv(to_uri(bucket, dir_path), mode=mode)
    

def get_timer(starting_time):
    """
    Displays the time that has elapsed between the input timer and the current time.

    Args:
        starting_time (timecode): timecode from Python 'time' package
    """
    end_time = time.time()
    minutes, seconds = divmod(int(end_time - starting_time), 60)
    print("{} minute(s) {} second(s)".format(int(minutes), seconds))


def union_all(l_df):
    """
    Apply union function on all spark dataframes in l_df

    """
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), l_df)


def date_to_week_id(date):
    """
    Turn a date to Decathlon week id
    Args:
        date (str, pd.Timestamp or pd.Series): the date or pandas column of dates
    Returns:
        (int): the week id

    """
    day_of_week = date.strftime("%w")
    date = date if (day_of_week != '0') else date + timedelta(days=1)
    return int(str(date.isocalendar()[0]) + str(date.isocalendar()[1]).zfill(2))


def get_current_week_id():
    """
    Return current week id (international standard ISO 8601 - first day of week
    is Sunday, with format 'YYYYWW', as integer

    """
    return date_to_week_id(datetime.today())


def get_shift_n_week(week_id, nb_weeks):
    """
    Return input week_id shifted by nb_weeks (could be negative)

    """
    shifted_date = datetime.strptime(str(week_id) + '1', '%G%V%u') + timedelta(weeks=nb_weeks)
    ret_week_id = date_to_week_id(shifted_date)
    return ret_week_id
