import time
import datetime
from datetime import datetime, timedelta


def to_uri(bucket, key):
    """
    List all files under a S3 bucket
    :param bucket: (string) name of the S3 bucket
    :param key: (string) S3 key
    :return: (string) URI format
    """
    return 's3://{}/{}'.format(bucket, key)


def read_parquet_s3(spark, bucket, key):
    """
    Read parquet files on s3 and return a spark dataframe
    :app: (SparkSession) spark app
    :param bucket: (string) name of the S3 bucket
    :param key: (string) S3 key
    :return: (SparkDataframe)
    """
    df = spark.read.parquet(to_uri(bucket, key))
    return df


def write_parquet_s3(df, bucket, key, mode='overwrite'):
    """
    Write a SparkDataframe to parquet files on a S3 bucket
    :df: (SparkDataframe)
    :param bucket: (string) name of the S3 bucket
    :param key: (string) S3 key
    """
    df.write.parquet(to_uri(bucket, key), mode=mode)


def get_current_week():
    """
    Return current week (international standard ISO 8601 - first day of week
    is Sunday, with format 'YYYYWW'
    :return current week (international standard ISO 8601) with format 'YYYYWW'
    """
    shifted_date = datetime.today() + timedelta(days=1)
    current_week_id = int(str(shifted_date.isocalendar()[0]) + str(shifted_date.isocalendar()[1]).zfill(2))
    return current_week_id


def get_timer(starting_time):
    """
    Displays the time that has elapsed between the input timer and the current time.
    :param starting_time: (timecode) timecode from Python 'time' package
    """
    end_time = time.time()
    minutes, seconds = divmod(int(end_time - starting_time), 60)
    print("{} minute(s) {} second(s)".format(int(minutes), seconds))