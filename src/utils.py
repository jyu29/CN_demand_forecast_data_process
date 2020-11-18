import time
import datetime
import pprint
import yaml
import s3fs
    
def read_yml(file_path):
    """
    Read a local yaml file and return a python dictionary
    :param file_path: (string) full path to the yaml file
    :return: (dict) data loaded
    """

    if file_path[:2] == "s3":
        fs = s3fs.S3FileSystem()
        with fs.open(file_path, 'r') as f:
            yaml_dict = yaml.safe_load(f)
    else:
        with open(file_path) as f:
            yaml_dict = yaml.safe_load(f)

    return yaml_dict


def pretty_print_dict(dict_to_print):
    """
    Pretty prints a dictionary
    :param dict_to_print: python dictionary
    """

    pprint.pprint(dict_to_print)


def get_current_week():
    """
    Return current week (international standard ISO 8601 - first day of week
    is Sunday, with format 'YYYYWW'
    :return current week (international standard ISO 8601) with format 'YYYYWW'
    """
    today = datetime.date.today()
    return date_to_week_id(today)


def to_uri(bucket, key):
    """
    List all files under a S3 bucket
    :param bucket: (string) name of the S3 bucket
    :param key: (string) S3 key
    :return: (string) URI format
    """
    return f's3://{bucket}/{key}'


def get_timer(starting_time):
    """
    Displays the time that has elapsed between the input timer and the current time.
    :param starting_time: (timecode) timecode from Python 'time' package
    """
    
    end_time = time.time()
    minutes, seconds = divmod(int(end_time - starting_time), 60)
    print("{} minute(s) {} second(s)".format(int(minutes), seconds))