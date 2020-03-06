import yaml
import os
import isoweek
import numpy as np
import time
from datetime import datetime, timedelta


def read_parquet_s3(app, bucket, file_path):
    """ """
    df = app.read.parquet(bucket + file_path)
    return df


def write_parquet_s3(spark_df, bucket, file_path):
    """ """
    spark_df.write.parquet(bucket + file_path, mode="overwrite")


def get_current_week_id():
    shifted_date = datetime.today() + timedelta(days=1)
    current_week_id = int(str(shifted_date.isocalendar()[0]) + str(shifted_date.isocalendar()[1]).zfill(2))
    return current_week_id


def get_next_week_id(week_id):
    '''
    ARGUMENTS:
    
    date ( integer ): week identifier in the format 'year'+'week_number'
    
    RETURNS:
    
    next week in the same format as the date argument
    '''
    if not (isinstance(week_id, (int, np.integer))):
        return 'DATE ARGUMENT NOT AN INT'
    if len(str(week_id)) != 6:
        return 'UNVALID DATE FORMAT'

    year = week_id // 100
    week = week_id % 100

    if week < 52:
        return week_id + 1
    elif week == 52:
        last_week = isoweek.Week.last_week_of_year(year).week
        if last_week == 52:
            return (week_id // 100 + 1) * 100 + 1
        elif last_week == 53:
            return week_id + 1
        else:
            return 'UNVALID ISOWEEK.LASTWEEK NUMBER'
    elif week == 53:
        if isoweek.Week.last_week_of_year(year).week == 52:
            return 'UNVALID WEEK NUMBER'
        else:
            return (date // 100 + 1) * 100 + 1
    else:
        return 'UNVALID DATE'


def get_next_n_week(week_id, n):
    next_n_week = [week_id]
    for i in range(n - 1):
        week_id = get_next_week_id(week_id)
        next_n_week.append(week_id)

    return next_n_week


def get_timer(starting_time):
    end = time.time()
    minutes, seconds = divmod(int(end - starting_time), 60)
    print("{} minute(s) {} second(s)".format(int(minutes), seconds))


class ProgramConfiguration():
    """
    Class used to handle and maintain all parameters of this program (timeouts, some other values...)
    """
    _config_tech = None
    _config_func = None

    def __init__(self, config_tech_path, config_func_path):
        """
        Constructor - Loads the given external YAML configuration file. Raises an error if not able to do it.
        :param config_file_path: (string) full path to the YAML configuration file
        """
        if os.path.exists(config_tech_path):
            with open(config_tech_path, 'r') as f:
                self._config_tech = yaml.load(f)
        else:
            raise Exception("Could not load external YAML configuration file '{}'".format(config_tech_path))

        if os.path.exists(config_func_path):
            with open(config_func_path, 'r') as f:
                self._config_func = yaml.load(f)
        else:
            raise Exception("Could not load external YAML configuration file '{}'".format(config_func_path))

    def get_s3_path_clean(self):
        return self._config_tech['s3_path_clean']

    def get_s3_path_refine_global(self):
        return self._config_tech['s3_path_refine_global']

    def get_s3_path_refine_specific(self):
        return self._config_tech['s3_path_refine_specific']

    def get_first_week_id(self):
        return self._config_func['first_week_id']

    def get_percentage_of_critical_decrease(self):
        return self._config_func['percentage_of_critical_decrease']

    def get_purch_org(self):
        return self._config_func['purch_org']

    def get_sales_org(self):
        return self._config_func['sales_org']

    def get_scope(self):
        return self._config_func['scope']

    def get_filter_type(self):
        return self._config_func['filter_type']

    def get_filter_val(self):
        return self._config_func['filter_val']

    def get_first_test_cutoff(self):
        return self._config_func['first_test_cutoff']

    def get_s3_path_refine_specific_scope(self):
        return self.get_s3_path_refine_specific() + '/' + self.get_scope() + '/'
