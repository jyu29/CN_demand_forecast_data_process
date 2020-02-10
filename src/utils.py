import yaml
import os
import isoweek
import numpy as np

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
    if not(isinstance(week_id, (int, np.integer))):
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


def sup_week(week_id):
    """ """
    week_id = str(week_id)
    y, w = int(week_id[:4]), int(week_id[4:])

    if w == 1:
        w = '52'
        y = str(y - 1)

    elif len(str(w))==1:
        w = w - 1
        y = str(y)
        w = '0' + str(w)

    elif w == 10:
        w = w - 1
        y = str(y)
        w = '0' + str(w)

    else:
        w = str(w - 1)
        y = str(y)

    n_wk = y + w
    return int(n_wk)

def __add_week(week, nb):
    if nb < 0 :
        for i in range(abs(nb)):
            week = sup_week(week)
    else:
        for i in range(nb):
            week = next_week(week)

    return week


def find_weeks(start, end):
    l = [int(start), int(end)]
    start = str(start)+'0'
    start = datetime.strptime(start, '%Y%W%w')
    end = sup_week(end)
    end = str(end)+'0'
    end = datetime.strptime(end, '%Y%W%w')


    for i in range((end - start).days + 1):
        d = (start + timedelta(days=i)).isocalendar()[:2] # e.g. (2011, 52)
        yearweek = '{}{:02}'.format(*d) # e.g. "201152"
        l.append(int(yearweek))

    return sorted(set(l))    
    
    
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
