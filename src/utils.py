import yaml
import os

def read_parquet_s3(app, bucket, file_path):
    """ """
    df = app.read.parquet(bucket + file_path)
    return df


def write_parquet_s3(spark_df, bucket, file_path):
    """ """
    spark_df.write.parquet(bucket + file_path, mode="overwrite")
    
    
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
    
    # ------------------
    
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
    
    def get_first_week_cutoff(self):
        return self._config_func['first_week_cutoff']
    # ------------------
    
    def get_s3_path_refine_specific_scope(self):
        return self.get_s3_path_refine_specific() + '/' + self.get_scope() + '/'
