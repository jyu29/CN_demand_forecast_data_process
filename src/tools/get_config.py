import logging
import os
import pprint
import yaml


class Configuration(object):
    """
    Class used to handle and maintain all parameters of this program (timeouts, some other values...)
    """
    _yaml_dict = None
    _yaml_list = None
    _logger = logging.getLogger()

    def __init__(self, file_path):
        """
        Read a local yaml file and return a python dictionary

        Args:
            file_path: (string) full path to the yaml file

        Returns:
            object: (dict) data loaded
        """
        if os.path.exists(file_path):
            with open(file_path) as f:
                self._yaml_dict = yaml.safe_load(f)
                self._logger.info("YAML configuration file ('{}') successfully loaded".format(file_path))
        else:
            raise Exception("Could not load the functional YAML configuration file '{}'".format(file_path))

        id_list = './config/special_list.yml'
        if os.path.exists(id_list):
            with open(id_list) as i:
                self._yaml_list = yaml.safe_load(i)
                self._logger.info("YAML whitelist file ('{}') successfully loaded".format(id_list))
        else:
            raise Exception("Could not load the functional YAML whitelist file '{}'".format(id_list))

        self.bucket_clean = self.get_bucket_clean()
        self.bucket_refined = self.get_bucket_refined()
        self.path_clean_datalake = self.get_path_clean_data()
        self.path_refined_global = self.get_path_refined_data()
        self.first_historical_week = self.get_first_historical_week()
        self.first_backtesting_cutoff = self.get_first_backtesting_cutoff()
        self.list_purch_org = self.get_list_purch_org()
        self.list_conf = self.get_spark_conf()
        self.white_list = self.get_white_list_id()
        self.taiwan_list = self.get_taiwan_self_sale_list_id()
        self.but_path = self.get_business_path()
        self.but_week = self.get_business_week()


    def pretty_print_dict(self):
        """
        Pretty prints the config dictionary
        """
        pprint.pprint(self._yaml_dict)

    def pretty_print_list(self):
        """
        Pretty prints the config dictionary
        """
        pprint.pprint(self._yaml_list)

    def get_bucket_clean(self):
        """
        Get the name of the bucket where clean data is located.

        Returns:
            object: (string) the name of the bucket where clean data is located
        """
        return self._yaml_dict['buckets']['clean']

    def get_bucket_refined(self):
        """
        Get the name of the bucket where to put refined data.

        Returns:
            object: (string) the name of the bucket where to put refined data
        """
        return self._yaml_dict['buckets']['refined']

    def get_path_clean_data(self):
        """
        Get global path of clean data (s3 prefix)

        Returns:
            object: (string) the global path of clean data
        """
        return self._yaml_dict['paths']['clean_datalake']

    def get_path_refined_data(self):
        """
        Get global path of refined data (s3 prefix)

        Returns:
            object: (string) the global path of refined data
        """
        return self._yaml_dict['paths']['refined_global']

    def get_spark_conf(self):
        """
        Get Spark configurations list

        Returns:
            object: (list) a list of tuples <spark_configuration_name, value> for spark configuration
        """
        return list(self._yaml_dict['technical_parameters']['spark_conf'].items())

    def get_first_historical_week(self):
        """
        Get first Historical week param (Functional Param).

        Returns:
            object: the first Historical week in conf
        """
        return self._yaml_dict['functional_parameters']['first_historical_week']

    def get_first_backtesting_cutoff(self):
        """
        Get first backtesting cutoff param (Functional Param)

        Returns:
            object: the first backtesting cutoff in conf
        """
        return self._yaml_dict['functional_parameters']['first_backtesting_cutoff']

    def get_list_purch_org(self):
        """
        Get list of countries where model is applied

        Returns:
            object: a list of countries in conf
        """
        return self._yaml_dict['functional_parameters']['list_purch_org']

    def get_white_list_id(self):
        """
        Get list of model_id which model need to keep

        Returns:
            object: a list of model_id in whitelist
        """
        if self._yaml_list['white_list']:
            white = self._yaml_list['white_list']
        else:
            white = []

        return white

    def get_taiwan_self_sale_list_id(self):
        """
        Get list of model_id which model need to keep

        Returns:
            object: a list of model_id in blacklist of Taiwan's own purchase model.
        """
        return self._yaml_list['taiwan_self_sale']

    def get_business_path(self):

        return self._yaml_dict['paths']['tableau']

    def get_business_week(self):

        but_week = self._yaml_dict['functional_parameters']['but_week']

        if but_week is None:
            raise ValueError("list of week for creating business_unit BI table must be here.")

        if self._yaml_dict['functional_parameters']['but_range']:
            but_week = [i for i in range(but_week[0],but_week[1])]

        return but_week