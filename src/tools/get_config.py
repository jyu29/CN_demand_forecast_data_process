import logging
import os
import pprint
import yaml


class Configuration(object):
    """
    Class used to handle and maintain all parameters of this program (timeouts, some other values...)
    """
    _yaml_dict = None
    _logger = logging.getLogger()

    def __init__(self, file_path):
        """
            Read a local yaml file and return a python dictionary
            :param file_path: (string) full path to the yaml file
            :return: (dict) data loaded
        """
        if os.path.exists(file_path):
            with open(file_path) as f:
                self._yaml_dict = yaml.safe_load(f)
                self._logger.info("YAML configuration file ('{}') successfully loaded".format(file_path))
        else:
            raise Exception("Could not load the functional YAML configuration file '{}'".format(file_path))

        self.bucket_clean = self.get_bucket_clean()
        self.bucket_refined = self.get_bucket_refined()
        self.schema_table = self.get_schema_table()
        self.path_clean_datalake = self.get_path_clean_data()
        self.path_refined_global = self.get_path_refined_data()
        self.first_historical_week = self.get_first_historical_week()
        self.first_backtesting_cutoff = self.get_first_backtesting_cutoff()
        self.lifestage_data_first_hist_week = self.get_lifestage_first_hist_week()
        self.max_nb_soldout_weeks = self.get_max_nb_soldout_weeks()

        self.list_purch_org = self.get_list_purch_org()
        self.list_conf = self.get_spark_conf()

    def pretty_print_dict(self):
        """
        Pretty prints the config dictionary
        """
        pprint.pprint(self._yaml_dict)

    def get_bucket_clean(self):
        """
        Retrieves the name of the bucket where clean data is located.
        :return: the name of the bucket where clean data is located (str).
        """
        return self._yaml_dict['buckets']['clean']

    def get_bucket_refined(self):
        """
        Retrieves the name of the bucket where to put refined data.
        :return: the name of the bucket where to put refined data(str).
        """
        return self._yaml_dict['buckets']['refined']

    def get_path_clean_data(self):
        """
        Get global path of clean data (s3 prefix)
        :return: the global path of clean data
        """
        return self._yaml_dict['paths']['clean_datalake']

    def get_path_refined_data(self):
        """
        Get global path of refined data (s3 prefix)
        :return: the global path of refined data
        """
        return self._yaml_dict['paths']['refined_global']

    def get_schema_table(self):
        """
        Get table from conf
        :return: table of the key
        """
        return self._yaml_dict['Tables']['schema']

    def get_spark_conf(self):
        """
        Retrieves Spark configurations list.
        :return: a list of tuples <spark_configuration_name, value>
        for spark configuration (list).
        """
        return list(self._yaml_dict['technical_parameters']['spark_conf'].items())

    def get_first_historical_week(self):
        """
        Get first Historical week param (Functional Param).
        :return: the first Historical week in conf
        """
        return self._yaml_dict['functional_parameters']['first_historical_week']

    def get_first_backtesting_cutoff(self):
        """
        Get first backtesting cutoff param (Functional Param).
        :return: the first backtesting cutoff in conf
        """
        return self._yaml_dict['functional_parameters']['first_backtesting_cutoff']

    def get_lifestage_first_hist_week(self):
        """
        Get first week of availability of historic range_choice data.
        :return: the range choice data first week
        """
        return self._yaml_dict['functional_parameters']['lifestage_data_first_hist_week']

    def get_max_nb_soldout_weeks(self):
        """
        Get max nb week of soldout for historic data. (before 2019)
        :return: max_nb_soldout_weeks
        """
        return self._yaml_dict['functional_parameters']['max_nb_soldout_weeks_for_hist']

    def get_list_purch_org(self):
        """
        Get list of countries where model is applied (EU)
        :return: a list of countries in conf
        """
        return self._yaml_dict['functional_parameters']['list_purch_org']
