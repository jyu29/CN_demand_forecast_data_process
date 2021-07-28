import os
import json
import logging.config
import argparse


def configure_logging(log_config_file):
    """
    Setup logging configuration (if file cannot be loaded or read, a fallback basic configuration is used instead)
    :param log_config_file: (string) path to external logging configuration file
    """
    if os.path.exists(log_config_file):
        with open(log_config_file, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)


def basic_parse_args():
    """
    Parse main program arguments to ensure everything is correctly launched
    :return: argparse object configured with mandatory and optional arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--configfile', required=True,
                        help="Technical configuration file (YAML format)")
    parser.add_argument('-s', '--scope', required=False,
                        help="Choosing scope of the program: refining, stocks")
    return parser.parse_args()