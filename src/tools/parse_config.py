import os
import json
import logging.config
import argparse


def configure_logging(log_config_file):
    """
    Setup logging configuration (if file cannot be loaded or read, a fallback basic configuration is used instead)

    Args:
        log_config_file: (string) path to external logging configuration file
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

    Returns:
        object: argparse object configured with mandatory and optional arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--configfile', required=True,
                        help="Technical configuration file (YAML format)")
    parser.add_argument('-w', '--whitelist', required=True,
                        help="model_id whitelist file (YAML format)")
    return parser.parse_args()


