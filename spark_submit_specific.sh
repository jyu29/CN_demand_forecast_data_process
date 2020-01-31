#!/bin/bash -x
echo "Technical environment configuration file: $1"
technical_conf_file = "conf/$1.yml"

spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    --py-files src/utils.py \
    src/data_refining_specific.py $technical_conf_file conf/functional.yml