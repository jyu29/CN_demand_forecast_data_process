#!/bin/bash -x
echo "Technical environment configuration file: $1"
technical_conf_file = "conf/$1.yml"
echo "The technical configuration file is $technical_conf_file"

spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    --py-files src/utils.py \
    src/data_refining_global.py $technical_conf_file conf/functional.yml