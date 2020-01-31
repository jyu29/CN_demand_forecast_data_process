#!/bin/bash -x
echo "Technical environment configuration file: $1"

sudo pip-3.6 install PyYAML==3.13

spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    --py-files src/utils.py \
    src/data_refining_specific.py $1 conf/functional.yml