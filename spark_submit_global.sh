#!/bin/bash -x
sudo pip-3.13 install PyYAML

spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    --py-files src/utils.py \
    src/data_refining_global.py
