#!/bin/bash -x
sudo pip-3.6 install PyYAML

spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    src/data_refining_global.py