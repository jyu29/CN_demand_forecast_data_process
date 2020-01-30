#!/bin/bash -x
spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    src/data_refining_global.py
