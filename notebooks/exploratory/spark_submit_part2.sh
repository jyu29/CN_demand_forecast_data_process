#!/bin/bash -x
spark-submit --deploy-mode client --master yarn \
    --driver-memory 19g \
    --executor-memory 19g \
    --driver-cores 5 \
    notebooks/exploratory/data_refining_part_2.py
