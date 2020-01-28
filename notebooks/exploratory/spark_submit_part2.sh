#!/bin/bash -x
spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    notebooks/exploratory/data_refining_part_2.py
