#!/bin/bash -x
spark-submit --deploy-mode client \
    --master yarn \
    --driver-memory 32g \
    notebooks/exploratory/data_refining_part_1.py
