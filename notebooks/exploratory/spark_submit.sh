#!/bin/bash -x
spark-submit --deploy-mode client --master yarn \
    --conf 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python/lib/py4j-src.zip:/usr/lib/spark/python/:<CPS>{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-src.zip' \
    --conf spark.yarn.isPython=true \
    --conf spark.executor.memoryOverhead=8192 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    notebooks/exploratory/data_refining_part_1.py
