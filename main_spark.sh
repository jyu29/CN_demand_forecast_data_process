#!/bin/bash
echo "Begining of Main Spark"

technical_env=${1:-dev}
echo "Technical environment: $technical_env"

technical_conf_file="./config/$technical_env.yml"
echo "Technical configuration file: $technical_conf_file"

sudo pip-3.6 install -r requirements.txt

spark-submit \
    --deploy-mode client \
    --master yarn \
	--conf spark.yarn.isPython=true \
	--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python/lib/py4j-src.zip:/usr/lib/spark/python/:<CPS>{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-src.zip' \
	--py-files src/utils.py \
	./src/data_refining_global.py $technical_conf_file

echo $? > code_status
my_exit_code=$(cat code_status)

if [ "$my_exit_code" != "0" ]
then
    exit $my_exit_code
fi

echo "End of Main Spark"