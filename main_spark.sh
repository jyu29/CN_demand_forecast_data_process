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
    --driver-memory 5g \
    --conf spark.memory.fraction=0.2 \
    --conf spark.memory.storageFraction=0.8 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.maximizeResourceAllocation=true \
    --conf spark.sql.crossJoin.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=50 \
    --conf spark.executor.extraJavaOptions="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p'" \
    --conf spark.driver.extraJavaOptions="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p'" \
	--py-files src/utils.py \
	./src/data_refining_global.py $technical_conf_file

echo $? > code_status
my_exit_code=$(cat code_status)

if [ "$my_exit_code" != "0" ]
then
    exit $my_exit_code
fi

echo "End of Main Spark"