#!/bin/bash
echo "Begining of Main Spark"

technical_env=${1:-dev}
scope=${2:-refining}
echo "Scope: $scope"
echo "Technical environment: $technical_env"

technical_conf_file="./spark/config/$technical_env.yml"
echo "Technical configuration file: $technical_conf_file"

sudo pip-3.6 install -r requirements.txt

# Wait 60s until the initialization of metastore is done
sleep 60s

spark-submit \
    --deploy-mode client \
    --master yarn \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.maximizeResourceAllocation=false \
    --conf spark.driver.memory=38g \
    --conf spark.executor.memory=38g \
    --conf spark.executor.cores=5 \
    --conf spark.driver.cores=5 \
    --conf spark.executor.instances=40 \
    --conf spark.executor.memoryOverhead=4g \
    --conf spark.default.parallelism=200 \
    --conf spark.executor.extraJavaOptions="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p'"\
    --conf spark.driver.extraJavaOptions="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p'"\
    --conf yarn.nodemanager.vmem-check-enabled=false \
    --conf yarn.nodemanager.pmem-check-enabled=false \
    --conf spark.driver.maxResultSize=4g \
	  --py-files spark/src/tools/utils.py \
	  ./spark/src/main_data_refining.py -s $scope -c $technical_conf_file

echo $? > code_status
my_exit_code=$(cat code_status)

if [ "$my_exit_code" != "0" ]
then
    exit $my_exit_code
fi

echo "End of Main Spark"