#!/bin/bash -x

echo "Technical environment configuration file: $1"
echo "Execute only last cutoff? $2"

technical_conf_file="conf/$1.yml"
only_last="$2"

sudo pip-3.6 install -r requirements.txt

spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.maximizeResourceAllocation=true \
    --conf spark.dynamicAllocation.minExecutors=2 \
    --conf spark.dynamicAllocation.maxExecutors=22 \
    --conf spark.executor.extraJavaOptions="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p'"\
    --conf spark.driver.extraJavaOptions="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p'"\
    --py-files src/utils.py \
    src/data_refining_global.py $technical_conf_file conf/functional.yml $only_last
echo  $? > code_status
my_exit_code=$(cat code_status)

if [ "$my_exit_code" != "0" ]
then
    exit $my_exit_code
fi

spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    --executor-cores 5 \
    --num-executors 8 \
    --executor-memory 16g \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.default.parallelism=80 \
    --conf spark.sql.shuffle.partitions=80 \
    --conf spark.yarn.am.cores=5 \
    --conf spark.yarn.am.memory=16g \
    --conf spark.yarn.am.memoryOverhead=2g \
    --conf spark.executor.extraJavaOptions="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p'"\
    --conf spark.driver.extraJavaOptions="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:MaxHeapFreeRatio=70 -XX:+CMSClassUnloadingEnabled -XX:OnOutOfMemoryError='kill -9 %p'"\
    --py-files src/utils.py \
    src/data_refining_specific.py $technical_conf_file conf/functional.yml $only_last
echo  $? > code_status