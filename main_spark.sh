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
    --conf spark.maximizeResourceAllocation=false
    --conf spark.dynamicAllocation.enabled=false
    --conf spark.executors.cores=5
    --conf spark.executor.memory=3g 
    --conf spark.executor.memoryOverhead=1g 
    --conf spark.executor.instances=8
    --conf spark.default.parallelism=80
    --conf spark.sql.shuffle.partitions=80
    --conf spark.yarn.am.cores=5
    --conf spark.yarn.am.memory=3g 
    --conf spark.yarn.am.memoryOverhead=1g 
	--py-files src/utils.py \
	./src/data_refining_global.py $technical_conf_file

echo $? > code_status
my_exit_code=$(cat code_status)

if [ "$my_exit_code" != "0" ]
then
    exit $my_exit_code
fi

echo "End of Main Spark"