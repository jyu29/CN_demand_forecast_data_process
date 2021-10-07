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
	  --py-files spark/src/tools/utils.py \
	  ./spark/src/main_data_refining.py -s $scope -c $technical_conf_file

echo $? > code_status
my_exit_code=$(cat code_status)

if [ "$my_exit_code" != "0" ]
then
    exit $my_exit_code
fi

echo "End of Main Spark"