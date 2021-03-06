#!/bin/bash
echo "Start of Data Refining Global"

technical_env=${1:-dev}
echo "Technical environment: $technical_env"

technical_conf_file="./config/$technical_env.yml"

echo "Technical configuration file: $technical_conf_file"

sudo pip3 install -r requirements.txt
zip -x ./src/refining_specific* -x ./src/refining_global* -r tools.zip ./src/

#echo "Wait 60s until the initialization of metastore is done"
#sleep 60s

echo "Spark submit:"
spark-submit \
    --deploy-mode client \
    --master yarn \
    --py-files tools.zip \
    ./src/refining_global/main_data_refining_global.py -c $technical_conf_file

echo $? > code_status
my_exit_code=$(cat code_status)

if [ "$my_exit_code" != "0" ]
then
    exit $my_exit_code
fi

echo "End of Data Refining Global"