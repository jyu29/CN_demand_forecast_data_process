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
    --py-files src/utils.py \
    src/data_refining_global.py $technical_conf_file conf/functional.yml
echo  $? > code_status
my_exit_code= $(cat code_status)

if [ "$my_exit_code" != "0" ]
then
    exit $my_exit_code
fi

spark-submit \
    --deploy-mode client \
    --master yarn \
    --driver-memory 5g \
    --py-files src/utils.py \
    src/data_refining_specific.py $technical_conf_file conf/functional.yml $only_last
echo  $? > code_status