# Forecast Data Refining CN

## Context
* [1. Description](#1Description)
* [2. Input & Output Data](#2inputandoutputData)
* [3. Code Architecture](#3-code-architecture)
* [4. How to run](#4-how-to-run)
    * [4.1. Bulid EMR and get cluster IP](#41-bulid-emr-and-get-cluster-ip)
    * [4.2. Build exposition pipeline on Jenkins](#42-build-exposition-pipeline-on-jenkins)
    * [4.3. Close EMR](#43-close-emr)
* [5. Common Error](#5-common-error)
* [6. What has been changed from master branch](#5-what-has-been-changed-from-master-branch)




## 1. Description
In this repo, we will use data from ingestion repo to do a pre-process tsak in order to turn a lots of dataset into 5 table with specific structure. These new table will be the modeling's input data. you can see all input and ouput data in the below table. 
Besides, if you want to know more about this pipeline's opreation, you can see these two picture:<br>
Flow chart:  https://github.com/dktunited/forecast-data-refining-demand/blob/forecast-data-refining-demand-cn-dev/readme_pic/%E3%80%90Refining%E3%80%9101_flow_chart_refining.drawio.png
<br>

ER diagram:  
1. model_week_sales : https://github.com/dktunited/forecast-data-refining-demand/blob/forecast-data-refining-demand-cn-dev/readme_pic/%E3%80%90Refining%E3%80%9102_model_week_sales.drawio.png <br>
2. model_week_mrp : https://github.com/dktunited/forecast-data-refining-demand/blob/forecast-data-refining-demand-cn-dev/readme_pic/%E3%80%90Refining%E3%80%9103_model_week_mrp.drawio.png <br>
3. model_week_tree : https://github.com/dktunited/forecast-data-refining-demand/blob/forecast-data-refining-demand-cn-dev/readme_pic/%E3%80%90Refining%E3%80%9104_model_week_tree.drawio.png <br>



## 2. Input & Output Data

<table>
  <thead>
    <tr>
        <th>Type</th>
        <th>TableName</th>
        <th>Columns</th>
        <th>S3_Path</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Input data</td>
      <td>f_transaction_detail <br> f_delivery_detail <br> f_currency_exchange <br> d_sku <br> d_sku_h <br> d_business_unit <br>
          sites_attribut_0plant_branches_h <br> d_general_data_warehouse_h <br> d_general_data_customer <br> d_day <br> d_week <br> apo_sku_mrp_status_h <br>
          ecc_zaa_extplan</td>
      <td>too many to show, please check ER diagram</td>
      <td>s3://fcst-clean-prod/datalake/</td>
    </tr>
  </tbody>
  <tfoot>
    <tr>
      <td rowspan=4>Output_data</td>
      <td>model_week_sales.parquet</td>
      <td>model_id <br> week_id <br> date <br> channel <br> sales_quantity</td>
      <td rowspan=4>s3://fcst-workspace/forecast-cn/fcst-refined-demand-forecast-dev/global/</td>
    </tr>
    <tr>
      <td>model_week_price.parquet</td>
      <td>model_id <br> week_id <br> date <br> channel <br> average_price</td>
    </tr>
    <tr>
      <td>model_week_turnover.parquet</td>
      <td>model_id <br> week_id <br> date <br> channel <br> sum_turnover</td>
    </tr>
    <tr>
      <td>model_week_tree.parquet</td>
      <td>model_id <br> family_id <br> sub_department_id <br> department_id <br> univers_id <br> product_nature_id <br> model_label <br> family_label <br>
          sub_department_label <br> department_label <br> univers_label <br> product_nature_label <br> brand_label <br> brand_type</td>
    </tr>
  </tfoot>
</table>



## 3. Code Architecture 

```
forecast-data-exposition-quicktest
│    .gitignore
│    Jenkinsfile
│    Jenkinsfile_debug
│    spark_submit_refining_global.sh
│    main.py 
│    requirements.txt
│    README.md
│
├─── config
│       dev.yml
│       prod.yml
│       debug.yml
│       special_list.yml
│
└───src
     │  
     ├─── refining_global
     │         main_data_refining_globaal.py
     │         model_week_sales.py
     │         model_week_mrp.py
     │         model_week_tree.py
     │         check_functions.py
     │         generic_filter.py
     │
     └─── tool
              get_config.py
              parase_config.py  
              utils.py
```

## 4. How to run

>data exposition pipeline build upon jenkins.

- `exposition_handler.py` : process all model_output data to same type, 
- `bi_table_create.py` : use all table from 'Exposition handler' to create BI table.


### 4.1. Bulid EMR and get cluster IP

   >Select EMR pipeline on Jenkins.```Pipeline EMR-CREATE-DEV-CLUSTER-V2```,use the parameters to build:
 
   
   ```
   parameters: [
                    string(name: "nameOfCluster", value: "${BUILD_TAG}"),
                    string(name: "versionEMR", value: "emr-6.4.0"),
                    string(name: "ClusterType", value: "batch_cluster"),
                    string(name: "instanceTypeMaster", value: "c5g.24xlarge"),
                    string(name: "masterNodeDiskSize", value: "256"),
                    string(name: "nbrCoreOnDemand", value: "6"),
                    string(name: "nbrCoreSpot", value: "0"),
                    string(name: "instanceTypeCore", value: "r5g.24xlarge"),
                    string(name: "coreNodeDiskSize", value: "256"),
                    string(name: "nbrTaskNode", value: "0"),
                    string(name: "instanceTypeTask", value: "r5.2xlarge"),
                    string(name: "taskNodeDiskSize", value: "64"),
                    string(name: "hdfsReplicationFactor", value: "3")
                    ]
   ```

   > check pipeline console to see the log. 
   
   example:https://forecast-jenkins.subsidia.org/view/EMR-HANDLING/job/EMR-CREATE-DEV-CLUSTER-V2/384/console
   
   ```
      Your EMR dev-cluster-emr is built !
      Your available URI are :
      http://IBENKH18-ganglia.forecast-emr.subsidia.org/ganglia/
      http://IBENKH18-hue.forecast-emr.subsidia.org
      http://IBENKH18-node.forecast-emr.subsidia.org
      http://IBENKH18-spark.forecast-emr.subsidia.org
      http://IBENKH18-hdfs.forecast-emr.subsidia.org
      L'ip de votre cluster est : 10.226.xxx.xxx'
      By !
      Jenkins
      [Pipeline] }
      [Pipeline] // withAWS
      [Pipeline] sh
      + echo CLUSTER_IP=10.226.xxx.xxx
      CLUSTER_IP=10.226.xxx.xxx
   ```
      
   > copy your EMR ip.   
     
   ```
   CLUSTER_IP=10.226.xxx.xxx
   ```
      

### 4.2. Build exposition pipeline on Jenkins

   > go to Jenkins, choose the specific pipeline: `forecast-data-refining-cn-dev-quicktest`.
   Before build jenkins pipeline,you should confirm the parameters in config file.
   
 
   ```
      1. run_env : dev | prod          (choose an env to run pipieline, it will decide which config file to be used.) 
      2. branch_name : forecast-data-exposition-cn-dev-quicktest (which branch you want to use tp run pipeline.)  
      3. master_ip : 10.226.xxx.xxx    (The EMR ip you get from above.)
   ```
   
   > Confim parameters in config file and push it. choose the file depend on your environment.
   
   > confirm your bucket, path and name are all right. with the config file name you read.(ex.`dev.yml`)
   
   ```
      buckets:
         clean: fcst-clean-prod
         refined: fcst-workspace
      paths:
         clean_datalake: datalake/
         refined_global: forecast-cn/fcst-refined-demand-forecast-dev/global/
   ```
         
   > confirm the code in `purch_org` are you want. 
   
   ```
      list_purch_org:
         - Z015
         - Z024
         - Z067
         - Z069
         - Z108
   ```
   
   > Remember to **Push and commit** the code from your IDE to the github branch name your entered .Then click the bottom **"bulid"** on the Jenkins 

      
   >See the jenkins console logs

   example: https://forecast-jenkins.subsidia.org/view/TEST/job/forecast-data-refining-demand-cn-dev/103/console

   >See the jenkins console logs
   
   ```
         Load data from clean bucket.
         Make global filter.
         ====> counting(cache) [model_week_sales] took 
         10 minute(s) 19 second(s)
         [model_week_sales] length: 5181802
         ====> counting(cache) [model_week_tree] took 
         1 minute(s) 19 second(s)
         [model_week_tree] length: 62113100
         ====> Model MRP for APO...
         ====> counting(cache) [model_week_mrp_apo] took 
         0 minute(s) 25 second(s)
         [model_week_mrp] length: 9663480
         .......

         End of Data Refining Global
   ```
         
   > when you see the **success** on the log, you are finih pipeline. 
   
   ```
      [Pipeline] }
      [Pipeline] // wrap
      [Pipeline] }
      [Pipeline] // stage
      [Pipeline] }
      [Pipeline] // withEnv
      [Pipeline] }
      [Pipeline] // withEnv
      [Pipeline] }
      [Pipeline] // node
      [Pipeline] End of Pipeline
      [withMaven] WARNING abort infinite build trigger loop. Please consider opening a Jira issue: Infinite loop of job triggers 
      Finished: SUCCESS
   ```
      

### 4.3. Close EMR

   > choose EMR build pipeline on Jenkins : `EMR-DELETE-DEV-CLUSTER`.  build EMR with parameter.check pipeline console to see the log, waiting it finish.

   example : https://forecast-jenkins.subsidia.org/view/EMR-HANDLING/job/EMR-DELETE-DEV-CLUSTER/507/console



## 5. Common error

#### error 1: (Actually it is not abosutly error, you just need to  wait a long time.)

example : https://forecast-jenkins.subsidia.org/view/TEST/job/forecast-data-refining-demand-cn-dev/100/console

   ```
   # first error message
   22/01/06 07:44:55 INFO Client: Application report for application_1641448074754_0013 (state: ACCEPTED)
   22/01/06 07:44:56 INFO Client: Application report for application_1641448074754_0013 (state: ACCEPTED)
   22/01/06 07:44:57 INFO Client: Application report for application_1641448074754_0013 (state: ACCEPTED)
   22/01/06 07:44:58 INFO Client: Application report for application_1641448074754_0013 (state: ACCEPTED)
   
   # scond error message
   21/12/31 03:44:38 INFO ContextHandler: Started o.s.j.s.ServletContextHandler@6df61295{/SQL/execution,null,AVAILABLE,@Spark}
   21/12/31 03:44:38 INFO ServerInfo: Adding filter to /SQL/execution/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
   21/12/31 03:44:38 INFO ContextHandler: Started o.s.j.s.ServletContextHandler@f8b842e{/SQL/execution/json,null,AVAILABLE,@Spark}
   21/12/31 03:44:38 INFO ServerInfo: Adding filter to /static/sql: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
   21/12/31 03:44:38 INFO ContextHandler: Started o.s.j.s.ServletContextHandler@5a832521{/static/sql,null,AVAILABLE,@Spark}
   ```
   
>  it will constantly print INFO message like this but not go on, or print sparkcontext has be shoutdown.when you stuck here, just reboost you EMR pipeline, it should be run normally. <


#### error 2:  (You don't give spark enough memory to finish task.)  

example : https://forecast-jenkins.subsidia.org/view/TEST/job/forecast-data-refining-demand-cn-dev/97/console
   ```
   # There is insufficient memory for the Java Runtime Environment to continue.
   # Native memory allocation (mmap) failed to map 935329792 bytesOpenJDK 64-Bit Server VM warning: 
   INFO: os::commit_memory(0x00000005bea00000, 935329792, 0) failed; error='Cannot allocate memory' (errno=12)
    for committing reserved memory.
   # An error report file with more information is saved as:
   # /home/hadoop/forecast-data-refining-demand-cn-dev/hs_err_pid22226.log
   ```
   
   > that has a simple way to fix this problem: add the cluster's configuration on AWS. you can try to add instanceTypeMaster and instanceTypeCore's level or number


## 6. What has been changed from master branch


#### adject
   - modify the purch_org code(in env.yml).
   - modify the table `d_business_unit` and `f_delivery_detail`'s join key to be `but_idr_business_unit_stock_origin` (in `model_week_sales.py` at line 61).
   - modify the canceled transaction record (in `model_week_sales.py` at line 74).
   - modify the `custom_zone`'s filter condition in table `apo_sku_mrp_status_h`(in `model_week_mrp.py` at line 88).
   - add the model_id's whitelist (in `model_week_mrp.py` at line 54).
   - add the columns `channel` in table `model_week_sales`( in `model_week_sales.py` at line 39 and 83).<br>

#### problem
   - delete the data from product which taiwan's shop buy it from other place but not from china (in `model_week_sales.py` at line 75).
   - add the china's self-currency in table `f_currency_exchange` and ensure sales table have the same currency code in it (in `generic_filter.py` at line 23).<br>


