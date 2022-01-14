# Forecast Data Exposition CN

## Context
* [1. Code Architecture](#1-Code_Architecture)
* [2. Pipeline Launch Step](#2-Pipeline_Launch_Step)
    * [2.1. Bulid EMR and get cluster IP](#21-Bulid_EMR_and_get_cluster_IP)
    * [2.2. Confirm data source path and name](#22-Confirm_data_source_path_and_name)
    * [2.3. Build refining pipeline on Jenkins](#23-Build_exposition_pipeline_on_Jenkins)
    * [2.4. Confirm result data path and name](#24-Confirm_result_data_path_and_name)
    * [2.5. Close the EMR](#25-Close_the_EMR)
* [3. Commond Error](#3-Commond_Error)
* [4. Code Adaption](#4-Code_Adaption)


## 1. Code_Architecture 

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

## 2. Pipeline_Launch_Step

#### Note : Before we start, we should know this pipeline have two stages when you launch it:

> Note : This program depends on Jenkins's pipeline, so we launch it on Jenkins, not on local env.
<br>

### 2.1. Bulid_EMR_and_get_cluster_IP

   1. Choose EMR build pipeline on Jenkins.
   ```Pipeline EMR-CREATE-DEV-CLUSTER-V2```

   2. use the parameters on the bottom to fill in the form and build it.
   <img src="./readme_pic/emr/set_emr_parameter.png" width = "400" align=center/>
   <br>
   
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
   
   4. check pipeline console to see the log, waiting it finish.
   
      1. choose the lighting pipeline task on the web left.
      <img src="./readme_pic//emr/choose_task.png" width = "400" align=center/>
      <br>

      2. you will see the pipeline result like this code paragraph, and if result succeeded, copy your EMR ip.
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
      

### 2.2. Confirm_data_source_path_and_name
   1. confirm you have access to S3.

      You'll have to use both saml2aws & the open-source tool [Cyberduck](https://cyberduck.io).

      Here is the link for the configuration of [saml2aws](https://wiki.decathlon.net/display/DATA/1.2.0.1.2+-+Saml2aws)

   2. check you have required file in s3:
      1. data source 
      <img src="./readme_pic/table_source.png" width = "400" align=center/>
      <br>    
      
      > Note : you need to confirm there are file which you need in this path.
   

### 2.3. Build_exposition_pipeline_on_Jenkins

   1. go to Jenkins, choose the specific pipeline: `forecast-data-refining-demand-cn-dev`  
   <br>

   2. fill the these required parameters into form. 
   <img src="./readme_pic/exposition/set_exposition_paras.png" width = "400" align=center/>
   <br>
   
   3. Jenkins parameters(like picture):
   ```
   1. run_env : dev | prod          (choose an env to run pipieline, it will decide which config file to be used.) 
   2. branch_name : forecast-data-exposition-cn-dev-quicktest (which branch you want to use tp run pipeline.)  
   3. master_ip : 10.226.xxx.xxx    (The EMR ip you get from above.)
   ```

   >Note: Don't click the build now! you should confirm the parameters in config file is already Okay. 
      
   
   3. Confim parameters in config file and push it

      1. choose the file depend on your environment, we use `dev.yml` to be exanple.
      <img src="./readme_pic/config.png" width = "400" align=center/>
      <br>
      
      2. confirm your bucket, path and name are all right.
      ```
      buckets:
         clean: fcst-clean-prod
         refined: fcst-workspace
      paths:
         clean_datalake: datalake/
         refined_global: forecast-cn/fcst-refined-demand-forecast-dev/global/
      ```
      
      3. confirm the code in `purch_org` are you want. 
      ```
      list_purch_org:
         - Z015
         - Z024
         - Z067
         - Z069
         - Z108
      ```
      
      5. Push the code you just adjusted to specific branch of github 
         1. commit your config file just rewrite
         <img src="./readme_pic/git_commit.png" width = "400" align=center/>
         <br>

         2. push your config file just rewrite
         <img src="./readme_pic/git_push.png" width = "400" align=center/>
         <br>

      ### Note : Now! you can click the bottom "bulid" on the Jenkins web. 

   4. How to check the console log of task on Jenkins.

      1. Click the lighting task on the left of web.
      <img src="./readme_pic/emr/choose_task.png" width = "400" align=center/>
      <br>
      
      2. You will see the log like this, if you pipeline run normal.
         1. exposition_handler
         ```
         ####################################################################################
         ########## RECONSTRUCTED SALES EXPOSITION FOR CUTOFF 202146 AND ALGO deepar ##########
         ####################################################################################
         Formatting reconstructed sales dataframe...
         Building outputs for channel 'sac'...
           Mapping forecast dataframe...
             Filtered reconstructed sales horizon : '201947'
         Writing outputs for channel 'sac...'
         ###########################################################################################
         ############################## DEMAND FORECAST GLOBAL EXPOSITION ###########################
         #############################################################################################
         Building outputs for channel 'sac'...
         Writing outputs for channel 'sac...'
         ```
         
      3. when you see the **success** on the log, you are finish pipeline. 
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
   
### 2.4. Confirm_result_data_path_and_name
   1. your result file will show in this folder in s3, there should be 5 files. 
   <img src="./readme_pic/table_result.png" width = "400" align=center/>
   <br>
   
   >Note : To this step, you already finish your exposition pipeline.


## 2.5. Close_the_EMR

   1. choose EMR build pipeline on Jenkins : `EMR-DELETE-DEV-CLUSTER`
   
   2. build EMR with parameter.
   <img src="./readme_pic/emr/build_emr_close.png" width = "400" align=center/>
   <br>
   
   3. check pipeline console to see the log, waiting it finish.
   
      1. choose the lighting pipeline task on the web left. 
      <img src="./readme_pic/emr/choose_task.png" width = "400" align=center/>
      <br>
      
      2. you will see the pipeline result on web bottom, if result is success, it means you close successfully.
      
      ### when you close the EMR, its pipeline process can be quick.


## 3. Commond_Error

> you may need to use Sagemaker to help you debug, the debug file already set in my account. 

1. `Exposition_handler.py` can't find your data source path, or it got not entire path.
   
   1. you will get the error like these message on the bottom, they are all same kand of error.
   ```
   # first error message
   Traceback (most recent call last):
      ......
      KeyError: 'filename'
   Traceback (most recent call last):
      ......
   ValueError: Wrong number of items passed 2, placement implies 1
   
   # second error message   
   file name: predict-2021-12-01-12-42-18-742 not in folder, use next file name to load.
   file name: predict-2021-12-01-13-48-08-809 not in folder, use next file name to load.
   Traceback (most recent call last):
      .....
   OSError: Passed non-file path: fcst-workspace/None
   ValueError: No objects to concatenate
   
   # third errro meassage  
   Formatting forecast dataframe...
   Traceback (most recent call last):
      .......
   ValueError: Length mismatch: Expected 6454 rows, received array of length 43920
   ```

   2. debug file path : {HTSAI/Sagemaker/exposition/expo_debug.ipyn}

   3. check if you got both path of input file and output file when you start the exposition_handler.

   4. If not entire input or output path, you may choose error week_id when you build the pipeline on jenkins or choose error model output name in your config file.<br>


2. spark congif start lag, or it can't get enough resource to run the pipeline.

   1. you will get the error like picture, it will constantly print INFO message like this but not go on, or print sparkcontext has be shoutdown.
   ```
   # first error message (Actually it is not abosutly error, you just need to  wait a long time.)
   22/01/06 07:44:55 INFO Client: Application report for application_1641448074754_0013 (state: ACCEPTED)
   22/01/06 07:44:56 INFO Client: Application report for application_1641448074754_0013 (state: ACCEPTED)
   22/01/06 07:44:57 INFO Client: Application report for application_1641448074754_0013 (state: ACCEPTED)
   22/01/06 07:44:58 INFO Client: Application report for application_1641448074754_0013 (state: ACCEPTED)
   
   # second error message
   21/12/31 03:29:54 ERROR FileFormatWriter: Aborting job 33e3847a-8b9a-4f5a.......
   java.lang.IllegaStateExceptio:SparkContext has been shutdown.....

   ```
   
   2. when you stuck here, just reboost you EMR pipeline, it should be run normally. <br>


## 4. Code_Adaption
- add parameter to choose specific file name for data source 
  - `Exposition_handler.py` : add load_name and save_tag to decide which model's output you want to be the table source.
  - `bi_create_table.py` : add load_path and save_path to decide BI table's path in s3.  <br>

- modify the BI table
  - due to chinese apo data from s3 can't be use, join the apo data from local-site. 
  - delete the redundant result table `outlier_model` and `cutoff_numweek_fcststep` 
  - add a new tmp table `realized_sales` to be a table store real_quantity.
  - group by the  and model_id to sum the column `real_quantity` of online and offline in table `model_week_sales`. <br>

- add filter to reduce the size of BI table 
  - add `whitelist` and `blacklist` to filter what model_id you want in BI table.
  - filter forecast value by `deepar` and `apo_gd` in BI table `quantity_forecast_sales` and `f_forecast_global_demand`. 
  - filter realized value by `y` and `y_1` in BI table `quantity_forecast_sales`. 
  - only keep `<= 10` `forecast_step` value in BI table. <br>
 
- make the process of creating BI table work automatically 
  - rewrite the `bigquery sql script` to the pysparkSQL script. 
  - compose the task of creating BI table and original jenkins pipeline <br>


