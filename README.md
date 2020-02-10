# Demand Forecast : Data Refining brick

## Context

This project represents the ML brick of the Demand Forecasting project for APO.

The run pipeline for the entire project is represented hereunder:
![Pipeline model](assets/pipeline_modelisation.png)

Here are the GitHub links to the other blocks of the project:
- [Data Ingestion](https://github.com/dktunited/forecast-data-ingestion.git)
- [Modelin](https://github.com/dktunited/forecast-modeling-demand.git)
- [Monitoring](https://github.com/dktunited/forecast-monitoring.git)
- [Automation](https://github.com/dktunited/forecast-automation.git)

## Description

This data refining block is orchestrated by a `main.sh` file which launches 2 distinct parts:  

The first one (`data_refining_global.py`) formats the data  in a global way so that it is usable whatever the period or scope you are working on. Concretely, the script: 
- reads the data tables provided by the Data Ingestion brick.
- joins and formats the information.
- creates and saves 3 tables: **actual_sales** which contains the sales quantities per week, **active_sales** which contains the sales quantities per week during the activity (production) periods of the products, and **model_info** which contains descriptive (and static) information about the products.

The second one (`data_refining_specific.py`), creates the training data for the modeling brick for a given scope and period (cutoff). Concretely, the script:
- reads the data tables provided by the previous part.
- 
- 

## Scheduling

This brick is scheduled through Jenkins:
- [Jenkins job](https://forecast-jenkins.subsidia.org/view/PIPELINE-RUN/job/forecast-data-refining-demand/)
- The hereinabove job is called upon by this [Run Pipeline](https://forecast-jenkins.subsidia.org/job/forecast-pipeline-demand/) job

## TODO
- Refactor the code in a functionnal way
- Document functions + Logging + Tests
- ...