## Overview

In this project, we will build an end-to-end data pipeline using **AWS** services and orchestrate it with **Airflow**. We will extract one year of historical weather data from the free website [Open Meteo](https://open-meteo.com/en/docs/historical-weather-api), perform transformations, and load it into a Snowflake table. The entire process will be fully automated.

## Prerequisites

- AWS Free Tier Account
- Airflow Setup on AWS EC2 (Refer to this [GitHub repo](https://github.com/shirsendu849/airflow_setup_repo) for installation)
- Snowflake Trial Account

## Tools and Technologies

- **Python**
- **SQL**
- **Apache Airflow**
- **AWS S3**
- **AWS Lambda**
- **AWS EC2**
- **Snowflake**

## Architecture Diagram

![Project Architecture Diagram](https://github.com/shirsendu849/weather-data-pipeline/blob/main/weather_etl_architecture.png)

## DAG Workflow

The DAG orchestrates the following steps:

1. **Global Variables**: Some global variables are set that can be used by different tasks in the DAG.
2. **Check API Status**: The **HttpSensor** checks if the weather API is active.
3. **Extract Weather Data**: The **LambdaInvokeFunctionOperator** triggers the **weather_data_extract** Lambda function to extract weather data.
4. **Wait for Raw Data**: The **S3KeySensor** waits for raw data in the S3 staging area.
5. **Transform Data**: The **LambdaInvokeFunctionOperator** triggers the **weather_data_transformation** Lambda function to transform the data.
6. **Wait for Parquet Files**: The **S3KeySensor** waits for transformed data in the S3 processed data location.
7. **List Processed Files**: The **S3ListOperator** lists the processed files in S3.
8. **Truncate Snowflake Tables**: The **SnowflakeOperator** truncates tables in Snowflake.
9. **Generate Copy Commands**: The **PythonOperator** generates dynamic copy commands based on S3 files.
10. **Load Data to Snowflake**: **SnowflakeOperator** tasks load the data into Snowflake tables.
11. **Quality Check**: SnowflakeOperator tasks perform quality checks by counting records in Snowflake tables.

## DAG Diagram

![weather_etl_diagram](https://github.com/shirsendu849/weather-data-pipeline/blob/main/weather_etl_dag_diagram.png)

## How to Set Up

1. **Create an AWS Account**  
   - Sign up for an AWS Free Tier account if you don't have one already.

2. **Set Up Airflow on AWS EC2**  
   - Launch an EC2 instance on AWS and install Apache Airflow by following the instructions provided in this [GitHub repo](https://github.com/shirsendu849/airflow_setup_repo).

3. **Create a Snowflake Account and Configure**  
   - Sign up for a Snowflake trial account if you don’t already have one and run the commands in the script provied one by one.

4. **Configure Lambda Functions**  
   - Create and paste the following Lambda functions from the script provided:
     - **weather_data_extract**
     - **weather_data_transformation**
   - Add the layer provided into the functions in order to use `boto3` and other python modules.
   - Modify IAM role to interact with S3 and atttached with the functions.

5. **Configure S3 Buckets**  
   - Create S3 bucket named **weather-batch-data** having two prefixes:
     - **raw_data**
     - **processed_data**
       
6. **Set Up Airflow Connections**  
   - In Airflow, configure the necessary connections for aws, snowflake from UI.

7. **Deploy the DAG**  
   - Place the given DAG file in the Airflow DAGs directory.

8. **Run the DAG**  
   - Trigger the DAG either manually or by setting a schedule. Monitor the DAG's execution in the Airflow UI.
