## Overview

This project creates an end-to-end data pipeline that extracts, transforms, and loads weather data using AWS services. The pipeline is orchestrated using Apache Airflow and automates the entire process, from data extraction to loading the data into Snowflake.

## Tools and Technologies

- **Python**
- **SQL**
- **Apache Airflow**
- **AWS S3**
- **AWS Lambda**
- **AWS EC2**
- **Snowflake**

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
