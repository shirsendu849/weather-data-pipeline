import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Function to set global variables from Airflow Variables
def global_var_set():
    json_connection_string = Variable.get("weather_api_query_string_var")
    json_connection = json.loads(json_connection_string)
    latitude = json_connection["latitude"]
    longitude = json_connection["longitude"]
    date_format = "%Y-%m-%d"
    current_date = datetime.now()
    start_date = datetime.strftime(current_date - relativedelta(years=1), date_format)
    end_date = datetime.strftime(current_date, date_format)
    Variable.set(key="latitude", value=latitude)
    Variable.set(key="longitude", value=longitude)
    Variable.set(key="start_date", value=start_date)
    Variable.set(key="end_date", value=end_date)
    endpoint = (
        f"archive?latitude={latitude}&longitude={longitude}"
        f"&start_date={start_date}&end_date={end_date}&daily=temperature_2m_max,temperature_2m_min,"
        "temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,"
        "apparent_temperature_mean,sunrise,sunset,daylight_duration,sunshine_duration,"
        "wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant"
    )
    return endpoint

# Function to get list of files from S3 and push them to XCom
def get_list_values(ti):
    files = ti.xcom_pull(task_ids='list_s3_files')
    weather_data_files = [file.split("/")[-1] for file in files if 'weather_data' in file]
    weather_units_files = [file.split("/")[-1] for file in files if 'weather_units' in file]
    logging.info(f"Weather data files: {weather_data_files}")
    logging.info(f"Weather units files: {weather_units_files}")
    ti.xcom_push(key='weather_data_files', value=weather_data_files)
    ti.xcom_push(key='weather_units_files', value=weather_units_files)

# Function to generate SQL statements for loading data into Snowflake
def generate_sql_statements(ti):
    weather_data_files = ti.xcom_pull(task_ids='get_list_values', key='weather_data_files')
    weather_units_files = ti.xcom_pull(task_ids='get_list_values', key='weather_units_files')

    if not weather_data_files or not weather_units_files:
        raise ValueError("No data files found in S3")

    load_weather_data_sql = f"""
        COPY INTO weather_db.weather_schema.weather_table (
            daily_date,
            temperature_2m_max,
            temperature_2m_min,
            temperature_2m_mean,
            apparent_temperature_max,
            apparent_temperature_min,
            apparent_temperature_mean,
            sunrise,
            sunset,
            daylight_duration,
            sunshine_duration,
            wind_speed_10m_max,
            wind_gusts_10m_max,
            wind_direction_10m_dominant
        ) FROM (
            SELECT
                $1:date::DATE AS daily_date,
                $1:temperature_2m_max::FLOAT AS temperature_2m_max,
                $1:temperature_2m_min::FLOAT AS temperature_2m_min,
                $1:temperature_2m_mean::FLOAT AS temperature_2m_mean,
                $1:apparent_temperature_max::FLOAT AS apparent_temperature_max,
                $1:apparent_temperature_min::FLOAT AS apparent_temperature_min,
                $1:apparent_temperature_mean::FLOAT AS apparent_temperature_mean,
                $1:sunrise::DATETIME AS sunrise,
                $1:sunset::DATETIME AS sunset,
                $1:daylight_duration::FLOAT AS daylight_duration,
                $1:sunshine_duration::FLOAT AS sunshine_duration,
                $1:wind_speed_10m_max::FLOAT AS wind_speed_10m_max,
                $1:wind_gusts_10m_max::FLOAT AS wind_gusts_10m_max,
                $1:wind_direction_10m_dominant::FLOAT AS wind_direction_10m_dominant
            FROM @weather_db.weather_schema.aws_s3_stage/{weather_data_files[0]}
        );
    """

    load_weather_unit_sql = f"""
        COPY INTO weather_db.weather_schema.weather_unit (measure, unit) FROM (
            SELECT
                $1:measure::STRING AS measure,
                $1:unit::STRING AS unit
            FROM @weather_db.weather_schema.aws_s3_stage/{weather_units_files[0]}
        );
    """
    logging.info(f"Load weather data SQL: {load_weather_data_sql}")
    logging.info(f"Load weather unit SQL: {load_weather_unit_sql}")
    ti.xcom_push(key='load_weather_data_sql', value=load_weather_data_sql)
    ti.xcom_push(key='load_weather_unit_sql', value=load_weather_unit_sql)

# Function to generate SQL query to check record count in a table
def check_record_count(table_name):
    return f"""
        SELECT COUNT(*) AS record_count
        FROM {table_name};
    """

# Define the DAG
with DAG(
    dag_id="weather_etl_dag",
    description="This DAG extracts data from Web API, transforms the data, and loads it into Snowflake Table",
    start_date=datetime(2025, 1, 24),
    schedule_interval='@daily',
    template_searchpath=['/home/mjunctionetl/airflow/dags/include'],
    tags=["Data Analyst", "Data Engineer"],
    catchup=False
) as dag:

    # Task to set global variables
    global_var_set_task = PythonOperator(
        task_id='global_var_set',
        python_callable=global_var_set
    )

    # Task to check API availability
    api_availibility_check = HttpSensor(
        task_id="api_availibility_check",
        endpoint="{{ti.xcom_pull(task_ids='global_var_set')}}",
        poke_interval=60,
        timeout=300,
        mode="poke",
        method="GET",
        http_conn_id="weather_api_conn"
    )

    # Task to extract weather data using AWS Lambda
    weather_data_extract = LambdaInvokeFunctionOperator(
        task_id="weather_data_extract",
        aws_conn_id="aws_conn_id",
        function_name="weather_data_extract",
        region_name="us-east-1"
    )

    # Task to sense raw data file in S3
    sense_raw_data_file = S3KeySensor(
        task_id='sense_raw_data_file',
        bucket_key='s3://weather-batch-data/raw_data/weather_data*.json',
        wildcard_match=True,
        bucket_name=None,
        aws_conn_id='aws_conn_id',
        timeout=300,
        poke_interval=60,
        mode='poke'
    )

    # Task to transform weather data using AWS Lambda
    weather_data_transform = LambdaInvokeFunctionOperator(
        task_id="weather_data_transform",
        aws_conn_id="aws_conn_id",
        function_name="weather_data_transformation",
        region_name="us-east-1"
    )

    # Task to sense processed fact data file in S3
    sense_process_fact_data_file = S3KeySensor(
        task_id='sense_process_fact_data_file',
        bucket_key='s3://weather-batch-data/processed_data/weather_data*.parquet',
        wildcard_match=True,
        bucket_name=None,
        aws_conn_id='aws_conn_id',
        timeout=300,
        poke_interval=60,
        mode='poke'
    )

    # Task to sense processed dimension data file in S3
    sense_process_dim_data_file = S3KeySensor(
        task_id='sense_process_dim_data_file',
        bucket_key='s3://weather-batch-data/processed_data/weather_units*.parquet',
        wildcard_match=True,
        bucket_name=None,
        aws_conn_id='aws_conn_id',
        timeout=300,
        poke_interval=60,
        mode='poke'
    )

    # Task to list files in S3
    list_s3_files = S3ListOperator(
        task_id="list_s3_files",
        bucket="weather-batch-data",
        prefix="processed_data/",
        delimiter="/",
        aws_conn_id="aws_conn_id"
    )

    # Task to truncate weather_unit table in Snowflake
    truncate_weather_unit = SnowflakeOperator(
        task_id="truncate_weather_unit",
        snowflake_conn_id="snowflake_conn_id",
        sql="TRUNCATE TABLE weather_db.weather_schema.weather_unit;"
    )

    # Task to truncate weather_table in Snowflake
    truncate_weather_table = SnowflakeOperator(
        task_id="truncate_weather_table",
        snowflake_conn_id="snowflake_conn_id",
        sql="TRUNCATE TABLE weather_db.weather_schema.weather_table;"
    )

    # Task to get list of values from S3
    get_list_values_task = PythonOperator(
        task_id='get_list_values',
        python_callable=get_list_values,
        provide_context=True
    )

    # Task to generate SQL statements for loading data into Snowflake
    generate_sql_statements_task = PythonOperator(
        task_id='generate_sql_statements',
        python_callable=generate_sql_statements,
        provide_context=True
    )

    # Task to load weather data into Snowflake
    load_weather_data = SnowflakeOperator(
        task_id="load_weather_data",
        snowflake_conn_id="snowflake_conn_id",
        sql="{{ ti.xcom_pull(task_ids='generate_sql_statements', key='load_weather_data_sql') }}"
    )

    # Task to load weather unit data into Snowflake
    load_weather_unit = SnowflakeOperator(
        task_id="load_weather_unit",
        snowflake_conn_id="snowflake_conn_id",
        sql="{{ ti.xcom_pull(task_ids='generate_sql_statements', key='load_weather_unit_sql') }}"
    )

    # Task to check record count in weather_table
    check_weather_data_count = SnowflakeOperator(
        task_id="check_weather_data_count",
        snowflake_conn_id="snowflake_conn_id",
        sql=check_record_count("weather_db.weather_schema.weather_table")
    )

    # Task to check record count in weather_unit table
    check_weather_unit_count = SnowflakeOperator(
        task_id="check_weather_unit_count",
        snowflake_conn_id="snowflake_conn_id",
        sql=check_record_count("weather_db.weather_schema.weather_unit")
    )

    # Define task dependencies
    global_var_set_task >> api_availibility_check >> weather_data_extract >> sense_raw_data_file >> weather_data_transform
    weather_data_transform >> [sense_process_fact_data_file, sense_process_dim_data_file] >> list_s3_files
    list_s3_files >> [truncate_weather_unit, truncate_weather_table] >> get_list_values_task >> generate_sql_statements_task >> [load_weather_data, load_weather_unit]
    load_weather_data >> check_weather_data_count
    load_weather_unit >> check_weather_unit_count