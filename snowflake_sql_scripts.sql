-- Creating database
CREATE DATABASE IF NOT EXISTS weather_db;

-- Creating DB schema
CREATE SCHEMA IF NOT EXISTS weather_db.weather_schema; 

-- Selecting db.schema
USE weather_db.weather_schema;

-- Sequence for Fact Table Serrogate Key
CREATE SEQUENCE IF NOT EXISTS weather_db.weather_schema.weather_table_seq
    START WITH 1
    INCREMENT BY 1
    COMMENT = 'This is the sequence for weather table';

-- Sequence for Dim Table Serrogate Key
CREATE SEQUENCE IF NOT EXISTS weather_db.weather_schema.weather_unit_seq
    START WITH 1
    INCREMENT BY 1
    COMMENT = 'This is the sequence for unit table';

-- Creating Fact Table DDL
CREATE TABLE IF NOT EXISTS weather_db.weather_schema.weather_table (
    id INT PRIMARY KEY DEFAULT weather_db.weather_schema.weather_table_seq.NEXTVAL,
    daily_date DATE NOT NULL,
    temperature_2m_max FLOAT NULL,
    temperature_2m_min FLOAT NULL,
    temperature_2m_mean FLOAT NULL,
    apparent_temperature_max FLOAT NULL,
    apparent_temperature_min FLOAT NULL,
    apparent_temperature_mean FLOAT NULL,
    sunrise DATETIME NULL,
    sunset DATETIME NULL,
    daylight_duration FLOAT NULL,
    sunshine_duration FLOAT NULL,
    wind_speed_10m_max FLOAT NULL,
    wind_gusts_10m_max FLOAT NULL,
    wind_direction_10m_dominant FLOAT NULL
);

-- Creating Dim Table DDL
CREATE TABLE IF NOT EXISTS weather_db.weather_schema.weather_unit (
    id INT PRIMARY KEY DEFAULT weather_db.weather_schema.weather_unit_seq.NEXTVAL,
    generate_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    measure VARCHAR(300) NOT NULL,
    unit VARCHAR(300) NOT NULL
);

-- Storage Integration Creation
CREATE STORAGE INTEGRATION IF NOT EXISTS weather_data_s3_storage_integration 
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/<AWS_IAM_ROLE>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://weather-batch-data/processed_data/')
    COMMENT = 'Connection to AWS S3'

-- List of Storage Integration Instances
SHOW STORAGE INTEGRATIONS;
    
-- Description about Storage Intgration Instance
DESC STORAGE INTEGRATION weather_data_s3_storage_integration;

-- Creating File Format
CREATE FILE FORMAT IF NOT EXISTS weather_db.weather_schema.aws_s3_file_format
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO';

-- List All File Format
SHOW FILE FORMATS;

-- Creating Stage Instance
CREATE EXTERNAL STAGE IF NOT EXISTS weather_db.weather_schema.aws_s3_stage 
    URL = 's3://weather-batch-data/processed_data/'
    STORAGE_INTEGRATION = weather_data_s3_storage_integration
    FILE_FORMAT = weather_db.weather_schema.aws_s3_file_format;

-- List All the Stage Instance
SHOW STAGES;

-- List All the Files Inside AWS S3
LIST @weather_db.weather_schema.aws_s3_stage;

-- Selecting the Weather Fact & Dim Data
SELECT * FROM @weather_db.weather_schema.aws_s3_stage/weather_data_20250129_185147_c8789dec-939f-4ec3-9a1c-5c40a7b1be56.parquet LIMIT 100;

SELECT * FROM @weather_db.weather_schema.aws_s3_stage/weather_units_20250129_185147_c8789dec-939f-4ec3-9a1c-5c40a7b1be56.parquet LIMIT 100;

-- Copy Command to Load Data into Dim Table
COPY INTO weather_db.weather_schema.weather_unit (measure, unit)
FROM (
    SELECT
        $1:measure::STRING AS measure,
        $1:unit::STRING AS unit
    FROM @weather_db.weather_schema.aws_s3_stage/weather_units_20250129_185147_c8789dec-939f-4ec3-9a1c-5c40a7b1be56.parquet
);

-- Querying Dim Data
SELECT * FROM weather_db.weather_schema.weather_unit LIMIT 100;

-- Copy Command to Load Data into Fact Table
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
)
FROM (
    SELECT
        $1:date::DATE AS time,
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
    FROM @weather_db.weather_schema.aws_s3_stage/weather_data_20250129_185147_c8789dec-939f-4ec3-9a1c-5c40a7b1be56.parquet
);

-- Querying Fact Table
SELECT * from weather_db.weather_schema.weather_table LIMIT 100;

