import json
import boto3
import pandas as pd
from datetime import datetime
import uuid

def lambda_handler(event, context):
    bucket_name = "weather-batch-data"
    source_prefix_name = "raw_data/"
    dest_prefix_name = "processed_data/"

    client = boto3.client('s3')

    try:
        # List files in source prefix
        source_bucket_metadata = client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix_name)
        
        if 'Contents' in source_bucket_metadata:
            source_bucket_files = [obj['Key'] for obj in source_bucket_metadata['Contents']]
            
            if len(source_bucket_files) == 1:
                source_file_name = source_bucket_files[0]
                file_obj = client.get_object(Bucket=bucket_name, Key=source_file_name)
                file_content = file_obj['Body'].read().decode('utf-8')
                weather_data = json.loads(file_content)  # Fix: Use json.loads instead of json.load
            elif len(source_bucket_files) > 1:
                return {
                    "statusCode": 400,
                    "message": f"Multiple files found in location {bucket_name}/{source_prefix_name}",
                    "files": source_bucket_files
                }
        else:
            return {
                "statusCode": 404,
                "message": f"No files found in location {bucket_name}/{source_prefix_name}"
            }
    except Exception as e:
        return {
            "statusCode": 500,
            "message": f"Error while accessing source bucket: {str(e)}"
        }

    # Extract data from the weather JSON
    try:
        df_weather_fact = pd.DataFrame({
            'date': weather_data['daily']['time'],
            'temperature_2m_max': weather_data['daily']['temperature_2m_max'],
            'temperature_2m_min': weather_data['daily']['temperature_2m_min'],
            'temperature_2m_mean': weather_data['daily']['temperature_2m_mean'],
            'apparent_temperature_max': weather_data['daily']['apparent_temperature_max'],
            'apparent_temperature_min': weather_data['daily']['apparent_temperature_min'],
            'apparent_temperature_mean': weather_data['daily']['apparent_temperature_mean'],
            'sunrise': weather_data['daily']['sunrise'],
            'sunset': weather_data['daily']['sunset'],
            'daylight_duration': weather_data['daily']['daylight_duration'],
            'sunshine_duration': weather_data['daily']['sunshine_duration'],
            'wind_speed_10m_max': weather_data['daily']['wind_speed_10m_max'],
            'wind_gusts_10m_max': weather_data['daily']['wind_gusts_10m_max'],
            'wind_direction_10m_dominant': weather_data['daily']['wind_direction_10m_dominant']
        })

        df_measurement_unit = pd.DataFrame(weather_data['daily_units'].items(), columns=['measure', 'unit'])

        # Convert DataFrames to Parquet
        weather_fact_parquet = df_weather_fact.to_parquet(index=False)
        measurement_unit_parquet = df_measurement_unit.to_parquet(index=False)

        unique_id = str(uuid.uuid4())
        weather_fact_file = f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{unique_id}.parquet"
        measurement_unit_file = f"weather_units_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{unique_id}.parquet"
    
    except Exception as e:
        return {
            "statusCode": 500,
            "message": f"Error while processing data: {str(e)}"
        }

    try:
        # List and delete existing files in destination prefix
        dest_metadata_response = client.list_objects_v2(Bucket=bucket_name, Prefix=dest_prefix_name)
        
        if 'Contents' in dest_metadata_response:
            existing_files = [obj['Key'] for obj in dest_metadata_response['Contents']]
            total_files = len(existing_files)
            
            delete_objects = [{'Key': key} for key in existing_files]
            client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})
            print(f"Deleted {total_files} files from prefix '{dest_prefix_name}'.")
        else:
            total_files = 0
            print(f"No files found in prefix '{dest_prefix_name}'.")

        # Upload new Parquet files
        client.put_object(Bucket=bucket_name, Key=f"{dest_prefix_name}{weather_fact_file}", Body=weather_fact_parquet)
        client.put_object(Bucket=bucket_name, Key=f"{dest_prefix_name}{measurement_unit_file}", Body=measurement_unit_parquet)
        
        print(f"Uploaded new files: {dest_prefix_name}{weather_fact_file}, {dest_prefix_name}{measurement_unit_file}")

        return {
            "statusCode": 200,
            "message": f"Deleted {total_files} files and uploaded new files '{weather_fact_file}', '{measurement_unit_file}'.",
            "existing_dest_files": existing_files if total_files > 0 else [],
            "total_files_deleted": total_files,
            "new_files_uploaded": [weather_fact_file, measurement_unit_file]
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "message": f"Error while uploading or deleting files: {str(e)}"
        }
