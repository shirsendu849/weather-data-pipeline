import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import boto3
import uuid

def lambda_handler(event, context):
    # Get environment variables
    latitude = os.environ.get('latitude')
    longitude = os.environ.get('longitude')
    
    # Prepare dates for weather data
    date_format = "%Y-%m-%d"
    current_date = datetime.now()
    start_date = datetime.strftime(current_date - relativedelta(years=1), date_format)
    end_date = datetime.strftime(current_date, date_format)

    # Fetch weather data
    weather_url = (
        f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}"
        f"&start_date={start_date}&end_date={end_date}&daily=temperature_2m_max,temperature_2m_min,"
        "temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,"
        "apparent_temperature_mean,sunrise,sunset,daylight_duration,sunshine_duration,"
        "wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant"
    )
    response = requests.get(weather_url)
    weather_data = response.json()

    # Initialize S3 client
    s3 = boto3.client("s3")
    bucket_name = "weather-batch-data"
    prefix = "raw_data/" 

    # Generate a unique filename
    unique_id = str(uuid.uuid4())
    filename = f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{unique_id}.json"

    existing_files = []
    total_files = 0

    try:
        # List existing files in the prefix
        metadata_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in metadata_response:
            existing_files = [obj['Key'] for obj in metadata_response['Contents']]
            total_files = len(existing_files)
            print(f"Existing {total_files} files in prefix '{prefix}':")
            for file in existing_files:
                print(f"  - {file}")
            
            # Delete existing files
            delete_objects = [{'Key': key} for key in existing_files]
            s3.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})
            print(f"Deleted {total_files} files from prefix '{prefix}'.")
        else:
            print(f"No files found in prefix '{prefix}'.")

        # Upload the new file
        file_content = json.dumps(weather_data, indent=4)
        s3.put_object(Bucket=bucket_name, Key=f"{prefix}{filename}", Body=file_content)
        print(f"Uploaded new file: {prefix}{filename}")

        return {
            "statusCode": 200,
            "message": f"Deleted {total_files} files and uploaded new file '{prefix}{filename}'.",
            "existing_files": existing_files,
            "total_files_deleted": total_files,
            "new_file_uploaded": filename
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "message": f"An error occurred: {str(e)}"
        }
