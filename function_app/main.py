import os
import json
import boto3
import pandas as pd
import geopandas as gpd
from datetime import datetime
from io import StringIO, BytesIO
from shapely.geometry import Point
from flask import Flask, request, jsonify
import logging
from dotenv import load_dotenv
from urllib.parse import urlparse


load_dotenv()


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Environment variables
BUCKET_RAW_LOGS = os.getenv('BUCKET_RAW_LOGS')
BUCKET_GEOPARQUET = os.getenv('BUCKET_GEOPARQUET')
BUCKET_PROCESSED_RAW_LOGS = os.getenv('BUCKET_PROCESSED_RAW_LOGS')
GEOPARQUET_FILE_NAME = os.getenv('GEOPARQUET_FILE_NAME')
FAAS_LOG_FILE = os.getenv('FAAS_LOG_FILE')



S3_HOST = os.getenv('S3_HOST')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_KEY = os.getenv('S3_KEY')
S3_SECRET = os.getenv('S3_SECRET')


# Parse bucket and endpoint from S3_HOST
parsed = urlparse(S3_HOST)
bucket = S3_BUCKET
endpoint_url = f"{parsed.scheme}://{parsed.netloc}/{bucket}"

print(f"DigitalOcean S3: {endpoint_url}/{bucket} ")

# Create S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    endpoint_url=endpoint_url
)


# Initialize S3 client for DigitalOcean Spaces
# s3_client = boto3.client(
#     's3',
#     endpoint_url=DO_SPACES_ENDPOINT,
#     aws_access_key_id=DO_SPACES_KEY,
#     aws_secret_access_key=DO_SPACES_SECRET
# )



def log_to_faas(message):
    """Append log message to FAAS log file"""
    try:
        timestamp = datetime.now(datetime.UTC)
        log_entry = f"{timestamp}: {message}\n"
        
        # Try to get existing log content
        try:
            response = s3_client.get_object(
                Bucket=BUCKET_GEOPARQUET, 
                Key=FAAS_LOG_FILE
            )
            existing_content = response['Body'].read().decode('utf-8')
        except s3_client.exceptions.NoSuchKey:
            existing_content = ""
        
        # Append new log entry
        updated_content = existing_content + log_entry
        
        # Upload back to S3
        s3_client.put_object(
            Bucket=BUCKET_GEOPARQUET,
            Key=FAAS_LOG_FILE,
            Body=updated_content.encode('utf-8'),
            ContentType='text/plain'
        )
        
        logger.info(f"Logged to FAAS: {message}")
        
    except Exception as e:
        logger.error(f"Failed to log to FAAS: {e}")

def get_txt_files_list():
    """Get list of TXT files from raw logs bucket"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_RAW_LOGS,
            Prefix=""
        )
        
        txt_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].lower().endswith('.txt'):
                    txt_files.append(obj['Key'])
        
        logger.info(f"Found {len(txt_files)} TXT files")
        return txt_files
        
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise

def read_txt_file(file_key):
    """Read TXT log file from S3 and return as DataFrame"""
    try:
        response = s3_client.get_object(
            Bucket=BUCKET_RAW_LOGS, 
            Key=file_key
        )
        content = response['Body'].read().decode('utf-8')
        
        # Parse CSV content
        df = pd.read_csv(StringIO(content))
        
        # Convert datetime column
        df['gps_datetime'] = pd.to_datetime(df['gps_datetime'])
        
        logger.info(f"Read {len(df)} records from {file_key}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading file {file_key}: {e}")
        raise

def convert_to_geoparquet(df):
    """Convert DataFrame to GeoDataFrame with Point geometries"""
    try:
        # Create Point geometries from lat/lon
        geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
        
        # Drop original lat/lon columns as they're now in geometry
        gdf = gdf.drop(['latitude', 'longitude'], axis=1)
        
        logger.info(f"Converted {len(gdf)} records to GeoDataFrame")
        return gdf
        
    except Exception as e:
        logger.error(f"Error converting to GeoParquet: {e}")
        raise

def append_to_geoparquet(new_gdf):
    """Append new data to existing GeoParquet file"""
    try:
        # Try to read existing GeoParquet file
        try:
            response = s3_client.get_object(
                Bucket=BUCKET_GEOPARQUET, 
                Key=GEOPARQUET_FILE_NAME
            )
            existing_gdf = gpd.read_parquet(BytesIO(response['Body'].read()))
            
            # Concatenate with new data
            combined_gdf = pd.concat([existing_gdf, new_gdf], ignore_index=True)
            logger.info(f"Appended to existing file. Total records: {len(combined_gdf)}")
            
        except s3_client.exceptions.NoSuchKey:
            # File doesn't exist, use new data as is
            combined_gdf = new_gdf
            logger.info(f"Creating new GeoParquet file with {len(combined_gdf)} records")
        
        # Convert to parquet bytes
        parquet_buffer = BytesIO()
        combined_gdf.to_parquet(parquet_buffer, engine='pyarrow')
        parquet_buffer.seek(0)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=BUCKET_GEOPARQUET,
            Key=GEOPARQUET_FILE_NAME,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        logger.info(f"Successfully updated GeoParquet file")
        
    except Exception as e:
        logger.error(f"Error appending to GeoParquet: {e}")
        raise

def move_processed_file(file_key):
    """Move processed file to processed bucket"""
    try:
        # Copy file to processed bucket
        copy_source = {
            'Bucket': BUCKET_RAW_LOGS,
            'Key': file_key
        }
        
        s3_client.copy(
            copy_source, 
            BUCKET_PROCESSED_RAW_LOGS, 
            file_key
        )
        
        # Delete from raw bucket
        s3_client.delete_object(
            Bucket=BUCKET_RAW_LOGS, 
            Key=file_key
        )
        
        logger.info(f"Moved {file_key} to processed bucket")
        
    except Exception as e:
        logger.error(f"Error moving file {file_key}: {e}")
        raise

def process_logs():
    """Main processing function"""
    try:
        log_to_faas("Starting log processing")
        
        # Get list of TXT files
        txt_files = get_txt_files_list()
        
        if not txt_files:
            log_to_faas("No TXT files found to process")
            return {"status": "success", "message": "No files to process"}
        
        processed_count = 0
        total_records = 0
        
        for file_key in txt_files:
            try:
                log_to_faas(f"Processing file: {file_key}")
                
                # Read TXT file
                df = read_txt_file(file_key)
                
                if len(df) == 0:
                    log_to_faas(f"Skipping empty file: {file_key}")
                    continue
                
                # Convert to GeoParquet format
                gdf = convert_to_geoparquet(df)
                
                # Append to main GeoParquet file
                append_to_geoparquet(gdf)
                
                # Move processed file
                move_processed_file(file_key)
                
                processed_count += 1
                total_records += len(df)
                
                log_to_faas(f"Successfully processed {file_key} - {len(df)} records")
                
            except Exception as e:
                error_msg = f"Error processing {file_key}: {e}"
                logger.error(error_msg)
                log_to_faas(error_msg)
                # Continue with next file
                continue
        
        success_msg = f"Processing complete. {processed_count} files processed, {total_records} total records"
        log_to_faas(success_msg)
        
        return {
            "status": "success", 
            "files_processed": processed_count,
            "total_records": total_records,
            "message": success_msg
        }
        
    except Exception as e:
        error_msg = f"Critical error in processing: {e}"
        logger.error(error_msg)
        log_to_faas(error_msg)
        return {"status": "error", "message": error_msg}

@app.route('/webhook', methods=['POST'])
def webhook():
    """Webhook endpoint to trigger log processing"""
    try:
        logger.info("Webhook triggered")
        
        # Optional: Validate webhook payload if needed
        # payload = request.get_json()
        
        # Process the logs
        result = process_logs()
        
        return jsonify(result), 200 if result["status"] == "success" else 500
        
    except Exception as e:
        error_msg = f"Webhook error: {e}"
        logger.error(error_msg)
        log_to_faas(error_msg)
        return jsonify({"status": "error", "message": error_msg}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

if __name__ == '__main__':
    # Validate required environment variables
    # required_vars = [
    #     'BUCKET_RAW_LOGS', 'BUCKET_GEOPARQUET', 'BUCKET_PROCESSED_RAW_LOGS',
    #     'GEOPARQUET_FILE_NAME', 'FAAS_LOG_FILE', 'DO_SPACES_KEY', 
    #     'DO_SPACES_SECRET', 'DO_SPACES_ENDPOINT'
    # ]
    
    # missing_vars = [var for var in required_vars if not os.getenv(var)]
    # if missing_vars:
    #     logger.error(f"Missing required environment variables: {missing_vars}")
    #     exit(1)
    
    # Run the app
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)