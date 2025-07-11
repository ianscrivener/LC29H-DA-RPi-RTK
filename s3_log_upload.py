import os
import time
import boto3
from dotenv import load_dotenv
from urllib.parse import urlparse
from datetime import datetime, timezone

# Load environment variables from .env
load_dotenv()

S3_HOST = os.getenv('S3_HOST')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_KEY = os.getenv('S3_KEY')
S3_SECRET = os.getenv('S3_SECRET')
S3_SECONDS = int(os.getenv('S3_SECONDS', '180'))
LOG_FILE = os.getenv('LOG_FILE', 'rtk_log.txt')


# Parse bucket and endpoint from S3_HOST
parsed = urlparse(S3_HOST)
bucket = S3_BUCKET
endpoint_url = f"{parsed.scheme}://{parsed.netloc}"


print(f"Logging to S3: {endpoint_url}/{bucket} every {S3_SECONDS} seconds")


# Create S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    endpoint_url=endpoint_url
)


def upload_log():
    if not os.path.isfile(LOG_FILE):
        print(f"Log file {LOG_FILE} not found.")
        return
    try:

        # create a timestamp variable in UTC
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        # Use timestamp_LOG_FILE as the uploaded filename
        upload_filename = f"{timestamp}_{os.path.basename(LOG_FILE)}"

        # rename the log file to include the timestamp
        os.rename(LOG_FILE, upload_filename)

        # move file to _LOGS_RAW
        raw_logs_dir = '_LOGS_RAW'
        if not os.path.exists(raw_logs_dir):
            os.makedirs(raw_logs_dir)
        raw_log_path = os.path.join(raw_logs_dir, upload_filename)  
        os.rename(upload_filename, raw_log_path)

        # Update upload_filename to point to the new location
        upload_filepath = os.path.join(raw_logs_dir, upload_filename)

        # Upload the renamed log file to S3
        s3.upload_file(upload_filepath, bucket, upload_filename)

        print(f"S3 Uploaded {LOG_FILE} to {bucket} as {upload_filename}")

    except Exception as e:
        print(f"S3 Upload failed: {e}")

if __name__ == "__main__":
    while True:
        upload_log()
        time.sleep(S3_SECONDS)