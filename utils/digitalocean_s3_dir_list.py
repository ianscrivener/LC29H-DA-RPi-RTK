import os
import boto3
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment variables from .env
load_dotenv()

S3_HOST = os.getenv('S3_HOST')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_KEY = os.getenv('S3_KEY')
S3_SECRET = os.getenv('S3_SECRET')
S3_SECONDS = int(os.getenv('S3_SECONDS', '180'))
LOG_FILE = os.getenv('LOG_FILE', 'rtk_log.txt')

S3_BUCKET_RAW = os.getenv('S3_BUCKET_RAW')
S3_BUCKET_RAW_PROCESSED = os.getenv('S3_BUCKET_RAW_PROCESSED')
S3_BUCKET_GEOJSONL = os.getenv('S3_BUCKET_GEOJSONL')
S3_BUCKET_GEOPARQUET = os.getenv('S3_BUCKET_GEOPARQUET')

# Parse bucket and endpoint from S3_HOST
parsed = urlparse(S3_HOST)
bucket = S3_BUCKET
endpoint_url = f"{parsed.scheme}://{parsed.netloc}"


print(f"S3_BUCKET_RAW: {S3_BUCKET_RAW}")
print(f"S3_BUCKET_RAW_PROCESSED: {S3_BUCKET_RAW_PROCESSED}")
print(f"S3_BUCKET_GEOJSONL: {S3_BUCKET_GEOJSONL}")
print(f"S3_BUCKET_GEOPARQUET: {S3_BUCKET_GEOPARQUET}")

def main():
    """
    List files in the S3 RAW bucket and print their details.
    """

    print(f"Connecting to S3: {endpoint_url}/{S3_BUCKET_RAW}")

    try:
        # Create S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
            endpoint_url=endpoint_url
        )

        # list buckets
        response = s3.list_buckets()
        print("Buckets:")
        for bucket in response['Buckets']:
            print(f"  - {bucket['Name']}")  
            


    except Exception as e:
        print(f"Error connecting to S3 or listing objects: {e}")

if __name__ == "__main__":
    main()