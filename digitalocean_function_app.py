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


# Parse bucket and endpoint from S3_HOST
parsed = urlparse(S3_HOST)
bucket = S3_BUCKET
endpoint_url = f"{parsed.scheme}://{parsed.netloc}"