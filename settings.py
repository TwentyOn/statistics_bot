import os
from dotenv import load_dotenv

load_dotenv()

tg_token = os.getenv('TG_TOKEN')
ym_token = os.getenv('YM_TOKEN')

DB_USER = os.getenv('DB_USER')
DB_NAME = os.getenv('DB_NAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
SECRET_KEY = os.getenv('S3_SECRET_KEY')
OUTER_ENDPOINT_URL = os.getenv('S3_OUTER_ENDPOINT_URL')
MINIO_SECURE = os.getenv('S3_SECURE')
