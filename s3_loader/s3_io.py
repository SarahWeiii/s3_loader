import s3fs
import os
import json
# Enable OpenEXR support
# Must be set before importing cv2
os.environ['OPENCV_IO_ENABLE_OPENEXR']='1'
import cv2
import numpy as np
from PIL import Image
import boto3
import io
from botocore.exceptions import ClientError
import time

def s3_init(s3_url='https://s3-haosu.nrp-nautilus.io'):
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        raise ValueError('AWS_ACCESS_KEY_ID not set')
    if 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise ValueError('AWS_SECRET_ACCESS_KEY not set')
    s3 = boto3.client(
        's3',
        endpoint_url=s3_url,
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )
    return s3

def file_exists_in_s3(s3, s3_path):
    bucket_name, key = separate_bucket_key(s3_path)
    try:
        # Attempt to retrieve metadata about the object
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        # If a ClientError is thrown, check if it was because the file does not exist
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # Re-raise the exception if it was a different error
            raise

def list_files_in_folder(s3, s3_path):
    bucket_name, key = separate_bucket_key(s3_path)
    if not key.endswith('/'):
        key += '/'
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=key)
    
    files = []
    for page in pages:
        if 'Contents' in page:
            for item in page['Contents']:
                files.append(f"{bucket_name}/{item['Key']}")
    
    return files

def read_file_as_bytes(s3, bucket, key, max_retries=3, backoff_factor=1.5):
    """Read an S3 object as bytes directly with retry logic."""
    attempts = 0
    while attempts < max_retries:
        try:
            # Attempt to retrieve the object
            response = s3.get_object(Bucket=bucket, Key=key)
            # Read the contents of the file as bytes
            return response['Body'].read()
        except Exception as e:
            attempts += 1
            if attempts >= max_retries:
                print(f"Failed to read file from S3 after {max_retries} attempts: {e}")
                return None
            else:
                print(f"Attempt {attempts}: Failed to read file from S3, retrying in {backoff_factor**attempts} seconds...")
                time.sleep(backoff_factor ** attempts)  # Exponential backoff
    
def separate_bucket_key(s3_path):
    bucket_name = s3_path.split('/')[0]
    key = s3_path.replace(bucket_name, "")
    assert len(key) > 0, "Key is empty"
    if key[0] == '/':
        key = key[1:]
    return bucket_name, key


def load_s3_json(s3, s3_path):
    bucket_name, key = separate_bucket_key(s3_path)
    bytes = read_file_as_bytes(s3, bucket_name, key)
    return json.loads(bytes)

def load_s3_txt(s3, s3_path):
    bucket_name, key = separate_bucket_key(s3_path)
    bytes = read_file_as_bytes(s3, bucket_name, key)
    return bytes.decode('utf-8')

def load_s3_image(s3, s3_path):
    bucket_name, key = separate_bucket_key(s3_path)
    bytes = read_file_as_bytes(s3, bucket_name, key)
    image_stream = io.BytesIO(bytes)
    return Image.open(image_stream)

def load_s3_exr(s3, s3_path):
    bucket_name, key = separate_bucket_key(s3_path)
    bytes = read_file_as_bytes(s3, bucket_name, key)
    content = np.frombuffer(bytes, dtype=np.uint8)
    content = cv2.imdecode(content, cv2.IMREAD_UNCHANGED)
    return content



