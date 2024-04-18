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

import requests
import s3_loader.hacks3lb_useast
from imgsvc.client import BatchRequester, ProcessRequest

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

# def read_file_as_bytes(s3, bucket, key, max_retries=3, backoff_factor=0.1):
#     """Read an S3 object as bytes directly with retry logic."""
#     attempts = 0
#     while attempts < max_retries:
#         try:
#             # Attempt to retrieve the object
#             response = s3.get_object(Bucket=bucket, Key=key)
#             # Read the contents of the file as bytes
#             return response['Body'].read()
#         except Exception as e:
#             if attempts >= max_retries:
#                 print(f"Failed to read file from S3 after {max_retries} attempts: {e}")
#                 return None
#             else:
#                 print(f"Attempt {attempts}: Failed to read file from S3, retrying in {backoff_factor + attempts * 0.5} seconds...")
#                 time.sleep(backoff_factor + attempts * 0.5)  # Exponential backoff
#             attempts += 1

def sign_for_file(s3, bucket, key, expire_time=600):
    return s3.generate_presigned_url('get_object',
                                     Params={'Bucket': bucket,
                                             'Key': key},
                                     ExpiresIn=expire_time)

def read_file_as_bytes(s3, bucket, key):
    signurl = sign_for_file(s3, bucket, key)
    return requests.get(signurl).content
    
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

def image_grid(imgs, rows, cols):
    assert len(imgs) == rows*cols

    w, h = imgs[0].size
    grid = Image.new('RGBA', size=(cols*w, rows*h))
    grid_w, grid_h = grid.size
    
    for i, img in enumerate(imgs):
        grid.paste(img, box=(i%cols*w, i//cols*h))
    return grid

def load_s3_image_batch(s3, s3_paths, tgt_size=(256, 256)):
    assert s3.meta.endpoint_url == 'http://rook-ceph-rgw-haosu.rook-haosu'
    br = BatchRequester("https://haosu-imgsvc.nrp-nautilus.io")
    src_nors = [sign_for_file(s3, *separate_bucket_key(s3_path)) for s3_path in s3_paths]
    reqs = [
        ProcessRequest(
            'pil', src_nors[i], 'png'
        ).resize(tgt_size[0], tgt_size[1], Image.LANCZOS)
        for i in range(len(s3_paths))
    ]
    results = br.get(reqs)
    images = [
        Image.open(io.BytesIO(r)) for r in results
    ]
    # img_grid = image_grid(images, 1, len(s3_paths))

    return images

# TODO: fix bug when loading depth
def load_s3_exr_batch(s3, s3_paths, tgt_size=(256, 256)):
    assert s3.meta.endpoint_url == 'http://rook-ceph-rgw-haosu.rook-haosu'
    br = BatchRequester("https://haosu-imgsvc.nrp-nautilus.io")
    src_nors = [sign_for_file(s3, *separate_bucket_key(s3_path)) for s3_path in s3_paths]
    reqs = [
        ProcessRequest(
            'cv2', src_nors[i], 'exr'
        ).resize(tgt_size[0], tgt_size[1], cv2.INTER_NEAREST)
        for i in range(len(s3_paths))
    ]
    results = br.get(reqs)
    images = [
        Image.fromarray(
            (cv2.imdecode(np.frombuffer(r, dtype=np.uint8), cv2.IMREAD_UNCHANGED) * 255).astype(np.uint8),
            mode="RGBA"
        ) for r in results
    ]
    img_grid = image_grid(images, 1, len(s3_paths))

    return img_grid





# import io
# import os
# import cv2
# import boto3
# import numpy
# import subprocess
# from PIL import Image
# from imgsvc.client import BatchRequester, ProcessRequest


# # server_process = subprocess.Popen(
# #     ["imgsvc-server"],
# #     env=dict(**os.environ, IMGSVC_ALLOW_URL_HOSTS="s3-haosu.nrp-nautilus.io")
# # )


# def image_grid(imgs, rows, cols):
#     assert len(imgs) == rows*cols

#     w, h = imgs[0].size
#     grid = Image.new('RGBA', size=(cols*w, rows*h))
#     grid_w, grid_h = grid.size
    
#     for i, img in enumerate(imgs):
#         grid.paste(img, box=(i%cols*w, i//cols*h))
#     return grid


# def sign_for_file(path, expire_time=600) -> str:
#     return s3.generate_presigned_url('get_object',
#                                      Params={'Bucket': 'meshlrm-objaverse',
#                                              'Key': path},
#                                      ExpiresIn=expire_time)


# key = ''
# secret = ''
# s3 = boto3.client(
#     's3',
#     endpoint_url='http://rook-ceph-rgw-haosu.rook-haosu',
#     aws_access_key_id=key,
#     aws_secret_access_key=secret
# )
# br = BatchRequester("https://haosu-imgsvc.nrp-nautilus.io")
# src_nors = [sign_for_file("views_000074a334c541878360457c672b6c2e/normal_00_in_%02d.exr" % i) for i in range(64)]
# src_cols = [sign_for_file("views_000074a334c541878360457c672b6c2e/color_00_in_%02d.png" % i) for i in range(64)]
# reqs = [
#     ProcessRequest(
#         'cv2', src_nors[i], 'exr'
#     ).resize(256, 256, cv2.INTER_NEAREST)
#     for i in range(64)
# ] + [
#     ProcessRequest(
#         'pil', src_cols[i], 'png'
#     ).resize(256, 256, Image.LANCZOS)
#     for i in range(64)
# ]
# print("Sending request")
# results = br.get(reqs)
# print("Request return")
# images = [
#     Image.fromarray(
#         (cv2.imdecode(numpy.frombuffer(r, dtype=numpy.uint8), cv2.IMREAD_UNCHANGED) * 255).astype(numpy.uint8),
#         mode="RGBA"
#     ) for r in results[:64]
# ] + [
#     Image.open(io.BytesIO(r)) for r in results[64:]
# ]
# image_grid(images, 8, 16).show()
# # server_process.terminate()



