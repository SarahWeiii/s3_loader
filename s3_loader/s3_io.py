import s3fs
import os
import json
# Enable OpenEXR support
# Must be set before importing cv2
os.environ['OPENCV_IO_ENABLE_OPENEXR']='1'
import cv2
import numpy as np
from PIL import Image

def s3_init(s3_url='https://s3-haosu.nrp-nautilus.io'):
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        raise ValueError('AWS_ACCESS_KEY_ID not set')
    if 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise ValueError('AWS_SECRET_ACCESS_KEY not set')
    
    s3 = s3fs.S3FileSystem(
        endpoint_url=s3_url,
        key=os.environ['AWS_ACCESS_KEY_ID'],
        secret=os.environ['AWS_SECRET_ACCESS_KEY'],
    )
    return s3

def load_s3_json(s3, s3_path, block_size=16 * 1024 * 1024):
    return json.loads(s3.open(s3_path, block_size=block_size).read())

def load_s3_txt(s3, s3_path, block_size=16 * 1024 * 1024):
    return s3.open(s3_path, block_size=block_size).read().decode('utf-8')

def load_s3_image(s3, s3_path, block_size=16 * 1024 * 1024):
    return Image.open(s3.open(s3_path, block_size=block_size))

def load_s3_exr(s3, s3_path, block_size=16 * 1024 * 1024):
    content = s3.open(s3_path, block_size=block_size).read()
    content = np.frombuffer(content, dtype=np.uint8)
    content = cv2.imdecode(content, cv2.IMREAD_UNCHANGED)

    return content



