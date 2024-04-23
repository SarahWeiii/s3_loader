from setuptools import setup, find_packages

setup(
    name='s3_loader',
    version='0.1',
    packages=find_packages(),
    description='A flash s3 dataloader for PyTorch',
    install_requires=[
        'boto3',
        'opencv-python',
        'Pillow',
        'typing_extensions',
    ]
)
