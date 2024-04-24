# s3 loader

This repo contains the library that load various formats of data from s3 bucket

## Installation
```
pip install git+https://github.com/eliphatfs/imgsvc
pip install git+https://github.com/SarahWeiii/s3_loader.git
```

Writing these to your environment:
```
export AWS_ACCESS_KEY_ID=[your_key]
export AWS_SECRET_ACCESS_KEY=[your_secret]
export AWS_ENDPOINT_URL=https://s3-haosu.nrp-nautilus.io
# If inside nautilus cluster:
# export AWS_ENDPOINT_URL=http://rook-ceph-rgw-haosu.rook-haosu	
```

## Functions
### Init s3 client
* s3_init(s3_url): return an s3 client using s3_url
### Load a single file
* load_s3_json(s3, s3_path)
* load_s3_txt(s3, s3_path)
* load_s3_image(s3, s3_path)
* load_s3_exr(s3, s3_path)
### Load batch data
Note: requires using s3_client with `http://rook-ceph-rgw-haosu.rook-haosu` endpoint
* load_s3_image_batch(s3, s3_paths, tgt_size): return a list of resized images (suggest len(s3_paths) >= 8)
* load_s3_exr_batch(s3, s3_paths, tgt_size): return a list of resized exr files (suggest len(s3_paths) >= 8)
### Upload / Download data
* upload_file_to_s3(s3, local_path, s3_path, quiet=False)
* download_file_from_s3(s3, local_path, s3_path, quiet=False)
### Others
* list_files_in_folder(s3, s3_path): return a list of files under s3_path
* file_exists_in_s3(s3, s3_path): check if a file exist

## Threaded Dataloader
Replace pytorch `DataLoader` with the following one:
```
from s3_loader.threaded_dataloader import ThreadedDataLoader as DataLoader
```