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

