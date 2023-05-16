# S3 Multipart Upload
The purpose of this app is to do multipart upload to S3 such that if the
upload is ever interrupted, it knows where and how to pick up where the upload
was left off.

# Getting started
## Prerequisite
1. Install `pyenv`
1. Install `aws-vault` to set up your AWS credential

If we want to do multipart upload manually, we must first split the file into smaller files. 
```bash
# -d: use number suffix
# -a: indicate the length of the suffix (00, 000, etc...)
# -b: split by the file size. Example below splits the file by 40 MB chunk
# This will split large_file.csv into:
#   /user/foo/split_file_000
#   /user/foo/split_file_001
#   /user/foo/split_file_002
#   ...
split -d -a 3 -b40M larg_file.csv /user/foo/split_file_
```

## Set up repo
1. Use pyenv to install the Python version documented in `.python-version`
1. Run `pyenv virtualenv <python_version> s3-multipart-upload` to create a
new virtual env for this repo
1. Run `pip install -r requirements.txt`

## Run the app
Prior to running the app, make sure to use `aws-vault` to log in to your AWS profile.

```bash
$ python s3_multipart_upload/main.py upload --help
usage: main.py upload [-h] -b BUCKET -k KEY -d FOLDER_PATH -p PREFIX -c CONFIG_PATH [-n START_PART_NUMBER]

options:
  -h, --help            show this help message and exit
  -b BUCKET, --bucket BUCKET
                        S3 bucket
  -k KEY, --key KEY     S3 key in which also includes the file name
  -d FOLDER_PATH, --folder-path FOLDER_PATH
                        Folder path where the split files are stored
  -p PREFIX, --prefix PREFIX
                        Prefix of the split files
  -c CONFIG_PATH, --config-path CONFIG_PATH
                        Path to the config file
  -n START_PART_NUMBER, --start-part-number START_PART_NUMBER
                        Optionally specify which part number to start
```

Example run
```bash
# First run gets interrupted when part number 3 is being uploaded
$ python s3_multipart_upload/main.py upload \
  -b "my-bucket" \
  -k "data/input.txt" \
  -d "/Users/foo/data/" \
  -p "input_data_" \
  -c "./upload_config.json"
[2023-05-16 16:36:16,993 - upload_logger INFO] Initiating multipart upload in ./upload_config.json (upload.py:56)
[2023-05-16 16:36:17,567 - upload_logger INFO] Uploading 1 - data/input_data_000 - WoJl8ZYhQfgZAbdKYDwEFA== (upload.py:83)
[2023-05-16 16:36:38,089 - upload_logger INFO] Uploading 2 - data/input_data_001 - yrFuoEEu+fllFwckqiF81A== (upload.py:83)
[2023-05-16 16:36:53,138 - upload_logger INFO] Uploading 3 - data/input_data_002 - VOHsvnwf6GNLKTPNp1NMmw== (upload.py:83)
ERROR: Credential AWS refreshed, please log back in.

# After refreshing AWS credential, the app knows to pick up 
# where it left off which is part number 3
$ python s3_multipart_upload/main.py upload \
  -b "my-bucket" \
  -k "data/input.txt" \
  -d "/Users/foo/data/" \
  -p "input_data_" \
  -c "./upload_config.json"
[2023-05-16 16:37:30,762 - upload_logger INFO] Continuing multipart upload from ./upload_config.json (upload.py:61)
[2023-05-16 16:37:31,448 - upload_logger WARNING] Skipped 2/4 files (upload.py:76)
[2023-05-16 16:37:31,621 - upload_logger INFO] Uploading 3 - data/input_data_002 - VOHsvnwf6GNLKTPNp1NMmw== (upload.py:83)
[2023-05-16 16:37:31,621 - upload_logger INFO] Uploading 4 - data/input_data_003 - VOHsvnwf6GNLKTPNp2NMmw== (upload.py:83)
[2023-05-16 16:43:23,766 - upload_logger INFO] Uploaded 2 part(s). Will now complete multipart upload (upload.py:100)
```

# How it works?
When you first start a brand new upload, the app will initiate a multipart upload
and save the bucket, key, and upload id to the `CONFIG_PATH` file. Then the app
uses `FOLDER_PATH` and `PREFIX` to find the files to be uploaded, and it uploads
each file one by one. After each file is uploaded, the app saves the response
data from AWS to `CONFIG_PATH`. If the upload is ever interrupted, the app can
refer to the data it saves in `CONFIG_PATH` to know where the upload was
interrupted.

The content of `CONFIG_PATH` looks something like this:
```json
{
  "Bucket": "my-bucket",
  "Key": "data/input.txt",
  "UploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--",
  "Parts": [
    {
      "ETag": "\"c449e066715cff5f5597b750bdfe1807\"",
      "PartNumber": 1
    },
    {
      "ETag": "\"c449e066715cff5f5597b750bdfe1808\"",
      "PartNumber": 2
    }
  ]
}
```

# References
* The logic of this app is based on this [AWS doc](https://repost.aws/knowledge-center/s3-multipart-upload-cli)
