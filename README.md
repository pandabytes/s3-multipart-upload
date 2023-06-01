# S3 Multipart Upload
The purpose of this app is to do multipart upload to S3 such that if the
upload is ever interrupted, it knows where and how to pick up where the upload
was left off.

# Getting started
## Prerequisite
1. Install `pyenv`
1. Install `aws-vault` to set up your AWS credential

To do multipart upload, we must first split the file into smaller files. 
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
`.python_version`: contains the Python version.

`.python-version`: used by pyenv and this is where we define the name of the virtual env.

1. Install the correct Python version: `cat .python_version | xargs pyenv install`
1. Create virtual env: `cat .python_version | xargs -I python_version pyenv virtualenv python_version s3-multipart-upload`
1. Install packages in the virtual env: `pip install -r requirements.txt`

## Run the app
Prior to running the app, make sure to use `aws-vault` to log in to your AWS profile.

```bash
$ python s3_multipart_upload/main.py upload --help
usage: main.py upload [-h] -b BUCKET -k KEY -d FOLDER_PATH -p PREFIX -m META_FILE_PATH -a PARTS_FILE_PATH [-t THREAD_COUNT]

options:
  -h, --help            show this help message and exit
  -b BUCKET, --bucket BUCKET
                        S3 bucket
  -k KEY, --key KEY     S3 key in which also includes the file name
  -d FOLDER_PATH, --folder-path FOLDER_PATH
                        Folder path where the split files are stored
  -p PREFIX, --prefix PREFIX
                        Prefix of the split files
  -m META_FILE_PATH, --meta-file-path META_FILE_PATH
                        Path to the multipart meta file
  -a PARTS_FILE_PATH, --parts-file-path PARTS_FILE_PATH
                        Path to the file containging the uploaded parts
  -t THREAD_COUNT, --thread-count THREAD_COUNT
                        Optionally specify number of threads to use
```

Example run
```bash
# First run gets interrupted
$ python s3_multipart_upload/main.py upload \                  
  -b my-bucket \
  -k "data/input.txt" \
  -d /Users/foo/data/ \
  -p input_data_ \
  -m ./input_meta.json \
  -a ./input_parts.jsonl \
  -t 2
[2023-05-31 19:23:52,269 - s3_multipart_upload.subcommands.upload.upload_multipart INFO] Initiating multipart upload in ./input_meta.json. (upload_multipart.py:40)
[2023-05-31 19:23:52,985 - s3_multipart_upload.subcommands.upload.upload_multipart INFO] Will upload 876 missing parts out of 876 total. (upload_multipart.py:77)
[2023-05-31 19:23:52,985 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] Uploading with 2 threads. (upload_multi_threading.py:36)
[2023-05-31 19:24:00,235 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] (6107148288) Done uploading 1 - /Users/foo/data/input_data_0000 - XKDchU+OizVrAEkUTtYwVg==. Completed in 7 seconds. (upload_multi_threading.py:79)
[2023-05-31 19:24:04,114 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] (6123974656) Done uploading 111 - /Users/foo/data/input_data_0110 - gWxq0YApP0zZN/xAvvSktQ==. Completed in 11 seconds. (upload_multi_threading.py:79)
[2023-05-31 19:24:04,476 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] (6107148288) Done uploading 2 - /Users/foo/data/input_data_0001 - rEQhesDCshemKoKy/UVAng==. Completed in 4 seconds. (upload_multi_threading.py:79)
[2023-05-31 19:24:07,858 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] (6123974656) Done uploading 112 - /Users/foo/data/input_data_0111 - guozFCoP2ruOj2vClmpJmw==. Completed in 3 seconds. (upload_multi_threading.py:79)
ERROR: Credential AWS refreshed, please log back in.

# After refreshing AWS credential, the app knows to pick up where it left off 
$ python s3_multipart_upload/main.py upload \                  
  -b my-bucket \
  -k "data/input.txt" \
  -d /Users/foo/data/ \
  -p input_data_ \
  -m ./input_meta.json \
  -a ./input_parts.jsonl \
  -t 2
[2023-05-31 19:27:27,625 - s3_multipart_upload.subcommands.upload.upload_multipart INFO] Continuing multipart upload from ./patient_meta.json. (upload_multipart.py:45)
[2023-05-31 19:27:28,140 - s3_multipart_upload.subcommands.upload.upload_multipart INFO] Will upload 814 missing parts out of 876 total. (upload_multipart.py:77)
[2023-05-31 19:27:28,141 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] Uploading with 2 threads. (upload_multi_threading.py:36)
[2023-05-31 19:27:31,599 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] (6120730624) Done uploading 32 - /Users/foo/data/input_data_0031 - zpuziiqU2Bz/PY4++udy6w==. Completed in 3 seconds. (upload_multi_threading.py:79)
[2023-05-31 19:27:34,657 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] (6120730624) Done uploading 33 - /Users/foo/data/input_data_0032 - Bxm9tBakZ4F3UVmg/KJjWw==. Completed in 3 seconds. (upload_multi_threading.py:79)
[2023-05-31 19:27:35,431 - s3_multipart_upload.subcommands.upload.upload_multi_threading INFO] (6137556992) Done uploading 165 - /Users/foo/data/input_data_0164 - PtsUyxBWLbax/Wr4eFj9fw==. Completed in 7 seconds. (upload_multi_threading.py:79)
```

# How it works?
When you first start a brand new upload, the app will initiate a multipart upload
and save the bucket, key, and upload id to the `META_FILE_PATH` file. Then the app
uses `FOLDER_PATH` and `PREFIX` to find the files to be uploaded, and it uploads
each file one by one using 1 thread, or in parallel if `THREAD_COUNT` is greater
than 1. After each file is uploaded, the app saves the response
data from AWS to `PARTS_FILE_PATH`. If the upload is ever interrupted, the app can
refer to the data it saves in `PARTS_FILE_PATH` to know where the upload was
interrupted.

The content of `META_FILE_PATH` looks something like this:
```json
{
  "Bucket": "my-bucket",
  "Key": "data/input.txt",
  "UploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"
}
```

The content of `PARTS_FILE_PATH` is in [jsonl](https://jsonlines.org/) format, and it looks something like this:
```json
{"ETag": "5ca0dc854f8e8b356b0049144ed63056", "PartNumber": 1, "UploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
{"ETag": "816c6ad180293f4cd937fc40bef4a4b5", "PartNumber": 111, "UploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
{"ETag": "ac44217ac0c2b217a62a82b2fd45409e", "PartNumber": 2, "UploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
{"ETag": "82ea33142a0fdabb8e8f6bc2966a499b", "PartNumber": 112, "UploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
{"ETag": "0f5dacd6cb10a651031979609e3f668c", "PartNumber": 3, "UploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
```

# References
* The logic of this app is based on this [AWS doc](https://repost.aws/knowledge-center/s3-multipart-upload-cli)
