# S3 Multipart Upload
The purpose of this app is to do multipart upload to S3 such that if the
upload is ever interrupted, it knows where and how to pick up where the upload
was left off.

# Getting started
## Prerequisite
1. Install `pyenv`
1. Install `aws-vault` to set up your AWS credential

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
usage: main.py upload [-h] -b BUCKET -k KEY -f FILE_PATH -m META_FILE_PATH -a PARTS_FILE_PATH [-t THREAD_COUNT] [-s SPLIT_SIZE]

options:
  -h, --help            show this help message and exit
  -b BUCKET, --bucket BUCKET
                        S3 bucket
  -k KEY, --key KEY     S3 key in which also includes the file name
  -f FILE_PATH, --file-path FILE_PATH
                        Path to where the file is
  -m META_FILE_PATH, --meta-file-path META_FILE_PATH
                        Path to the multipart meta file
  -a PARTS_FILE_PATH, --parts-file-path PARTS_FILE_PATH
                        Path to the file containging the uploaded parts
  -t THREAD_COUNT, --thread-count THREAD_COUNT
                        Optionally specify number of threads to use
  -s SPLIT_SIZE, --split-size SPLIT_SIZE
                        Optionally specify split size (in bytes). Default is 5000000 bytes.
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
  -t 1
[2023-12-24 17:10:18,707 INFO] Initiating multipart upload in ./input_meta.json. (upload_multipart.py:50)
[2023-12-24 17:10:19,083 INFO] Will upload 100 chunks. (upload_multi_threading.py:76)
[2023-12-24 17:10:30,688 INFO] (6264107008) Done uploading 1 - tPQ7nlet6xsjSTnkJHCjNA==. Completed in 11 seconds. (upload_multi_threading.py:128)
[2023-12-24 17:10:30,909 INFO] (6264107008) Done uploading 2 - nYmdpQPUJZzoSONUm7gotw==. Completed in 11 seconds. (upload_multi_threading.py:128)
[2023-12-24 17:10:31,183 INFO] (6264107008) Done uploading 3 - 6u+l/o2aLsIxkYVTj8CVEw==. Completed in 12 seconds. (upload_multi_threading.py:128)

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
[2023-12-24 17:10:45,573 INFO] Continuing multipart upload from ./input_meta.json. (upload_multipart.py:55)
[2023-12-24 17:10:45,928 INFO] 3 parts have already been uploaded. (upload_multipart.py:72)
[2023-12-24 17:10:45,936 INFO] Will upload 997 chunks. (upload_multi_threading.py:76)
[2023-12-24 17:10:57,240 INFO] (6264107008) Done uploading 4 - vBVx1lx47qmhVIiEjO4VLQ==. Completed in 11 seconds. (upload_multi_threading.py:128)
[2023-12-24 17:10:57,240 INFO] (6264107008) Done uploading 5 - vBVx1lx47qmhVIiEjO4VLQ==. Completed in 11 seconds. (upload_multi_threading.py:128)
```

# How it works?
When you first start a brand new upload, the app will initiate a multipart upload
and save the bucket, key, split size, and upload id to the `META_FILE_PATH` file.
Then the app divides the file into multiple chunks based on the `split-size`, and
it uploads each chunk one by one using 1 thread, or in parallel if `THREAD_COUNT`
is greater than 1. After each chunk is uploaded, the app saves the response
data from AWS to `PARTS_FILE_PATH`. If the upload is ever interrupted, the app can
refer to the data it saves in `PARTS_FILE_PATH` to know where the upload was
interrupted.

The content of `META_FILE_PATH` looks something like this:
```json
{
  "bucket": "my-bucket",
  "key": "data/input.txt",
  "uploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--",
  "splitSize": 5000000
}
```

The content of `PARTS_FILE_PATH` is in [jsonl](https://jsonlines.org/) format, and it looks something like this:
```json
{"eTag": "5ca0dc854f8e8b356b0049144ed63056", "partNumber": 1, "uploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
{"eTag": "816c6ad180293f4cd937fc40bef4a4b5", "partNumber": 111, "uploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
{"eTag": "ac44217ac0c2b217a62a82b2fd45409e", "partNumber": 2, "uploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
{"eTag": "82ea33142a0fdabb8e8f6bc2966a499b", "partNumber": 112, "uploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
{"eTag": "0f5dacd6cb10a651031979609e3f668c", "partNumber": 3, "uploadId": "OQBrK98WoGKAzzd1NxXyimERi2gJR.hSo5Cn1_nETKIj6VCxI5y33HGX5rciJe_FAPxCxKuYjIQ_qDfptl1EoDb2v39rWIIrOb0QZpdLsG7i.cQr6j6w0OiujFraIay7.6peX7A_WWTv1kHniysEfA--"}
```

# References
* The logic of this app is based on this [AWS doc](https://repost.aws/knowledge-center/s3-multipart-upload-cli)
