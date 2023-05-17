import base64
import hashlib
import os

from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger
from s3_multipart_upload.subcommands.config import (
  MultipartUploadConfig,
  UploadedPart,
  UploadFile,
  load_multipart_file,
  save_multipart_file,
)

logger = get_logger('upload_logger')

def upload_multipart(
  s3_client: S3Client,
  bucket: str,
  key: str,
  folder_path: str,
  prefix: str,
  config_path: str,
  starting_part_number: int | None
):
  # Determine if we need to initiate a new multipart upload 
  # or to continue with an existing multipart upload
  config = load_multipart_file(config_path)
  upload_id = None
  if config is None:
    logger.info(f'Initiating multipart upload in {config_path}')
    upload_id = _initiate_multipart_upload(s3_client, bucket, key)
    config = MultipartUploadConfig(bucket, key, upload_id, [])
    save_multipart_file(config_path, config)
  else:
    logger.info(f'Continuing multipart upload from {config_path}')
    upload_id = config.UploadId
    if not _is_multipart_in_progress(s3_client, bucket, upload_id):
      logger.warning(f'Upload id in {config_path} is either completed or aborted. No upload will be done.')
      return

  # Get the files that we want to upload and if user specifies
  # a starting part number, then we filter the files and only
  # upload the files from the specified part number and onward
  start_part_number = _get_start_part_number(config, starting_part_number)
  upload_files = _get_upload_files(folder_path, prefix)
  filtered_upload_files = _filter_file_paths(upload_files, start_part_number)

  skip_file_count = len(upload_files) - len(filtered_upload_files)
  if skip_file_count > 0:
    logger.warning(f'Skipped {skip_file_count}/{len(upload_files)} files')

  # Upload file one by one and save its ETag (returned from AWS)
  for upload_file in filtered_upload_files:
    file_path, part_number = upload_file.FilePath, upload_file.PartNumber
    md5 = _get_md5(file_path)

    logger.info(f'Uploading {part_number} - {file_path} - {md5}')
    with open(file_path, 'rb') as part_file:
      upload_response = s3_client.upload_part(
        Bucket=bucket, 
        Key=key,
        PartNumber=part_number,
        Body=part_file,
        UploadId=upload_id,
        ContentMD5=md5,
      )

    e_tag = upload_response['ETag'].replace('""', '')
    config.Parts.append(UploadedPart(e_tag, part_number))
    save_multipart_file(config_path, config)

  # Finally complete the multipart upload
  if len(filtered_upload_files) > 0:
    logger.info(f'Uploaded {len(filtered_upload_files)} part(s). Will now complete multipart upload')
    _complete_multipart_upload(s3_client, bucket, key, config, upload_id)

def _get_md5(file_path: str) -> str:
  with open(file_path, 'rb') as f:
    h = hashlib.md5()
    for chunk in f:
      h.update(chunk)
    return base64.b64encode(h.digest()).decode()

def _is_multipart_in_progress(s3_client: S3Client, bucket: str, upload_id: str):
  response = s3_client.list_multipart_uploads(Bucket=bucket)
  if 'Uploads' not in response:
    return False

  upload_ids = {upload['UploadId'] for upload in response['Uploads']}
  return upload_id in upload_ids

def _initiate_multipart_upload(s3_client: S3Client, bucket: str, key: str) -> str:
  response = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
  return response['UploadId']

def _complete_multipart_upload(s3_client: S3Client, bucket: str, key: str, config: MultipartUploadConfig, upload_id: str):
  config_dict = config.to_dict()
  parts_dict = {'Parts': config_dict['Parts']}

  s3_client.complete_multipart_upload(
    Bucket=bucket,
    Key=key,
    MultipartUpload=parts_dict,
    UploadId=upload_id
  )

def _get_upload_files(folder_path: str, prefix: str):
  file_paths = sorted([
    os.path.join(folder_path, file_name) for file_name in os.listdir(folder_path)
    if file_name.startswith(prefix)
  ])

  return [UploadFile(file_path, index + 1) for index, file_path in enumerate(file_paths)]

def _get_start_part_number(config: MultipartUploadConfig, starting_part_number: int | None):
  if starting_part_number is None:
    return config.Parts[-1].PartNumber + 1 if config.Parts else 1
  return starting_part_number

def _filter_file_paths(upload_files: list[UploadFile], starting_part_number: int):
  return [upload_file for upload_file in upload_files if upload_file.PartNumber >= starting_part_number]
