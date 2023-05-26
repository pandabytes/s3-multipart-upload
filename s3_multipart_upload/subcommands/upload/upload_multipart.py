import os
from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger

from s3_multipart_upload.subcommands.upload.upload_file import UploadFile
from s3_multipart_upload.subcommands.upload.s3_multipart_upload import (
  complete_multipart_upload,
  initiate_multipart_upload,
  is_multipart_in_progress,
)
from s3_multipart_upload.subcommands.upload.io.multipart_meta import (
  MultipartUploadMeta,
  load_multipart_meta_file,
  save_multipart_meta_file, 
)
from s3_multipart_upload.subcommands.upload.io.uploaded_part import (
  UploadedPart,
  UploadedPartFileReader,
  UploadedPartFileWriter,
)


LOGGER = get_logger(__name__)

def upload_multipart(
  s3_client: S3Client,
  bucket: str,
  key: str,
  folder_path: str,
  prefix: str,
  meta_file_path: str,
  parts_file_path: str,
  starting_part_number: int | None
):
  # Determine if we need to initiate a new multipart upload 
  # or to continue with an existing multipart upload
  multipart_meta = load_multipart_meta_file(meta_file_path)
  if multipart_meta is None:
    LOGGER.info(f'Initiating multipart upload in {meta_file_path}.')
    upload_id = initiate_multipart_upload(s3_client, bucket, key)
    multipart_meta = MultipartUploadMeta(bucket, key, upload_id)

    # Save these data to disk
    # save_uploaded_parts_file(parts_file_path, [])
    save_multipart_meta_file(meta_file_path, multipart_meta)
  else:
    LOGGER.info(f'Continuing multipart upload from {meta_file_path}.')
    
    # Multipart upload started but there are some verifications we need to check
    if bucket != multipart_meta.Bucket or key != multipart_meta.Key:
      LOGGER.error(f'bucket or key does not match with Bucket or Key in {meta_file_path}.')
      return

    if not is_multipart_in_progress(s3_client, multipart_meta.Bucket, multipart_meta.UploadId):
      LOGGER.error(f'Upload id in {meta_file_path} is either invalid, completed, or aborted.')
      return

  # Get the files that we want to upload and if user specifies
  # a starting part number, then we filter the files and only
  # upload the files from the specified part number and onward
  start_part_number = _get_start_part_number(parts_file_path, starting_part_number)
  upload_files = _get_upload_files(folder_path, prefix)
  filtered_upload_files = _filter_file_paths(upload_files, start_part_number)

  upload_files_count = len(upload_files)
  if upload_files_count == 0:
    LOGGER.warning(f'No files found. Please make sure your folder path and prefix are correct.')
    return

  skip_file_count = len(upload_files) - len(filtered_upload_files)
  if skip_file_count > 0:
    LOGGER.warning(f'Skipped {skip_file_count}/{upload_files_count} files.')

  # Upload file one by one and save its ETag (returned from AWS)
  with UploadedPartFileWriter(parts_file_path, 'a', True) as writer:
    for upload_file in filtered_upload_files:
      file_path, part_number, md5 = upload_file.FilePath, upload_file.PartNumber, upload_file.MD5

      LOGGER.info(f'Uploading {part_number}/{upload_files_count} - {file_path} - {md5}')
      e_tag = _upload_part(s3_client, file_path, part_number, md5, multipart_meta)
      uploaded_part = UploadedPart(e_tag, part_number)
      writer.write(uploaded_part)

  # Finally complete the multipart upload
  if len(filtered_upload_files) > 0:
    LOGGER.info(f'Uploaded {len(filtered_upload_files)} part(s). Will now complete multipart upload.')
    parts = _get_parts(parts_file_path)
    complete_multipart_upload(s3_client, multipart_meta, parts)

def _upload_part(s3_client: S3Client, file_path: str, part_number: int, md5: str, multipart_meta: MultipartUploadMeta) -> str:
  """ Upload the file and returns the ETag string from the AWS response. """
  with open(file_path, 'rb') as part_file:
    upload_response = s3_client.upload_part(
      Bucket=multipart_meta.Bucket, 
      Key=multipart_meta.Key,
      PartNumber=part_number,
      Body=part_file,
      UploadId=multipart_meta.UploadId,
      ContentMD5=md5,
    )

  return upload_response['ETag'].replace('"', '')

def _get_upload_files(folder_path: str, prefix: str):
  file_paths = sorted([
    os.path.join(folder_path, file_name) for file_name in os.listdir(folder_path)
    if file_name.startswith(prefix)
  ])

  return [UploadFile(file_path, index + 1) for index, file_path in enumerate(file_paths)]

def _get_start_part_number(parts_file_path: str, starting_part_number: int | None):
  if starting_part_number is None:
    uploaded_parts = _get_parts(parts_file_path)
    if not uploaded_parts:
      return 1
    
    last_uploaded_part = max(uploaded_parts, key=lambda u: u.PartNumber)
    return last_uploaded_part.PartNumber + 1

  return starting_part_number

def _filter_file_paths(upload_files: list[UploadFile], starting_part_number: int):
  return [upload_file for upload_file in upload_files if upload_file.PartNumber >= starting_part_number]

def _get_parts(parts_file_path: str) -> list[UploadedPart]:
  with UploadedPartFileReader(parts_file_path) as reader:
    return [uploaded_part for uploaded_part in reader.read()]
