import os
from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger

from s3_multipart_upload.subcommands.upload.s3_multipart_upload import (
  complete_multipart_upload,
  initiate_multipart_upload,
  is_multipart_in_progress,
)
from s3_multipart_upload.io.multipart_meta import (
  MultipartUploadMeta,
  load_multipart_meta_file,
  save_multipart_meta_file, 
)
from s3_multipart_upload.io.uploaded_part import (
  UploadedPart,
  UploadedPartFileReader,
)
from s3_multipart_upload.subcommands.upload.upload_multi_threading import upload_using_multi_threading

LOGGER = get_logger(__name__)

def upload_multipart(
  s3_client: S3Client,
  bucket: str,
  key: str,
  file_path: str,
  meta_file_path: str,
  parts_file_path: str,
  thread_count: int,
  split_size: int
) -> bool:
  if not os.path.exists(file_path):
    LOGGER.info(f'File {file_path} does not exist. Exiting...')
    return False

  file_size = os.path.getsize(file_path)
  if not file_size:
    LOGGER.info('File is empty. Exiting...')
    return False

  # Determine if we need to initiate a new multipart upload 
  # or to continue with an existing multipart upload
  multipart_meta = load_multipart_meta_file(meta_file_path)
  if multipart_meta is None:
    LOGGER.info(f'Initiating multipart upload in {meta_file_path}.')
    upload_id = initiate_multipart_upload(s3_client, bucket, key)
    multipart_meta = MultipartUploadMeta(bucket, key, upload_id, split_size)
    save_multipart_meta_file(meta_file_path, multipart_meta)
  else:
    LOGGER.info(f'Continuing multipart upload from {meta_file_path}.')
    
    # Multipart upload started but there are some verifications we need to check
    if bucket != multipart_meta.bucket or key != multipart_meta.key:
      LOGGER.error(f'bucket or key does not match with Bucket or Key in {meta_file_path}.')
      return False
    
    if split_size != multipart_meta.split_size:
      LOGGER.error(f'split-size does not match with splitSize in {meta_file_path}.')
      return False

    if not is_multipart_in_progress(s3_client, multipart_meta.bucket, multipart_meta.upload_id):
      LOGGER.error(f'Upload id in {meta_file_path} is either invalid, completed, or aborted.')
      return False

  uploaded_parts = _get_uploaded_parts(parts_file_path)
  if uploaded_parts:
    LOGGER.info(f'{len(uploaded_parts)} parts have already been uploaded.')

  # Check if the the parts' UploadId in the parts file is
  # the same as the one in the multipart meta file. So that
  # we can correlate the the 2 files, by using UploadId
  if _have_unrelated_parts(multipart_meta, uploaded_parts):
    LOGGER.error(f'{parts_file_path} has parts that have different uploadId from '
                 f'the uploadId in {meta_file_path}. Please remove the file '
                 f'{parts_file_path} and re-run the upload.')
    return False

  part_numbers = {uploaded_part.part_number for uploaded_part in uploaded_parts}
  upload_results = upload_using_multi_threading(
    s3_client=s3_client,
    multipart_meta=multipart_meta,
    uploaded_part_numbers=part_numbers,
    thread_count=thread_count,
    split_size=split_size,
    file_path=file_path,
    parts_file_path=parts_file_path,
  )

  failed_uploads = [upload_result
                    for upload_result in upload_results
                    if upload_result.failure is not None]

  if failed_uploads:
    failed_part_numbers = [u.part_number for u in failed_uploads]
    LOGGER.error('Upload ran into a problem when uploading with '
                 f'multi-threading. Failed part numbers: {failed_part_numbers}.')
    return False

  # Construct the full list of all the uploaded
  # parts: existing uploads come from the parts file and
  # new uploads come from the upload above
  for upload_result in upload_results:
    uploaded_parts.append(upload_result.uploaded_part)

  LOGGER.info('Will now complete multipart upload.')
  complete_multipart_upload(s3_client, multipart_meta, uploaded_parts)

  LOGGER.info(f'Upload to "s3://{bucket}/{key}" completed successfully.')
  return True

def _get_uploaded_parts(parts_file_path: str) -> list[UploadedPart]:
  """ If `parts_file_path` does not exist, then return an empty list. """
  if not os.path.exists(parts_file_path):
    return []

  with UploadedPartFileReader(parts_file_path) as reader:
    return [uploaded_part for uploaded_part in reader.read()]

def _have_unrelated_parts(multipart_meta: MultipartUploadMeta, uploaded_parts: list[UploadedPart]) -> bool:
  """ Unrelated parts are parts that have different `uploadId` from the `uploadId`
      in `multipart_meta`.
  """
  if not uploaded_parts:
    return False

  for uploaded_part in uploaded_parts:
    if uploaded_part.upload_id != multipart_meta.upload_id:
      return True
  return False
