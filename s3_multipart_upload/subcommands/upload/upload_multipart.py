import os
from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger

from s3_multipart_upload.subcommands.upload.upload_file import UploadFile
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
  folder_path: str,
  prefix: str,
  meta_file_path: str,
  parts_file_path: str,
  thread_count: int | None
):
  # Determine if we need to initiate a new multipart upload 
  # or to continue with an existing multipart upload
  multipart_meta = load_multipart_meta_file(meta_file_path)
  if multipart_meta is None:
    LOGGER.info(f'Initiating multipart upload in {meta_file_path}.')
    upload_id = initiate_multipart_upload(s3_client, bucket, key)
    multipart_meta = MultipartUploadMeta(bucket, key, upload_id)
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

  # Check if the the parts' UploadId in the parts file is
  # the same as the one in the multipart meta file. So that
  # we can correlate the the 2 files, by using UploadId
  uploaded_parts_from_file = _get_uploaded_parts_from_file(parts_file_path)
  if _have_unrelated_parts(multipart_meta, uploaded_parts_from_file):
    LOGGER.error(f'{parts_file_path} has parts that have different UploadId from the UploadId in {meta_file_path}. '
                 f'Please remove the file {parts_file_path} and re-run the upload.')
    return

  # If no files found, simply exit
  upload_files = _get_upload_files(folder_path, prefix)
  upload_files_count = len(upload_files)
  if upload_files_count == 0:
    LOGGER.warning('No files found. Please make sure your folder path and prefix are correct.')
    return

  # First determine the number parts we need to upload.
  # It can be all the parts (new upload) or a subset
  # of the all the parts (from an existing upload)
  missing_part_numbers = _get_missing_part_numbers(uploaded_parts_from_file, upload_files_count, sort=True)
  missing_upload_files = _find_upload_files(upload_files, missing_part_numbers)
  LOGGER.info(f'Will upload {len(missing_upload_files)} missing parts out of {upload_files_count} total.')

  results = upload_using_multi_threading(s3_client, thread_count, missing_upload_files, multipart_meta, parts_file_path)
  failed_uploads = [result for result in results if result.Failure is not None]
  if failed_uploads:
    failed_part_numbers = [u.File.PartNumber for u in failed_uploads]
    LOGGER.error(f'Upload ran into a problem when uploading with multi-threading. Failed part numbers: {failed_part_numbers}.')
    return

  # At this point, the upload is done and we
  # need to refresh this list from the file
  uploaded_parts_from_file = _get_uploaded_parts_from_file(parts_file_path)

  # Finally, complete the multipart upload  
  if uploaded_parts_from_file:
    LOGGER.info(f'Uploaded {len(missing_upload_files)} part(s) in this run. Will now complete multipart upload.')
    complete_multipart_upload(s3_client, multipart_meta, uploaded_parts_from_file)

def _get_upload_files(folder_path: str, prefix: str):
  """ Return a list of sorted upload file objects. """
  file_paths = sorted([
    os.path.join(folder_path, file_name) for file_name in os.listdir(folder_path)
    if file_name.startswith(prefix)
  ])

  return [UploadFile(file_path, index + 1) for index, file_path in enumerate(file_paths)]

def _get_uploaded_parts_from_file(parts_file_path: str) -> list[UploadedPart]:
  with UploadedPartFileReader(parts_file_path) as reader:
    return [uploaded_part for uploaded_part in reader.read()]

def _get_missing_part_numbers(uploaded_parts: list[UploadedPart], expect_total: int, sort: bool = True):
  if len(uploaded_parts) > expect_total:
    raise ValueError('Size of uploaded_parts must be less than or equal to expect_total.')

  sorted_parts = uploaded_parts
  if sort:
    sorted_parts = sorted(uploaded_parts, key=lambda p: p.PartNumber)

  part_numbers = {part.PartNumber for part in sorted_parts}
  expect_part_numbers = {number for number in range(1, expect_total + 1)}

  missing_numbers = set(expect_part_numbers).difference(part_numbers)
  return missing_numbers

def _find_upload_files(upload_files: list[UploadFile], part_numbers: set[int]) -> list[UploadFile]:
  if not upload_files or not part_numbers:
    return []
  
  return [upload_file for upload_file in upload_files if upload_file.PartNumber in part_numbers]

def _have_unrelated_parts(multipart_meta: MultipartUploadMeta, uploaded_parts: list[UploadedPart]) -> bool:
  """ Unrelated parts are parts that have different `UploadId` from the `UploadId`
      in `multipart_meta`.
  """
  if not uploaded_parts:
    return False
  
  for uploaded_part in uploaded_parts:
    if uploaded_part.UploadId != multipart_meta.UploadId:
      return True
  return False
