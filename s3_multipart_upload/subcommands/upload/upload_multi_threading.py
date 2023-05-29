import threading
import time
from dataclasses import dataclass
from multiprocessing.dummy import Lock
from multiprocessing.dummy import Pool as ThreadPool

from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger
from s3_multipart_upload.io.multipart_meta import MultipartUploadMeta
from s3_multipart_upload.io.uploaded_part import (
  UploadedPart,
  UploadedPartFileWriter,
)
from s3_multipart_upload.subcommands.upload.upload_file import UploadFile

@dataclass(frozen=True)
class UploadResult:
  File: UploadFile
  Failure: Exception | None = None

LOGGER = get_logger(__name__)
PARTS_FILE_LOCK = Lock()

def upload_using_multi_threading(
    s3_client: S3Client,
    thread_count: int,
    upload_files: list[UploadFile],
    multipart_meta: MultipartUploadMeta,
    parts_file_path: str
) -> list[UploadResult]:
  """ """
  if len(upload_files) == 0:
    return []

  LOGGER.info(f'Uploading with {thread_count} threads.')

  with ThreadPool(thread_count) as thread_pool:
    with UploadedPartFileWriter(parts_file_path, 'a', True) as writer:
      args = [
        (s3_client, upload_file, multipart_meta, writer)
        for upload_file in upload_files  
      ]  
      return thread_pool.starmap(_upload_part_thread, args)

def _save_uploaded_part_thread_safe(writer: UploadedPartFileWriter, uploaded_part: UploadedPart):
  """ Save the uploaded part in a thread-safe manner using `lock`. """
  try:
    PARTS_FILE_LOCK.acquire()
    writer.write(uploaded_part)
  finally:
    PARTS_FILE_LOCK.release()

def _upload_part_thread(s3_client: S3Client, upload_file: UploadFile, multipart_meta: MultipartUploadMeta, writer: UploadedPartFileWriter) -> UploadResult:
  """ Upload the file. This is the unit of work for each thread.
  """
  thread_id = threading.get_ident()

  try:
    file_path, part_number, md5 = upload_file.FilePath, upload_file.PartNumber, upload_file.MD5
    with open(file_path, 'rb') as part_file:
      start_time = time.time()

      upload_response = s3_client.upload_part(
        Bucket=multipart_meta.Bucket, 
        Key=multipart_meta.Key,
        PartNumber=part_number,
        Body=part_file,
        UploadId=multipart_meta.UploadId,
        ContentMD5=md5,
      )

      e_tag = upload_response['ETag'].replace('"', '')
      _save_uploaded_part_thread_safe(writer, UploadedPart(e_tag, part_number))

      end_time = time.time()
      elapsed_seconds = int(end_time - start_time)

      LOGGER.info(f'({thread_id}) Done uploading {part_number} - {file_path} - {md5}. Completed in {elapsed_seconds} seconds.')
      return UploadResult(upload_file)
  except Exception as ex:
    LOGGER.error(f'({thread_id}) An error occurred during uploading {upload_file}. {ex}.')
    return UploadResult(upload_file, ex)
