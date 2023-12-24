import base64
import hashlib
import time
import os
from dataclasses import dataclass
import boto3
import threading

from multiprocessing.dummy import Lock
from multiprocessing.dummy import Pool as ThreadPool

from mypy_boto3_s3 import S3Client
from s3_multipart_upload.io.multipart_meta import MultipartUploadMeta
from s3_multipart_upload.io.uploaded_part import UploadedPart, UploadedPartFileWriter
from s3_multipart_upload.logger import get_logger

LOGGER = get_logger(__name__)
PARTS_FILE_LOCK = Lock()

@dataclass(frozen=True)
class UploadResult:
  start: int
  end: int
  part_number: int
  uploaded_part: UploadedPart | None = None
  failure: Exception | None = None

def upload_using_multi_threading(
  s3_client: S3Client,
  multipart_meta: MultipartUploadMeta,
  uploaded_part_numbers: set[int],
  thread_count: int,
  split_size: int,
  file_path: str,
  parts_file_path: str
) -> list[UploadResult]:
  with ThreadPool(processes=thread_count) as pool:
    file_size = os.path.getsize(file_path)
    with (
      open(file_path, 'rb') as file_handle,
      UploadedPartFileWriter(parts_file_path, 'a', True) as writer,
    ):
      start = 0
      part_number = 1
      results = []
      new_upload_count = 0

      while start < file_size:
        # Determine the offset, if it's the last one
        # then we make the offset the file_size
        offset = file_size if (start + split_size > file_size) else (start + split_size)
        file_handle.seek(offset)
        end = file_handle.tell()

        # Only upload parts that have not been uploaded yet
        if part_number not in uploaded_part_numbers:
          args = [
            s3_client,
            writer,
            multipart_meta,
            file_path,
            part_number,
            start,
            end
          ]

          result = pool.apply_async(_upload_part, args=args)
          results.append(result)
          new_upload_count +=1

        # Move to next chunk
        start = end
        part_number += 1

      LOGGER.info(f'Will upload {new_upload_count} chunks.')
      pool.close()
      pool.join()

      return [result.get() for result in results]

def _save_uploaded_part_thread_safe(
  writer: UploadedPartFileWriter,
  uploaded_part: UploadedPart
):
  """ Save the uploaded part in a thread-safe manner using `lock`. """
  try:
    PARTS_FILE_LOCK.acquire()
    writer.write(uploaded_part)
  finally:
    PARTS_FILE_LOCK.release()

def _upload_part(
  s3_client: S3Client,
  writer: UploadedPartFileWriter,
  multipart_meta: MultipartUploadMeta,
  file_path:str,
  part_number: int,
  start: int,
  end: int
) -> UploadedPart:
  thread_id = threading.get_ident()
  try:
    with open(file_path, 'rb') as file_handle:
      start_time = time.time()

      # Read only the specified chunk in the file
      size_to_read = end - start
      file_handle.seek(start)

      content = file_handle.read(size_to_read)
      md5 = _compute_md5(content)

      upload_response = s3_client.upload_part(
        Bucket=multipart_meta.Bucket, 
        Key=multipart_meta.Key,
        PartNumber=part_number,
        Body=content,
        UploadId=multipart_meta.UploadId,
        ContentMD5=md5,
      )
      
      e_tag = upload_response['ETag'].replace('"', '')
      uploaded_part = UploadedPart(e_tag, part_number, multipart_meta.UploadId)
      _save_uploaded_part_thread_safe(writer, uploaded_part)

      elapsed_seconds = int(time.time() - start_time)
      LOGGER.info(f'({thread_id}) Done uploading {part_number} - {md5}. Completed in {elapsed_seconds} seconds.')
      return UploadResult(start, end, part_number, uploaded_part)
  except Exception as e:
    LOGGER.error(f'({thread_id}) An error occurred during uploading. {e}.')
    return UploadResult(start, end, part_number, failure=e)

def _compute_md5(value: bytes):
  md5_hash = hashlib.md5(value)
  return base64.b64encode(md5_hash.digest()).decode()
