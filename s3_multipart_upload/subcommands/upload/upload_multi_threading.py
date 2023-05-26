import threading
from multiprocessing.dummy import Lock
from multiprocessing.dummy import Pool as ThreadPool

from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger
from s3_multipart_upload.subcommands.io.multipart_meta import \
    MultipartUploadMeta
from s3_multipart_upload.subcommands.io.uploaded_part import (
    UploadedPart, UploadedPartFileWriter)
from s3_multipart_upload.subcommands.upload.upload_file import UploadFile

LOGGER = get_logger(__name__)
PARTS_FILE_LOCK = Lock()

def upload_using_multi_threading(
    s3_client: S3Client,
    thread_count: int,
    upload_files: list[UploadFile],
    multipart_meta: MultipartUploadMeta,
    parts_file_path: str
) -> list[Exception | None]:
  """ """
  if len(upload_files) == 0:
    LOGGER.info('No files to be uploaded.')
    return

  LOGGER.info(f'Uploading with {thread_count} threads.')

  with ThreadPool(thread_count) as thread_pool:
    with UploadedPartFileWriter(parts_file_path, 'a', True) as writer:
      args = [
        (s3_client, upload_file, multipart_meta, writer)
        for upload_file in upload_files  
      ]      
      return thread_pool.starmap(_upload_part_thread, args)

def _save_multipart_file_thread_safe(writer: UploadedPartFileWriter, uploaded_part: UploadedPart):
  """ Save the uploaded part in a thread-safe manner using `lock`. """
  try:
    PARTS_FILE_LOCK.acquire()
    writer.write(uploaded_part)
  finally:
    PARTS_FILE_LOCK.release()

def _upload_part_thread(s3_client: S3Client, upload_file: UploadFile, multipart_meta: MultipartUploadMeta, writer: UploadedPartFileWriter) -> Exception | None:
  """ Upload the file and upon successful returns None, upon failure returns 
      the raised Exception. This is the unit of work for each thread.
  """
  thread_id = threading.get_ident()
  file_path, part_number, md5 = upload_file.FilePath, upload_file.PartNumber, upload_file.MD5

  try:
    with open(file_path, 'rb') as part_file:
      upload_response = s3_client.upload_part(
        Bucket=multipart_meta.Bucket, 
        Key=multipart_meta.Key,
        PartNumber=part_number,
        Body=part_file,
        UploadId=multipart_meta.UploadId,
        ContentMD5=md5,
      )

      e_tag = upload_response['ETag'].replace('"', '')
      _save_multipart_file_thread_safe(writer, UploadedPart(e_tag, part_number))
      
      LOGGER.info(f'({thread_id}) Done uploading {part_number} - {file_path} - {md5}')
  except Exception as ex:
    LOGGER.error(f'({thread_id}) An error occurred during uploading. {ex}.')
    return ex
