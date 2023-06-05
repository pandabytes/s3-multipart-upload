from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger
from s3_multipart_upload.io.multipart_meta import load_multipart_meta_file

LOGGER = get_logger(__name__)

def abort_multipart_upload(s3_client: S3Client, meta_file_path: str):
  multipart_meta = load_multipart_meta_file(meta_file_path)
  if not multipart_meta:
    LOGGER.error(f'Meta file {meta_file_path} not found or empty.')
    return False
  
  s3_client.abort_multipart_upload(Bucket=multipart_meta.Bucket, Key=multipart_meta.Key, UploadId=multipart_meta.UploadId)
  LOGGER.info(f'Aborted multipart upload in {meta_file_path}.')
  return True
