from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger
from s3_multipart_upload.subcommands.io.multipart_meta import load_multipart_meta_file

logger = get_logger('abort_logger')

def abort_multipart_upload(s3_client: S3Client, meta_file_path: str):
  config = load_multipart_meta_file(meta_file_path)
  if not config:
    logger.error(f'Meta file {meta_file_path} not found or empty.')
    return
  
  s3_client.abort_multipart_upload(Bucket=config.Bucket, Key=config.Key, UploadId=config.UploadId)
  logger.info(f'Aborted multipart upload in {meta_file_path}.')
