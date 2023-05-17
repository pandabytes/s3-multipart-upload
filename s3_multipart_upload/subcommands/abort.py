from mypy_boto3_s3 import S3Client

from s3_multipart_upload.logger import get_logger
from s3_multipart_upload.subcommands.config import load_multipart_file

logger = get_logger('abort_logger')

def abort_multipart_upload(s3_client: S3Client, config_path: str):
  config = load_multipart_file(config_path)
  if not config:
    logger.warning(f'Config file {config_path} not found or empty')
    return
  
  s3_client.abort_multipart_upload(Bucket=config.Bucket, Key=config.Key, UploadId=config.UploadId)
  logger.info(f'Aborted multipart upload in {config_path}')
