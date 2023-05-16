from mypy_boto3_s3 import S3Client

def abort_multipart_upload(s3_client: S3Client, bucket: str, key: str, upload_id: str):
  return s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
