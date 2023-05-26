from mypy_boto3_s3 import S3Client
from s3_multipart_upload.subcommands.upload.io.multipart_meta import MultipartUploadMeta
from s3_multipart_upload.subcommands.upload.io.uploaded_part import UploadedPart


def complete_multipart_upload(s3_client: S3Client, meta: MultipartUploadMeta, parts: list[UploadedPart]):
  parts_dict = {'Parts': [part.to_dict() for part in parts]}

  s3_client.complete_multipart_upload(
    Bucket=meta.Bucket,
    Key=meta.Key,
    UploadId=meta.UploadId,
    MultipartUpload=parts_dict,
  )

def is_multipart_in_progress(s3_client: S3Client, bucket: str, upload_id: str):
  response = s3_client.list_multipart_uploads(Bucket=bucket)
  if 'Uploads' not in response:
    return False

  upload_ids = {upload['UploadId'] for upload in response['Uploads']}
  return upload_id in upload_ids

def initiate_multipart_upload(s3_client: S3Client, bucket: str, key: str) -> str:
  """ Return an upload id. """
  response = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
  return response['UploadId']
