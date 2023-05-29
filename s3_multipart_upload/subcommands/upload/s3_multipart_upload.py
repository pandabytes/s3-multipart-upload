from mypy_boto3_s3 import S3Client
from s3_multipart_upload.io.multipart_meta import MultipartUploadMeta
from s3_multipart_upload.io.uploaded_part import UploadedPart


def complete_multipart_upload(s3_client: S3Client, meta: MultipartUploadMeta, parts: list[UploadedPart]):
  if not parts:
    raise ValueError('parts cannot be empty.')

  sorted_parts = sorted(parts, key=lambda p: p.PartNumber)
  if _have_missing_parts(sorted_parts):
    raise ValueError('parts list has missing parts.')

  parts_dict = {'Parts': [{'ETag': part.ETag, 'PartNumber': part.PartNumber} for part in sorted_parts]}
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

def _have_missing_parts(parts: list[UploadedPart]):
  """ Assume `parts` is already sorted. """
  first_part, last_part = parts[0], parts[-1]
  return len(parts) != (last_part.PartNumber - first_part.PartNumber + 1)