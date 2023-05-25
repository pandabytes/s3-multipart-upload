from mypy_boto3_s3 import S3Client


def upload_part(s3_client: S3Client, file_path: str, part_number: int, md5: str, bucket: str, key: str, upload_id: str) -> str:
  """ Upload the file and returns the ETag string from the AWS response. """
  with open(file_path, 'rb') as part_file:
    upload_response = s3_client.upload_part(
      Bucket=bucket, 
      Key=key,
      PartNumber=part_number,
      Body=part_file,
      UploadId=upload_id,
      ContentMD5=md5,
    )

  return upload_response['ETag'].replace('"', '')
