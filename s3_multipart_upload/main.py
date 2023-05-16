import argparse
import sys

import boto3

from s3_multipart_upload.subcommands.abort import abort_multipart_upload
from s3_multipart_upload.subcommands.upload import upload_multipart

s3_client = boto3.client('s3')

def build_args_parser(args: list[str]):
  parser = argparse.ArgumentParser(description='Script to .')
  subparsers = parser.add_subparsers(dest='subcommand', help='Available sub-commands', required=True)

  # Upload sub-command
  upload_cmd_parser = subparsers.add_parser('upload', help='Do multipart upload')
  upload_cmd_parser.add_argument('-b', '--bucket', type=str, required=True, help='S3 bucket')
  upload_cmd_parser.add_argument('-k', '--key', type=str, required=True, help='S3 key in which also includes the file name')
  upload_cmd_parser.add_argument('-d', '--folder-path', type=str, required=True, help='Folder path where the split files are stored')
  upload_cmd_parser.add_argument('-p', '--prefix', type=str, required=True, help='Prefix of the split files')
  upload_cmd_parser.add_argument('-c', '--config-path', type=str, required=True, help='Path to the config file')
  upload_cmd_parser.add_argument('-n', '--start-part-number', type=int, required=False, help='Optionally specify which part number to start')

  # Abort sub-command
  abort_cmd_parser = subparsers.add_parser('abort', help='Abort multipart upload')
  abort_cmd_parser.add_argument('-b', '--bucket', type=str, required=True, help='S3 bucket')
  abort_cmd_parser.add_argument('-k', '--key', type=str, required=True, help='S3 key in which also includes the file name')
  abort_cmd_parser.add_argument('-u', '--upload-id', type=str, required=True, help='Upload id')

  return parser.parse_args(args)

if __name__ == '__main__':
  args = build_args_parser(sys.argv[1:])
  subcommand_name: str = args.subcommand

  if subcommand_name == 'upload':
    bucket: str = args.bucket
    key: str = args.key
    folder_path: str = args.folder_path
    prefix: str = args.prefix
    config_path: str = args.config_path
    starting_part_number: int | None = args.start_part_number
    upload_multipart(s3_client, bucket, key, folder_path, prefix, config_path, starting_part_number)
  elif subcommand_name == 'abort':
    bucket: str = args.bucket
    key: str = args.key
    upload_id: str = args.upload_id
    abort_multipart_upload(s3_client, bucket, key, upload_id)
