import argparse
import sys

import boto3

from s3_multipart_upload.subcommands.abort.abort import abort_multipart_upload
from s3_multipart_upload.subcommands.upload.upload_multipart import upload_multipart

S3_CLIENT = boto3.client('s3')
PART_MIN_SIZE = int(5e6)

def check_positive_int(number_str: str):
  number = int(number_str)
  if number <= 0:
    raise argparse.ArgumentTypeError('Must be a positive number greater than 0.')
  return number

def check_split_size(number_str: str):
  number = int(number_str)
  if number < PART_MIN_SIZE:
    raise argparse.ArgumentTypeError(f'Must be at least {PART_MIN_SIZE}.')
  return number

def build_args_parser(args: list[str]):
  parser = argparse.ArgumentParser(description='Script to .')
  subparsers = parser.add_subparsers(dest='subcommand', help='Available sub-commands', required=True)

  # Upload sub-command
  upload_cmd_parser = subparsers.add_parser('upload', help='Do multipart upload')
  upload_cmd_parser.add_argument('-b', '--bucket', type=str, required=True, help='S3 bucket')
  upload_cmd_parser.add_argument('-k', '--key', type=str, required=True, help='S3 key in which also includes the file name')
  upload_cmd_parser.add_argument('-f', '--file-path', type=str, required=True, help='Path to where the file is')
  upload_cmd_parser.add_argument('-m', '--meta-file-path', type=str, required=True, help='Path to the multipart meta file')

  upload_cmd_parser.add_argument('-a', '--parts-file-path', type=str, required=True,
                                 help='Path to the file containging the uploaded parts')

  upload_cmd_parser.add_argument('-t', '--thread-count', type=check_positive_int, required=False, default=1,
                                 help='Optionally specify number of threads to use')

  upload_cmd_parser.add_argument('-s', '--split-size', type=check_split_size, required=False, default=PART_MIN_SIZE,
                                 help=f'Optionally specify split size (in bytes). Default is {PART_MIN_SIZE} bytes.')

  # Abort sub-command
  abort_cmd_parser = subparsers.add_parser('abort', help='Abort multipart upload defined in the multipart meta file')
  abort_cmd_parser.add_argument('-m', '--meta-file-path', type=str, required=True, help='Path to the multipart meta file')

  return parser.parse_args(args)

if __name__ == '__main__':
  args = build_args_parser(sys.argv[1:])
  subcommand_name: str = args.subcommand

  success = False
  if subcommand_name == 'upload':
    bucket: str = args.bucket
    key: str = args.key
    file_path: str = args.file_path
    meta_file_path: str = args.meta_file_path
    parts_file_path: str = args.parts_file_path
    thread_count: int = args.thread_count
    split_size: int = args.split_size

    success = upload_multipart(
      s3_client=S3_CLIENT,
      bucket=bucket,
      key=key,
      file_path=file_path,
      meta_file_path=meta_file_path,
      parts_file_path=parts_file_path,
      thread_count=thread_count,
      split_size=split_size,
    )

  elif subcommand_name == 'abort':
    meta_file_path: str = args.meta_file_path
    success = abort_multipart_upload(S3_CLIENT, meta_file_path)

  exit(0 if success else 1)
