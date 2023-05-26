import argparse
import sys

import boto3

from s3_multipart_upload.subcommands.abort.abort import abort_multipart_upload
from s3_multipart_upload.subcommands.upload.upload_multipart import upload_multipart

S3_CLIENT = boto3.client('s3')

def check_positive_int(numberStr: str):
  number = int(numberStr)
  if number <= 0:
    raise argparse.ArgumentTypeError(f'{numberStr} must be a positive number greater than 0.')
  return number

def build_args_parser(args: list[str]):
  parser = argparse.ArgumentParser(description='Script to .')
  subparsers = parser.add_subparsers(dest='subcommand', help='Available sub-commands', required=True)

  # Upload sub-command
  upload_cmd_parser = subparsers.add_parser('upload', help='Do multipart upload')
  upload_cmd_parser.add_argument('-b', '--bucket', type=str, required=True, help='S3 bucket')
  upload_cmd_parser.add_argument('-k', '--key', type=str, required=True, help='S3 key in which also includes the file name')
  upload_cmd_parser.add_argument('-d', '--folder-path', type=str, required=True, help='Folder path where the split files are stored')
  upload_cmd_parser.add_argument('-p', '--prefix', type=str, required=True, help='Prefix of the split files')
  upload_cmd_parser.add_argument('-m', '--meta-file-path', type=str, required=True, help='Path to the multipart meta file')
  upload_cmd_parser.add_argument('-a', '--parts-file-path', type=str, required=True, help='Path to the file containging the uploaded parts')
  upload_cmd_parser.add_argument('-n', '--start-part-number', type=check_positive_int, required=False, help='Optionally specify which part number to start')
  upload_cmd_parser.add_argument('-t', '--thread-count', type=check_positive_int, required=False, help='Optionally specify number of threads to use')

  # Abort sub-command
  abort_cmd_parser = subparsers.add_parser('abort', help='Abort multipart upload defined in the multipart meta file')
  abort_cmd_parser.add_argument('-m', '--meta-file-path', type=str, required=True, help='Path to the multipart meta file')

  return parser.parse_args(args)

if __name__ == '__main__':
  args = build_args_parser(sys.argv[1:])
  subcommand_name: str = args.subcommand

  if subcommand_name == 'upload':
    bucket: str = args.bucket
    key: str = args.key
    folder_path: str = args.folder_path
    prefix: str = args.prefix
    meta_file_path: str = args.meta_file_path
    parts_file_path: str = args.parts_file_path
    starting_part_number: int | None = args.start_part_number
    thread_count: int | None = args.thread_count
    upload_multipart(S3_CLIENT, bucket, key, folder_path, prefix, meta_file_path, parts_file_path, starting_part_number, thread_count)
  elif subcommand_name == 'abort':
    meta_file_path: str = args.meta_file_path
    abort_multipart_upload(S3_CLIENT, meta_file_path)
