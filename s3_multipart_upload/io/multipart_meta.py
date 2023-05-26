import json
import os
from dataclasses import dataclass

from s3_multipart_upload.base_frozen_dataclass import BaseFrozenDataClass


@dataclass(frozen=True)
class MultipartUploadMeta(BaseFrozenDataClass):
  Bucket: str
  Key: str
  UploadId: str

def load_multipart_meta_file(file_path: str):
  # File not found or file is empty
  if not os.path.exists(file_path) or os.stat(file_path).st_size == 0 :
    return None

  with open(file_path, 'r') as multipart_file:
    json_obj = json.load(multipart_file)
    return MultipartUploadMeta(**json_obj)
  
def save_multipart_meta_file(file_path: str, multipart_meta: MultipartUploadMeta):
  with open(file_path, 'w') as multipart_file:
    json_str = multipart_meta.to_json_str()
    multipart_file.write(json_str)
