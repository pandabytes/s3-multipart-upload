import os
from dataclasses import dataclass

from dataclass_wizard import JSONWizard

@dataclass(frozen=True)
class MultipartUploadMeta(JSONWizard):
  bucket: str
  key: str
  upload_id: str
  split_size: int

  class Meta(JSONWizard.Meta):
    key_transform_with_dump='CAMEL'
    key_transform_with_load='SNAKE'

def load_multipart_meta_file(file_path: str) -> MultipartUploadMeta:
  # File not found or file is empty
  if not os.path.exists(file_path) or os.stat(file_path).st_size == 0 :
    return None

  with open(file_path, 'r') as multipart_file:
    json_str = multipart_file.read()
    return MultipartUploadMeta.from_json(json_str)
  
def save_multipart_meta_file(file_path: str, multipart_meta: MultipartUploadMeta):
  with open(file_path, 'w') as multipart_file:
    json_str = multipart_meta.to_json(indent=2)
    multipart_file.write(json_str)
