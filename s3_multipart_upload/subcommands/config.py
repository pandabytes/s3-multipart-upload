import json
import os
from dataclasses import asdict, dataclass

@dataclass(frozen=True)
class UploadedPart:
  ETag: str
  PartNumber: int

  def __post_init__(self):
    if self.PartNumber <= 0:
      raise ValueError('PartNumber must be at least 1.')

@dataclass
class MultipartUploadConfig:
  Bucket: str
  Key: str
  UploadId: str
  Parts: list[UploadedPart]

  def __post_init__(self):
    if (isinstance(self.Parts, list) and
        self.Parts and
        isinstance(self.Parts[0], dict)):
      self.Parts = [UploadedPart(**part) for part in self.Parts]

  def to_dict(self) -> dict:
    return asdict(self)
  
  def to_json(self) -> str:
    return json.dumps(self.to_dict(), indent=2)

def load_multipart_file(file_path: str):
  # File not found or file is empty
  if not os.path.exists(file_path) or os.stat(file_path).st_size == 0 :
    return None

  with open(file_path, 'r') as multipart_file:
    json_obj = json.load(multipart_file)
    return MultipartUploadConfig(**json_obj)
  
def save_multipart_file(file_path: str, config: MultipartUploadConfig):
  with open(file_path, 'w') as multipart_file:
    multipart_file.write(config.to_json())
