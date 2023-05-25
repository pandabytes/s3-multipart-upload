import base64
import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class UploadFile:
  FilePath: str
  PartNumber: int

  @property
  def MD5(self) -> str:
    """ Dynamically compute the MD5 hash of `FilePath`. This can be
        expensive if called multiple times if the file is large.
    """
    with open(self.FilePath, 'rb') as f:
      h = hashlib.md5()
      for chunk in f:
        h.update(chunk)
      return base64.b64encode(h.digest()).decode()

  def __post_init__(self):
    if self.PartNumber <= 0:
      raise ValueError('PartNumber must be at least 1.')
