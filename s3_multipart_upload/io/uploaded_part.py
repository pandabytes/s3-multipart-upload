from dataclasses import dataclass
import types
from typing import Literal

import jsonlines

from s3_multipart_upload.base_frozen_dataclass import BaseFrozenDataClass


@dataclass(frozen=True)
class UploadedPart(BaseFrozenDataClass):
  ETag: str
  PartNumber: int
  UploadId: str

  def __post_init__(self):
    if self.PartNumber <= 0:
      raise ValueError('PartNumber must be at least 1.')

class BaseUploadedPartFileReaderWriter:
  """ Base class for uploaded part reader and writer. """
  def __enter__(self):
    self.open()
    return self

  def __exit__(
    self,
    _exc_type: type[BaseException] | None,
    _exc_val: BaseException | None,
    _exc_tb: types.TracebackType | None
  ):
    self.close()

  def open(self):
    raise NotImplemented()

  def close(self):
    raise NotImplemented()

class UploadedPartFileWriter(BaseUploadedPartFileReaderWriter):
  def __init__(self, file_path: str, mode: Literal['w', 'a'], flush: bool = False) -> None:
    if mode not in {'w', 'a'}:
      raise ValueError('mode must be "w" or "a".')

    self._file_path = file_path
    self._mode: Literal['w', 'a'] = mode
    self._flush = flush
    self._writer = None

  def open(self):
    self._writer = jsonlines.open(self._file_path, mode=self._mode, flush=self._flush)

  def close(self):
    self._writer.close()

  def write(self, uploaded_part: UploadedPart):
    if self._writer is None:
      raise IOError('Writer is not opened yet.')

    self._writer.write(uploaded_part.to_dict())

class UploadedPartFileReader(BaseUploadedPartFileReaderWriter):
  def __init__(self, file_path: str) -> None:
    self._file_path = file_path
    self._reader = None

  def open(self):
    self._reader = jsonlines.open(self._file_path, mode='r')

  def close(self):
    self._reader.close()

  def read(self):
    if self._reader is None:
      raise IOError('Reader is not opened yet.')

    for json_obj in self._reader.iter(skip_empty=True, skip_invalid=False):
      yield UploadedPart(**json_obj)
