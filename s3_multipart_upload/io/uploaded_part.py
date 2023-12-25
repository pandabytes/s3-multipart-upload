import types
from dataclasses import dataclass
from typing import Literal

import jsonlines

from dataclass_wizard import JSONWizard

@dataclass(frozen=True)
class UploadedPart(JSONWizard):
  e_tag: str
  part_number: int
  upload_id: str

  class Meta(JSONWizard.Meta):
    key_transform_with_dump='CAMEL'
    key_transform_with_load='SNAKE'

  def __post_init__(self):
    if self.part_number <= 0:
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
    self._writer: jsonlines.Writer | None = None

  def open(self):
    self._writer = jsonlines.open(self._file_path, mode=self._mode, flush=self._flush)

  def close(self):
    self._raise_exception_if_writer_not_opened()
    self._writer.close()

  def write(self, uploaded_part: UploadedPart):
    self._raise_exception_if_writer_not_opened()
    self._writer.write(uploaded_part.to_dict())

  def _raise_exception_if_writer_not_opened(self):
    if self._writer is None:
      raise IOError('Writer is not opened yet.')

class UploadedPartFileReader(BaseUploadedPartFileReaderWriter):
  def __init__(self, file_path: str) -> None:
    self._file_path = file_path
    self._reader: jsonlines.Reader | None = None

  def open(self):
    self._reader = jsonlines.open(self._file_path, mode='r')

  def close(self):
    self._raise_exception_if_reader_not_opened()
    self._reader.close()

  def read(self):
    self._raise_exception_if_reader_not_opened()

    for json_obj in self._reader.iter(skip_empty=True, skip_invalid=False):
      yield UploadedPart.from_dict(json_obj)

  def _raise_exception_if_reader_not_opened(self):
    if self._reader is None:
      raise IOError('Reader is not opened yet.')
