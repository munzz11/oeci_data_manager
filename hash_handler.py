#!/usr/bin/env python3

import hashlib

from file_info import FileInfo

class HashHandler:
  def __init__(self):
    self.hasher = hashlib.sha256
    self.label = 'sha256'

  def needsProcessing(self, file: FileInfo):
    if file.has_meta_value(self, 'hash') and not file.is_modified():
      return False
    return True

  def process(self, file: FileInfo) -> FileInfo:
    if self.needsProcessing(file):
      hash = self.hasher()
      sp = file.source_path()
      if sp is not None:
        with open(file.source_path(), 'rb') as f:
          for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)

      file.update_meta_value(self, 'hash', hash.hexdigest())
    return file

