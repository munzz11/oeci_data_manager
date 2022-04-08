#!/usr/bin/env python3

import pathlib
import json
from xmlrpc.client import Boolean

from simplejson import JSONDecodeError

class FileInfo:
  def __init__(self, project, local_path: pathlib.Path = None, meta_path: pathlib.Path = None) -> None:
    self.project = project
    self.meta = None
    self.meta_updated = False
    self.local_path = None
    self.meta_path = None
    self.size = None
    self.modify_time = None
    self.file_exists = None
    self.meta_exists = None
    self.pending_processors = []
    if local_path is not None:
      self.local_path = local_path
      self.meta_path = self.project.meta_path/self.local_path.parent/(self.local_path.name+'.meta.json')
    elif meta_path is not None:
      self.local_path = meta_path.relative_to(project.meta_path).parent/(meta_path.parts[-1][:-10])
      self.meta_path = meta_path

  def load_meta(self) -> Boolean:
    if self.meta_exists is None:
      if self.meta_path is not None:
        if self.meta_path.is_file():
          try:
            self.meta = json.load(open(self.meta_path))
            self.meta_exists = True
            return True
          except json.decoder.JSONDecodeError as e:
            print('error loading meta:', self.meta_path.absolute(),e)
        self.meta = {}
        self.meta_exists = False
        return True
      return False
    return True

  def update_from_source(self, force: Boolean = False) -> Boolean:
    if self.file_exists is None or force:
      if self.local_path is None:
        return False
      path = self.project.find_source_path(self.local_path)
      if path is None:
        self.file_exists = False
        return False
      s = path.stat()
      self.size = s.st_size
      self.modify_time = s.st_mtime
      self.file_exists = True
      return True
    return self.file_exists

  def update_meta_value(self, handler, key, value):
    if self.meta is None:
      if not self.load_meta():
        return False
    handler_label = type(handler).__name__
    if not handler_label in self.meta:
      self.meta[handler_label] = {}
    if key in self.meta[handler_label]:
      if self.meta[handler_label][key] != value:
        self.meta_updated = True
    self.meta[handler_label][key] = value
    return True

  def has_meta_value(self, handler, key) -> Boolean:
    if self.meta is None:
      if not self.load_meta():
        return False
    handler_label = type(handler).__name__
    if not handler_label in self.meta:
      return False
    return key in self.meta[handler_label]

  def get_meta_value(self, handler, key):
    handler_label = type(handler).__name__
    return self.meta[handler_label][key]

  def save_meta(self):
    if self.meta_path is None:
      return False
    if self.file_exists:
      if not self.update_meta_value(self, 'size', self.size):
        return False
      if not self.update_meta_value(self, 'modify_time', self.modify_time):
        return False
    self.meta_path.parent.mkdir(parents=True, exist_ok=True)
    outfile = self.meta_path.open('w')
    json.dump(self.meta, outfile)
    return True

  def is_modified(self) -> Boolean:
    self.update_from_source()
    if self.has_meta_value(self, 'size') and self.has_meta_value(self, 'modify_time'):
      if self.get_meta_value(self, 'size') == self.size and self.get_meta_value(self, 'modify_time') == self.modify_time:
        return False
    return True

  def status(self):
    ret = 'unknown'
    if self.meta_exists is not None:
      if self.meta_exists:
        if self.file_exists is not None:
          if self.is_modified():
            ret = 'modified'
          else:
            ret = 'up-to-date'
          if self.needs_processing():
            ret = 'needs processing'
    if self.file_exists is not None and not self.file_exists:
      ret = 'missing'
    return ret

  def source_path(self) -> pathlib.Path:
    return self.project.find_source_path(self.local_path)

  def add_processor(self, processor):
    processor_label = type(processor).__name__
    if not processor_label in self.pending_processors:
      self.pending_processors.append(processor_label)

  def remove_processor(self, processor):
    processor_label = type(processor).__name__
    if processor_label in self.pending_processors:
      self.pending_processors.remove(processor_label)

  def needs_processing_by(self, processor):
    processor_label = type(processor).__name__
    return processor_label in self.pending_processors

  def needs_processing(self):
    return len(self.pending_processors) > 0
