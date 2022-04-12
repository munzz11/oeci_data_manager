#!/usr/bin/env python3

import pathlib
import json
import time
import datetime

from multiprocessing import Pool

from odm_utils import resolvePath
from file_info import FileInfo

from typing import Dict, Iterator

def previewFile(file: FileInfo, handler_list):
  pipeline = []
  for h in handler_list:
    pipeline.append(h())
    
  for processor in pipeline:
    if file.source_path() is not None:
      if processor.needsProcessing(file):
        file.add_processor(processor)
  return file

def processFile(file: FileInfo, handler_list):
  pipeline = []
  for h in handler_list:
    pipeline.append(h())

  for processor in pipeline:
    if file.needs_processing_by(processor):
      processor.process(file)
      file.remove_processor(processor)
  file.save_meta()
  return file

class Project:
  def __init__(self, config_path: pathlib.Path):
    self.config_path = config_path
    self.files = {}
    self.ignore_list = []
    self.label = config_path.parts[-1]
    self.config_file = config_path/'config.json'
    self.meta_path = config_path/'meta'
    if self.config_file.exists():
      try:
        self.config = json.load(self.config_file.open())
        self.source = pathlib.Path(self.config['source'])
        self.output = pathlib.Path(self.config['output'])
        self.manifest_file = self.source/'manifest.txt'
        self.ignore_list.append(self.manifest_file)
      except:
        raise Exception('Error reading config file:'+str(self.config_file))
    else:
      self.config = None
      self.source = None
      self.output = None

    self.progress_interval = datetime.timedelta(seconds=0.5)

  def valid(self):
    return self.config is not None

  def create(self, source: pathlib.Path, output: pathlib.Path = None):
    if self.valid():
      raise Exception("Can't create project, config alredy exists: "+str(self.config_file))
    try:
      self.source = resolvePath(source)
    except RuntimeError:
      raise Exception("Can't resolve source path "+str(source))
    if not self.source.is_dir():
      raise Exception("Source is not a directory: "+str(self.source))
    if output is None:
      self.output = self.source
    else:
      try:
        self.output = resolvePath(output)
      except RuntimeError:
        raise Exception("Can't resolve output path "+str(output))
    self.config = {'source': str(self.source), 'output': str(self.output)}
    self.manifest_file = self.source/'manifest.txt'
    self.ignore_list.append(self.manifest_file)
    if not self.config_path.is_dir():
      self.config_path.mkdir(parents=True)
    json.dump(self.config, self.config_file.open("w"))

  def load(self):
    for f in self.meta_path.glob("**/*.meta.json"):
      local_path = f.relative_to(self.meta_path)
      if local_path in self.files:
        self.files[local_path].load_meta()
      else:
        fi = FileInfo(self, meta_path=f)
        if fi.load_meta():
          self.files[fi.local_path] = fi

  def __call__(self, path: pathlib.Path = None) -> Iterator[FileInfo]:
    for f in self.files:
      if path is None or path in pathlib.Path(f).parents:
        yield self.files[f]

  def get_fileinfo(self, local_path: pathlib.Path) -> FileInfo:
    if local_path in self.files:
      return self.files[local_path]

  def source_files(self) -> Iterator[pathlib.Path]:
    for f in self.source.glob("**/*"):
      if f.is_file():
        yield f
    if self.source != self.output:
      for f in self.output.glob("**/*"):
        if f.is_file():
          yield f

  def structure(self) -> Dict:
    ret = {}
    expedition = self.source.parts[-1]
    ret[expedition] = {}
    description_path = self.source/'ExpeditionDescription.json'
    ret[expedition]['description']=description_path.relative_to(self.source)
    ret[expedition]['description_present']=description_path.is_file()
    

    platforms = {}
    for p in self.source.glob('*'):
      if p.is_dir():
        platform_label = p.parts[-1]
        data_stages = {}
        for s in p.glob('*'):
          if s.is_dir():
            stage_label = s.parts[-1]
            sensors = []
            for sensor in s.glob('*'):
              if sensor.is_dir():
                sensors.append(sensor.parts[-1])
            data_stages[stage_label] = sensors
        platforms[platform_label] = data_stages

    ret[expedition]['platforms'] = platforms

    return ret

  def scan_source(self, progress_callback = None):
    if progress_callback is not None:
      count = 0
      last_report_time = datetime.datetime.now()
    for potential_file in self.source_files():
      if self.source in potential_file.parents:
        local_path = potential_file.relative_to(self.source)
      else:
        local_path = potential_file.relative_to(self.output)
      if not local_path in self.files:
        fi = FileInfo(self, local_path=local_path)
        fi.load_meta()
        self.files[local_path] = fi
      self.files[local_path].update_from_source(True)
      if progress_callback is not None:
        count += 1
        now = datetime.datetime.now()
        if now - last_report_time > self.progress_interval:
          progress_callback(count)
          last_report_time = now

  def scan(self, handlers, process_count=1, progress_callback = None):
    scanned_count = 0

    if progress_callback is not None:
      last_report_time = datetime.datetime.now()

    if process_count > 1:
      pool = Pool(processes=process_count)
      results_list = []

    for f in self.files:
      if process_count > 1:
        while len(results_list) >= process_count*2:
          done_list = []
          for r in results_list:
            if r.ready():
              done_list.append(r)
          if len(done_list):
            for d in done_list:
              d.get()
              scanned_count += 1
              results_list.remove(d)
          else:
            time.sleep(.05)
        results_list.append(pool.apply_async(previewFile,(self.files[f], handlers)))
      else:
        f = previewFile(self.files[f], handlers)
        scanned_count += 1
      if progress_callback is not None:
        now = datetime.datetime.now()
        if now - last_report_time > self.progress_interval:
          if progress_callback(scanned_count):
            return
          last_report_time = now
    if process_count > 1:
      for r in results_list:
        r.wait()
        f = r.get()
        scanned_count += 1

  def process(self, handlers, process_count=1, progress_callback = None):
    processed_count = 0
    processed_size = 0

    if process_count > 1:
      pool = Pool(processes=process_count)
      results_list = []

    for f in self.files:
      file = self.files[f]
      if file.needs_processing():
        if process_count > 1:
          while len(results_list) >= process_count*2:
            done_list = []
            for r in results_list:
              if r.ready():
                done_list.append(r)
            if len(done_list):
              for d in done_list:
                f = d.get()
                processed_count += 1
                processed_size += f.size
                results_list.remove(d)
            else:
              time.sleep(.05)
              if progress_callback is not None:
                if progress_callback(processed_size):
                  return
          results_list.append(pool.apply_async(processFile,(file, handlers)))
        else:
          f = processFile(file, handlers)
          processed_count += 1
          processed_size += f.size
        if progress_callback is not None:
          if progress_callback(processed_size):
            return

        
    if process_count > 1:
      for r in results_list:
        r.wait()
        f = r.get()
        processed_count += 1
        processed_size += f.size


  def find_processing_path_from_raw(self, path: pathlib.Path) -> pathlib.Path:
    ret = pathlib.Path(path.parts[0])
    for p in path.parts[1:]:
      if p == '02-raw':
        ret = ret/'03-processing'
      else:
        ret = ret/p
    return ret

  def find_output_path(self, path: pathlib.Path) -> pathlib.Path:
    return self.output/path.relative_to(self.source)

  def find_source_path(self, local_path: pathlib.Path) -> pathlib.Path:
    ret = self.source/local_path
    if ret.is_file():
      return ret
    if self.output != self.source:
      ret = self.output/local_path
      if ret.is_file():
        return ret
    return None

  def generate_file_stats(self, path: pathlib.Path = None):
    ret = {
      'total':{'count': 0, 'size':0},
      'needs_processing':{'count': 0, 'size':0},
      'new':{'count': 0, 'size':0},
      'updated':{'count': 0, 'size':0},
      'missing':{'count': 0}
    }

    for file in self(path):
      if not file.update_from_source():
        ret['missing']['count'] += 1
      else:
        ret['total']['count'] += 1
        ret['total']['size'] += file.size
        if file.load_meta():
          if not file.meta_exists:
            ret['new']['count'] += 1
            ret['new']['size'] += file.size
          else:
            if file.is_modified():
              ret['updated']['count'] += 1
              ret['updated']['size'] += file.size
        if file.needs_processing():
          ret['needs_processing']['count'] += 1
          ret['needs_processing']['size'] += file.size
    return ret


