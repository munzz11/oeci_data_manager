#!/usr/bin/env python3

import pathlib
import json

from data_manager_utils import resolvePath

def previewFile(filename: pathlib.Path, project, handler_list):
  meta=None

  loader = handler_list[0](project)
  if loader.needsProcessing(filename, meta):
    meta = loader.process(filename, meta)

  if meta is not None:
    if len(handler_list) > 1:
      pipeline = []
      for h in handler_list[1:]:
        pipeline.append(h(project))
    
      for processor in pipeline:
        if processor.needsProcessing(filename, meta):
          meta['needs_update'] = True
  
  return filename, meta


class Project:
  def __init__(self, config_path: pathlib.Path, source: pathlib.Path = None, output: pathlib.Path = None):
    self.config_path = config_path
    self.ignore_list = []
    self.label = config_path.parts[-1]
    self.config_file = config_path/'config.json'
    self.meta_path = config_path/'meta'
    if self.config_file.exists():
      # if we pass in a source, we want to create a new project, otherwise open an existing one
      if source is not None:
        raise Exception("Can't create project, config alredy exists: "+str(self.config_file))
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
      if source is not None:
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

  def valid(self):
    return self.config is not None

  def project_files(self):
    for f in self.source.glob("**/*"):
      if f.is_file():
        yield f

  def structure(self):
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

  def scan(self, handlers):
    self.files = {}
    self.total_size = 0
    self.need_processing_size = 0
    self.need_processing_files = []
    for potential_file in self.project_files():
      f,m = previewFile(potential_file, self, handlers)
      self.files[f] = m
      if m is not None:
        self.total_size += m['size']
        if 'needs_update' in m and m['needs_update']:
          self.need_processing_size += m['size']
          self.need_processing_files.append(f)
