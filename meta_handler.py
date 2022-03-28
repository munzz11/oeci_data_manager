#!/usr/bin/env python3

import json

from project import Project

class MetaReader:
  def __init__(self, project: Project):
    self.project = project

  def needsProcessing(self, filename, meta):
    if filename in self.project.ignore_list:
      return False
    return filename.is_file()

  def process(self, filename, meta):
    if meta is None:
      meta = {}
    file_size = filename.stat().st_size
    mod_time = filename.stat().st_mtime
    needs_update = True
    meta_file = self.project.meta_path/filename.relative_to(self.project.source).parent/(filename.name+'.json')
    if not 'saved' in meta:
      meta['saved'] = {}
    if meta_file.is_file():
      try:
        m = json.load(open(meta_file))
        needs_update = not ('size' in m and m['size'] == file_size and 'modify_time' in m and m['modify_time'] == mod_time)
        meta['saved'] = m
      except json.decoder.JSONDecodeError:
        print('error loading meta:', meta_file.absolute(),'\n  ', meta_file.open().read())
    meta['size'] = file_size
    meta['modify_time'] = mod_time
    meta['needs_update'] = needs_update
    meta['meta_file'] = meta_file
    return meta

class MetaSaver:
  def __init__(self, project: Project):
    pass

  def needsProcessing(self, filename, meta):
    return meta['needs_update']

  def process(self, filename, meta):
    meta['saved']['size'] = meta['size']
    meta['saved']['modify_time'] = meta['modify_time']
    meta['meta_file'].parent.mkdir(parents=True, exist_ok=True)
    out_file = open(meta['meta_file'],"w")
    json.dump(meta['saved'], out_file)
    out_file.close()
    return meta

