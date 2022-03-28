#!/usr/bin/env python3

import hashlib

from project import Project

class HashHandler:
  def __init__(self, project: Project):
    self.hasher = hashlib.sha256
    self.label = 'sha256'

  def needsProcessing(self, filename, meta):
    if meta is None:
      return False
    if 'needs_update' in meta and meta['needs_update']:
      return True
    if not 'saved' in meta or not 'hash' in meta['saved']:
      return True
    return False

  def process(self, filename, meta):
    hash = self.hasher()
    with open(filename, 'rb') as f:
      for chunk in iter(lambda: f.read(4096), b""):
        hash.update(chunk)

    if not 'saved' in meta:
      meta['saved'] = {}
    needs_update = True
    if 'hash' in meta['saved'] and meta['saved']['hash'] == hash.hexdigest():
      needs_update = False
    meta['saved']['hash'] = hash.hexdigest()
    if needs_update:
      meta['needs_update'] = True
    return meta

