#!/usr/bin/env python3

import pathlib
from typing import List

from odm_utils import resolvePath

from project import Project


class ConfigPath:
  def __init__(self, path: pathlib.Path):
    self.path = resolvePath(path)
    if self.path.exists() and not self.path.is_dir():
      raise Exception("Config path is not a directory: "+str(self.path))

  def exists(self):
    return self.path.is_dir()

  def get_projects(self) -> List[Project]:
    ret = []
    for p in self.path.glob('*'):
      proj = Project(p)
      if proj.valid():
        ret.append(proj)
    return ret


  def create_project(self, label: str, source: pathlib.Path, output: pathlib.Path) -> Project:
    p = Project(self.path/label)
    p.create(source, output)
    return p

  def get_project(self, label: str) -> Project:
    return Project(self.path/label)
