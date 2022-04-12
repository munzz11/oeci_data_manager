#!/usr/bin/env python3

import rosbag
import shutil
import pathlib

from file_info import FileInfo

class RosBagIndexHandler:
  def __init__(self):
    pass

  def needsProcessing(self, file: FileInfo):
    if file.local_path.suffix == '.bag' or file.local_path.parts[-1].endswith('.bag.active'):
      if file.has_meta_value(self, 'indexed'):
        indexed = file.get_meta_value(self, 'indexed')
        if indexed:
          return False
        if file.has_meta_value(self, 'indexed_file'):
          indexed_path = pathlib.Path(file.get_meta_value(self, 'indexed_file'))
          if indexed_path in file.project.files:
            indexed_file = file.project.files[indexed_path]
            if indexed_file.has_meta_value(self, 'indexed'):
              return not indexed_file.get_meta_value(self, 'indexed')
      return True
    return False

  def process(self, file: FileInfo):
    if self.needsProcessing(file):
      try:
        rosbag.Bag(file.source_path() , 'r')
        file.update_meta_value(self,'indexed',True)
      except rosbag.ROSBagUnindexedException:
        file.update_meta_value(self,'indexed',False)
      except Exception as e:
        print("error opening bag to check if indexed", file.local_path)
        print(type(e))
        print(e)
        return file

      if not file.get_meta_value(self,'indexed'):
        try:
          outfilename = file.local_path
          if file.local_path.suffix == '.active':
            outfilename = file.local_path.parent/file.local_path.stem
          else:
            outfilename = file.local_path.parent/(file.local_path.stem+'.indexed.bag')
          local_outfn = outfilename
          outfilename = file.project.output/outfilename
          if outfilename.is_file():
            try:
              rosbag.Bag(outfilename, 'r')
              file.update_meta_value(self,'indexed_file',str(local_outfn))
              return file
            except rosbag.ROSBagUnindexedException:
              pass
          outfilename.parent.mkdir(parents=True, exist_ok=True)
          shutil.copy(str(file.source_path()), str(outfilename))
          bag = rosbag.Bag(outfilename, 'a', allow_unindexed=True)
          try:
            for offset in bag.reindex():
              pass
          except:
            pass
          bag.close()
          file.update_meta_value(self,'indexed_file',str(local_outfn))
        except Exception as e:
          print("error trying to index", file.local_path)
          print(type(e))
          print(e)
    return file




