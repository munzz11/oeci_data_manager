#!/usr/bin/env python3

import json
import rosbag
import rospy
import datetime
import pathlib
import subprocess

from file_info import FileInfo

class DrixDeploymentsHandler:
  def __init__(self):
    pass


  def needsProcessing(self, file: FileInfo):
    if file.local_path.name == 'deployments.json' and len(file.local_path.parts) == 3 and file.local_path.parts[1] == '01-catalog':
      platform = pathlib.Path(file.local_path.parts[0])
      deployments_meta = None
      if file.has_meta_value(self, 'deployments'):
        deployments_meta = file.get_meta_value(self, 'deployments')

      deployments_info = json.load(file.source_path().open())
      print(deployments_info)
      need_processing = False
      for d in deployments_info:
        deployment_id = d['name']
        start_time = datetime.datetime.fromisoformat(d['begin'])
        end_time = datetime.datetime.fromisoformat(d['end'])
        drix_bagfiles = []
        robobox_bagfiles = []
        p11_bagfiles = []
        for f in file.project(platform):
          try:
            f_start_time = datetime.datetime.fromtimestamp(f.meta['RosBagHandler']['start_time'])
            f_end_time = datetime.datetime.fromtimestamp(f.meta['RosBagHandler']['end_time'])
            if f_start_time < end_time and f_end_time > start_time:
              if 'ROBOBOX' in f.local_path.name:
                robobox_bagfiles.append(f)
              elif 'DRIX' in f.local_path.name and f.local_path.parts[-2] == 'mission_logs':
                drix_bagfiles.append(f)
              elif 'project11' in f.local_path.name:
                p11_bagfiles.append(f)
          except:
            pass
        if deployments_meta is None:
          deployments_meta = {}
        if not deployment_id in deployments_meta:
          deployments_meta[deployment_id] = {}
        robobox_outpath = platform/'03-processing/drix'/(deployment_id+'_ROBOBOX.bag')
        deployments_meta[deployment_id]['robobox_outpath'] = str(robobox_outpath)
        robobox_outfile = file.project.get_fileinfo(robobox_outpath)
        if robobox_outfile is None:
          need_processing = True
        else:
          robobox_outfile.update_from_source()
          if len(robobox_bagfiles):
            if not robobox_outfile.file_exists:
              need_processing = True
            else:
              if 'robobox_sources' in deployments_meta[deployment_id]:
                for s in robobox_bagfiles:
                  if not str(s) in deployments_meta[deployment_id]['robobox_sources']:
                    need_processing = True
                    break
                  if file.project.get_fileinfo(s).is_newer_than(robobox_outfile):
                    need_processing = True
                    break
        robobox_sources = []
        for r in robobox_bagfiles:
          robobox_sources.append(str(r.local_path))
        deployments_meta[deployment_id]['robobox_sources'] = robobox_sources

        drix_outpath = platform/'03-processing/drix'/(deployment_id+'_DRIX.bag')
        deployments_meta[deployment_id]['drix_outpath'] = str(drix_outpath)
        drix_outfile = file.project.get_fileinfo(drix_outpath)
        if drix_outfile is None:
          need_processing = True
        else:
          drix_outfile.update_from_source()
          if len(drix_bagfiles):
            if not drix_outfile.file_exists:
              need_processing = True
            else:
              if 'drix_sources' in deployments_meta[deployment_id]:
                for s in drix_bagfiles:
                  if not str(s) in deployments_meta[deployment_id]['drix_sources']:
                    need_processing = True
                    break
                  if file.project.get_fileinfo(s).is_newer_than(drix_outfile):
                    need_processing = True
                    break
        drix_sources = []
        for d in drix_bagfiles:
          drix_sources.append(str(d.local_path))
        deployments_meta[deployment_id]['drix_sources'] = drix_sources

        p11_outpath = platform/'03-processing/drix'/(deployment_id+'_project11.bag')
        deployments_meta[deployment_id]['project11_outpath'] = str(p11_outpath)
        p11_outfile = file.project.get_fileinfo(p11_outpath)
        if p11_outfile is None:
          need_processing = True
        else:
          p11_outfile.update_from_source()
          if len(p11_bagfiles):
            if not p11_outfile.file_exists:
              need_processing = True
            else:
              if 'project11_sources' in deployments_meta[deployment_id]:
                for s in p11_bagfiles:
                  if not str(s) in deployments_meta[deployment_id]['project11_sources']:
                    need_processing = True
                    break
                  if file.project.get_fileinfo(s).is_newer_than(p11_outfile):
                    need_processing = True
                    break
        p11_sources = []
        for d in p11_bagfiles:
          p11_sources.append(str(d.local_path))
        deployments_meta[deployment_id]['project11_sources'] = p11_sources

        file.update_meta_value(self,'deployments', deployments_meta)

      if need_processing:
        return True
    return False

  def merge_bags(self, file: FileInfo, outpath, start_time, end_time, sources):
    output = file.project.output/outpath
    output.parent.mkdir(parents=True, exist_ok=True)
    command = ['merge_bags','-p']
    command.append('-o')
    command.append(str(output))
    command.append('-s')
    command.append(str(start_time.timestamp()))
    command.append('-e')
    command.append(str(end_time.timestamp()))
    for s in sources:
      command.append(file.project.find_source_path(pathlib.Path(s)))
    subprocess.run(command)

  def process(self, file: FileInfo) -> FileInfo:
    if self.needsProcessing(file):
      if file.has_meta_value(self, 'deployments'):
        deployments_info = json.load(file.source_path().open())
        deployments_meta = file.get_meta_value(self, 'deployments')
        for d in deployments_info:
          deployment_id = d['name']
          start_time = datetime.datetime.fromisoformat(d['begin'])
          end_time = datetime.datetime.fromisoformat(d['end'])
          if deployment_id in deployments_meta:
            deployment = deployments_meta[deployment_id]
            if 'robobox_outpath' in deployment and 'robobox_sources' in deployment and len(deployment['robobox_sources']):
              self.merge_bags(file, deployment['robobox_outpath'], start_time, end_time,deployment['robobox_sources'])
            if 'drix_outpath' in deployment and 'drix_sources' in deployment and len(deployment['drix_sources']):
              self.merge_bags(file, deployment['drix_outpath'], start_time, end_time,deployment['drix_sources'])
            if 'project11_outpath' in deployment and 'project11_sources' in deployment and len(deployment['project11_sources']):
              self.merge_bags(file, deployment['project11_outpath'], start_time, end_time,deployment['project11_sources'])

    return file
