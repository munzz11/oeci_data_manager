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
        p11_operator_bagfiles = []
        for f in file.project(platform):
          try:
            f_start_time = datetime.datetime.fromtimestamp(f.meta['RosBagHandler']['start_time'])
            f_end_time = datetime.datetime.fromtimestamp(f.meta['RosBagHandler']['end_time'])
            if f_start_time < end_time and f_end_time > start_time:
              print(f.local_path)
              if 'ROBOBOX' in f.local_path.name:
                robobox_bagfiles.append(f)
              elif 'DRIX' in f.local_path.name and f.local_path.parts[-2] == 'mission_logs':
                drix_bagfiles.append(f)
              elif 'project11' in f.local_path.name:
                if 'project11_operator' in f.local_path.name:
                  p11_operator_bagfiles.append(f)
                else:
                  p11_bagfiles.append(f)
          except:
            pass
        if deployments_meta is None:
          deployments_meta = {}
        if not deployment_id in deployments_meta:
          deployments_meta[deployment_id] = {}

        if 'robobox_sources' in deployments_meta[deployment_id]:
          for s in robobox_bagfiles:
            if not str(s) in deployments_meta[deployment_id]['robobox_sources']:
              need_processing = True
              break
        robobox_sources = []
        for r in robobox_bagfiles:
          robobox_sources.append(str(r.local_path))
        deployments_meta[deployment_id]['robobox_sources'] = robobox_sources

        if 'drix_sources' in deployments_meta[deployment_id]:
          for s in drix_bagfiles:
            if not str(s) in deployments_meta[deployment_id]['drix_sources']:
              need_processing = True
              break
        drix_sources = []
        for d in drix_bagfiles:
          drix_sources.append(str(d.local_path))
        deployments_meta[deployment_id]['drix_sources'] = drix_sources

        if 'project11_sources' in deployments_meta[deployment_id]:
          for s in p11_bagfiles:
            if not str(s) in deployments_meta[deployment_id]['project11_sources']:
              need_processing = True
              break
        p11_sources = []
        for d in p11_bagfiles:
          p11_sources.append(str(d.local_path))
        deployments_meta[deployment_id]['project11_sources'] = p11_sources

        if 'project11_operator_sources' in deployments_meta[deployment_id]:
          for s in p11_operator_bagfiles:
            if not str(s) in deployments_meta[deployment_id]['project11_operator_sources']:
              need_processing = True
              break
        p11_operator_sources = []
        for d in p11_operator_bagfiles:
          p11_operator_sources.append(str(d.local_path))
        deployments_meta[deployment_id]['project11_operator_sources'] = p11_operator_sources

        file.update_meta_value(self,'deployments', deployments_meta)

      print('deplyments need processing?',need_processing)
      if need_processing:
        return True
    return False

  def process(self, file: FileInfo) -> FileInfo:
    if self.needsProcessing(file):
      if file.has_meta_value(self, 'deployments'):
        deployments_info = json.load(file.source_path().open())
        deployments_meta = file.get_meta_value(self, 'deployments')

        for d in deployments_info:
          deployment_id = d['name']
          start_time = datetime.datetime.fromisoformat(d['begin']).timestamp()
          end_time = datetime.datetime.fromisoformat(d['end']).timestamp()
          if deployment_id in deployments_meta:
            print(deployment_id)
            deployment = deployments_meta[deployment_id]
            for sources in ('drix_sources', 'project11_sources', 'robobox_sources'):
              if sources in deployment:
                bounds = {}
                tracks = {}
                for ds in deployment[sources]:
                  print('  ds:',ds)
                  if ds in file.project.files:
                    fi = file.project.files[ds]
                    if fi.meta is not None and 'RosBagHandler' in fi.meta:
                      if 'tracks' in fi.meta['RosBagHandler']:
                        for v in fi.meta['RosBagHandler']['tracks']:
                          if not v in tracks:
                            tracks[v] = []
                            bounds[v] = {'min':{},'max':{}}
                          for p in fi.meta['RosBagHandler']['tracks'][v]:
                            if p['timestamp'] >= start_time and p['timestamp'] <= end_time:
                              tracks[v].append(str(p['timestamp'])+','+str(p['latitude'])+','+str(p['longitude'])+str(p['longitude'])+','+str(p['altitude']))
                              if 'latitude' in bounds[v]['min']:
                                bounds[v]['min']['latitude'] = min(p['latitude'],bounds[v]['min']['latitude'])
                                bounds[v]['min']['longitude'] = min(p['longitude'],bounds[v]['min']['longitude'])
                                bounds[v]['min']['altitude'] = min(p['altitude'],bounds[v]['min']['altitude'])
                                bounds[v]['max']['latitude'] = min(p['latitude'],bounds[v]['max']['latitude'])
                                bounds[v]['max']['longitude'] = min(p['longitude'],bounds[v]['max']['longitude'])
                                bounds[v]['max']['altitude'] = min(p['altitude'],bounds[v]['max']['altitude'])
                              else:
                                bounds[v]['min']['latitude'] = p['latitude']
                                bounds[v]['min']['longitude'] = p['longitude']
                                bounds[v]['min']['altitude'] = p['altitude']
                                bounds[v]['max']['latitude'] = p['latitude']
                                bounds[v]['max']['longitude'] = p['longitude']
                                bounds[v]['max']['altitude'] = p['altitude']
                  print(bounds)






    return file
