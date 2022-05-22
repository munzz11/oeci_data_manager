#!/usr/bin/env python3

import json
import odm_utils
import rospy
import datetime
import pathlib
import subprocess

from file_info import FileInfo
from project import Project

class DrixDeployments:
  def __init__(self, project: Project):
    self.project = project

  def generate(self):
    for platform in self.project.platforms():
      deployments_path = self.project.find_source_path(pathlib.Path(platform)/'01-catalog/deployments.json')
      deployments_info = json.load(open(deployments_path))
      for d in deployments_info:
        print (d)
        deployment_id = d['name']
        output_path = deployments_path.parent/deployment_id
        start_time = datetime.datetime.fromisoformat(d['begin']+'+00:00').timestamp()
        end_time = datetime.datetime.fromisoformat(d['end']+'+00:00').timestamp()
        print(start_time,'to',end_time)
        bagfiles = {'drix':[],'robobox':[],'p11':[],'p11_operator':[]}
        for f in self.project( pathlib.Path(platform)):
          # if f.local_path.suffix == '.bag' and not 'RosBagHandler' in f.meta:
          #   print('RosBagHandler not in meta for ',f.local_path)

          if 'RosBagHandler' in f.meta and 'start_time' in f.meta['RosBagHandler'] and 'end_time' in f.meta['RosBagHandler']:
            f_start_time = f.meta['RosBagHandler']['start_time']
            f_end_time = f.meta['RosBagHandler']['end_time']
            if f_start_time < end_time and f_end_time > start_time:
              #print('      ',f.local_path.name)
              if 'ROBOBOX' in f.local_path.name:
                bagfiles['robobox'].append(f)
              elif 'DRIX' in f.local_path.name and f.local_path.parts[-2] == 'mission_logs':
                bagfiles['drix'].append(f)
              elif 'project11' in f.local_path.name:
                if 'project11_operator' in f.local_path.name:
                  bagfiles['p11_operator'].append(f)
                else:
                  bagfiles['p11'].append(f)

        for sources in bagfiles:
          print('  ',sources,len(bagfiles[sources]),'sources')
          bounds = {}
          tracks = {}
          for fi in bagfiles[sources]:
            if fi.meta is not None and 'RosBagHandler' in fi.meta:
              for k in fi.meta['RosBagHandler']:
                #print (' ',k)
                if 'tracks' in fi.meta['RosBagHandler']:
                  for v in fi.meta['RosBagHandler']['tracks']:
                    if not v in tracks:
                      tracks[v] = []
                      bounds[v] = {'min':{},'max':{}}
                    last_time = None
                    for p in fi.meta['RosBagHandler']['tracks'][v]:
                      if p['timestamp'] >= start_time and p['timestamp'] <= end_time:
                        if last_time is None or p['timestamp'] >= last_time+1.0:
                          tracks[v].append(datetime.datetime.fromtimestamp(p['timestamp'], tz=datetime.timezone(datetime.timedelta(0.0))).isoformat()+','+str(p['timestamp'])+','+str(p['latitude'])+','+str(p['longitude'])+','+str(p['altitude']))
                          last_time = p['timestamp']
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
          for v in tracks:
            tracks[v].sort()
            dedup_track = []
            dedup_track.append(tracks[v][0])
            for p in tracks[v]:
              if p != dedup_track[-1]:
                dedup_track.append(p)
            print('original:',len(tracks[v]),'dedup:',len(dedup_track))
            tracks[v] = dedup_track
            nav_file = output_path/sources/(v+'.txt')
            nav_file.parent.mkdir(parents=True, exist_ok=True)
            nav = (nav_file).open('w')
            for p in  tracks[v]:
              nav.write(p+'\n')
            nav.close()
            bounds_path = output_path/sources/(v+'_bounds.json')
            bounds_path.parent.mkdir(parents=True, exist_ok=True)
            bounds_file = bounds_path.open('w')
            json.dump(bounds[v],bounds_file)
            odm_utils.toKML(output_path/sources/(v+'.kml'),tracks[v], deployment_id+'_'+v,{'Mothership':'mothership','DriX':'drix','Nautilus':'mothership','nui':'nui','Mesobot':'mesobot'}[v])
          






