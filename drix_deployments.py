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
  def __init__(self, project: Project, verbose=0):
    self.project = project
    self.verbose = verbose

  def generate(self):
    # Loops over possible platforms, drix08, plus others, in the top level archive directory.
    for platform in self.project.platforms():
      print("Extracting deployment data for %s" % platform)
      # Read the deployments json file, which gives the name and time bounds of each platform deployment.
      deployments_path = self.project.find_source_path(pathlib.Path(platform)/'01-catalog/deployments.json')
      deployments_info = json.load(open(deployments_path))
      # Loop through each deployment.
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

            # Find the logs from each source having timestamps within the bounds of the 
            # deployment start and end time:
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

        # For each bag recording source (drix mdt, robobox, project11, etc.)
        for sources in bagfiles:
          print('  ',sources,len(bagfiles[sources]),'sources')
          bounds = {}
          deployment_tracks = {}
          # For each bagfile.
          for fi in bagfiles[sources]:
            if fi.meta is not None and 'RosBagHandler' in fi.meta:
              # The metadata consists a list of dictionaries. Each "handler" optionally creates
              # metadata about the file, creating one of these dictionaries. This for loop
              # seems like it is not required. 
              for k in fi.meta['RosBagHandler']:
                #print (' ',k)
                # The RosBagHandler extracts the position data from each bag file, and stores this in the 
                # meta data as a "tracks" dictionary keyed by the vehicle name.  
                if 'tracks' in fi.meta['RosBagHandler']:
                  # Then we loop through each vehicle track.
                  for vehicle in fi.meta['RosBagHandler']['tracks']:
                    
                    if not vehicle in deployment_tracks:
                      deployment_tracks[vehicle] = []
                      bounds[vehicle] = {'min':{},'max':{}}
                    last_time = None
                    for position in fi.meta['RosBagHandler']['tracks'][vehicle]:
                      if position['timestamp'] >= start_time and position['timestamp'] <= end_time:
                        if last_time is None or position['timestamp'] >= last_time+1.0:
                          deployment_tracks[vehicle].append(datetime.datetime.fromtimestamp(position['timestamp'], tz=datetime.timezone(datetime.timedelta(0.0))).isoformat()+','+str(position['timestamp'])+','+str(position['latitude'])+','+str(position['longitude'])+','+str(position['altitude']))
                          last_time = position['timestamp']
                        if 'latitude' in bounds[vehicle]['min']:
                          bounds[vehicle]['min']['latitude'] = min(position['latitude'],bounds[vehicle]['min']['latitude'])
                          bounds[vehicle]['min']['longitude'] = min(position['longitude'],bounds[vehicle]['min']['longitude'])
                          bounds[vehicle]['min']['altitude'] = min(position['altitude'],bounds[vehicle]['min']['altitude'])
                          bounds[vehicle]['max']['latitude'] = min(position['latitude'],bounds[vehicle]['max']['latitude'])
                          bounds[vehicle]['max']['longitude'] = min(position['longitude'],bounds[vehicle]['max']['longitude'])
                          bounds[vehicle]['max']['altitude'] = min(position['altitude'],bounds[vehicle]['max']['altitude'])
                        else:
                          bounds[vehicle]['min']['latitude'] = position['latitude']
                          bounds[vehicle]['min']['longitude'] = position['longitude']
                          bounds[vehicle]['min']['altitude'] = position['altitude']
                          bounds[vehicle]['max']['latitude'] = position['latitude']
                          bounds[vehicle]['max']['longitude'] = position['longitude']
                          bounds[vehicle]['max']['altitude'] = position['altitude']
              
          # Now we've looped through all the bag files, extracting positions that fall within
          # the deployment time span. Because topics get duplicated between vehicle and 
          # operating station, we deduplicate them here.
          for vehicle in deployment_tracks:
            deployment_tracks[vehicle].sort()
            dedup_track = []
            dedup_track.append(deployment_tracks[vehicle][0])
            for position in deployment_tracks[vehicle]:
              if position != dedup_track[-1]:
                dedup_track.append(position)
            print('original:',len(deployment_tracks[vehicle]),'dedup:',len(dedup_track))
            deployment_tracks[vehicle] = dedup_track
            # Write the navigation file.
            nav_file = output_path/sources/(vehicle+'.txt')
            nav_file.parent.mkdir(parents=True, exist_ok=True)
            print("Writing nav_file: %s" % nav_file)
            nav = (nav_file).open('w')
            for position in  deployment_tracks[vehicle]:
              nav.write(position+'\n')
            nav.close()
            # Write the navigation bounds file.
            bounds_path = output_path/sources/(vehicle+'_bounds.json')
            bounds_path.parent.mkdir(parents=True, exist_ok=True)
            print("Writing deployment spatial bounds: %s" % bounds_path)
            bounds_file = bounds_path.open('w')
            json.dump(bounds[vehicle],bounds_file)
            # Write to kml.
            print("Writing to kml.")
            odm_utils.toKML(output_path/sources/(vehicle+'.kml'),deployment_tracks[vehicle], deployment_id+'_'+vehicle,{'Mothership':'mothership','DriX':'drix','Nautilus':'mothership','nui':'nui','Mesobot':'mesobot'}[vehicle])
          






