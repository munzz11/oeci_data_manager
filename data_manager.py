#!/usr/bin/env python3

import sys
import pathlib
import hashlib
import datetime
import json
import time

from multiprocessing import Pool

from ros_bag_handler import RosBagHandler

def usage():
  print ("Usage: data_manager.py top_level_dir")
  sys.exit(1)

class HashHandler:
  def __init__(self):
    self.hasher = hashlib.sha256
    self.label = 'sha256'

  def needsProcessing(self, filename, meta):
    return meta['needs_update'] or not 'hash' in meta['saved']

  def process(self, filename, meta):
    hash = self.hasher()
    with open(filename, 'rb') as f:
      for chunk in iter(lambda: f.read(4096), b""):
        hash.update(chunk)
    meta['saved']['hash'] = hash.hexdigest()

class MetaReader:
  def __init__(self, toplevel, meta_root='.data_manager', ignore=[]):
    self.toplevel = toplevel
    self.meta_root = toplevel/meta_root
    self.ignore = ignore

  def needsProcessing(self, filename, meta):
    if filename in self.ignore:
      return False
    if not self.meta_root in filename.parents:
      return filename.is_file()
    return False

  def process(self, filename, meta):
    file_size = filename.stat().st_size
    mod_time = filename.stat().st_mtime
    needs_update = True
    meta_file = self.meta_root/filename.relative_to(self.toplevel).parent/(f.name+'.json')
    if meta_file.is_file():
      m = json.load(open(meta_file))
      needs_update = not ('size' in m and m['size'] == file_size and 'modify_time' in m and m['modify_time'] == mod_time)
      meta['saved'] = m
    else:
      meta['saved'] = {}
    meta['size'] = file_size
    meta['modify_time'] = mod_time
    meta['needs_update'] = needs_update
    meta['meta_file'] = meta_file

class MetaSaver:
  def __init__(self):
    pass

  def needsProcessing(self, filename, meta):
    return meta['needs_update']

  def process(self, filename, meta):
    meta['saved']['size'] = meta['size']
    meta['saved']['modify_time'] = meta['modify_time']
    meta['meta_file'].parent.mkdir(parents=True, exist_ok=True)
    json.dump(meta['saved'], open(meta['meta_file'],"w"))


# from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
def human_readable_size(size, decimal_places=3):
    for unit in ['B','KiB','MiB','GiB','TiB']:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{unit}"

def toKML(nav_file):
  kml_out = (nav_file.parent/(nav_file.name+'.kml')).open(mode='w')
  kml_out.write('''<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>''')
  kml_out.write(nav_file.name)
  kml_out.write('''</name>
    <Style id="yellowLineGreenPoly">
      <LineStyle>
        <color>7f00ffff</color>
        <width>4</width>
      </LineStyle>
      <PolyStyle>
        <color>7f00ff00</color>
      </PolyStyle>
    </Style>
    <Placemark>
      <name>deployment</name>
      <styleUrl>#yellowLineGreenPoly</styleUrl>
      <LineString>
        <extrude>1</extrude>
        <tessellate>1</tessellate>
        <altitudeMode>absolute</altitudeMode>
        <coordinates>''')
  for l in nav_file.open().readlines():
    t,lat,lon = l.strip().split(',')
    kml_out.write(lon.strip()+','+lat.strip()+',0\n')
  kml_out.write('''</coordinates>
      </LineString>
    </Placemark>
  </Document>
</kml>''')


def processFile(filename, meta):
  pipeline = []

  saver = MetaSaver()
  pipeline.append(RosBagHandler())
  pipeline.append(saver)
  pipeline.append(HashHandler())
  pipeline.append(saver)

  for processor in pipeline:
    if processor.needsProcessing(filename, meta):
      processor.process(filename, meta)

  return filename



if __name__ == '__main__':

  if len(sys.argv) < 2:
    usage()

  process_count = 1
  if len(sys.argv) > 2:
    process_count = int(sys.argv[1])

  print ("process count", process_count)

  manifest_path = 'manifest.txt'

  top_level = pathlib.Path(sys.argv[-1]).absolute() # last item in argv, stuff before could be options

  if not top_level.is_dir():
    print(top_level,"does not seem to be a directory")
    usage()

  print (top_level)

  data_manager_dir = top_level/'.data_manager'
  print("config dir:",data_manager_dir," is dir?", data_manager_dir.is_dir())

  expedition = top_level.parts[-1]
  print('\nExpedition:', expedition)

  description_path = top_level / 'ExpeditionDescription.json'

  print("  Description present?", description_path.is_file())

  platforms = []
  for p in top_level.glob('*'):
    if p != data_manager_dir:
      if p.is_dir():
        platforms.append(p)

  for platform in platforms:
    print ('    Platform:', platform.parts[-1])
    data_stages = []
    for p in platform.glob('*'):
      if p.is_dir():
        data_stages.append(p)
    for stage in data_stages:
      print ('      data stage:', stage.parts[-1])
      sensors = []
      for p in stage.glob('*'):
        if p.is_dir():
          sensors.append(p)
      for sensor in sensors:
        print ('        sensor:', sensor.parts[-1])

  files = []
  start_time = datetime.datetime.now()


  meta_reader = MetaReader(top_level, ignore=[top_level/manifest_path,])

  for potential_file in top_level.glob("**/*"):
    if meta_reader.needsProcessing(potential_file, None):
      files.append(potential_file)


  metadata = {}
  total_size = 0

  for f in files:
    metadata[f] = {}
    meta_reader.process(f, metadata[f])
    total_size += metadata[f]['size']

  print ('scan time:', datetime.datetime.now()-start_time)
  print (len(files),'files totaling',human_readable_size(total_size),'bytes')

  last_report_time = datetime.datetime.now()
  last_report_newly_processed_size = 0
  newly_processed_count = 0
  newly_processed_size = 0

  pipeline = []

  pipeline.append(RosBagHandler())
  pipeline.append(HashHandler())

  need_processing_size = 0
  need_processing_files = []

  for processor in pipeline:
    for f in files:
      if processor.needsProcessing(f, metadata[f]):
        if not f in need_processing_files:
          need_processing_size += metadata[f]['size']
          need_processing_files.append(f)

  print(len(need_processing_files),'files need processing totaling', human_readable_size(need_processing_size))

  start_process_time = datetime.datetime.now()


  multi = process_count > 1

  if multi:
    pool = Pool(processes=process_count)
    results_list = []

    for f in need_processing_files:
      if len(results_list) < process_count*2:
        results_list.append(pool.apply_async(processFile,(f,metadata[f])))
      while len(results_list) >= process_count*2:
        done_list = []
        for r in results_list:
          if r.ready():
            done_list.append(r)
        if len(done_list):
          for d in done_list:
            fname = d.get()
            newly_processed_count += 1
            newly_processed_size += metadata[fname]['size']
            results_list.remove(d)
          now = datetime.datetime.now()
          since_last_report = now-last_report_time
          if since_last_report > datetime.timedelta(seconds=5):
            processing_rate = (newly_processed_size-last_report_newly_processed_size)/since_last_report.total_seconds()
            estimated_time_remaining = datetime.timedelta(seconds=(need_processing_size - newly_processed_size) / processing_rate)
            percentage_complete = 100*newly_processed_size/need_processing_size
            print("percent complete:", percentage_complete,"rate:",human_readable_size(processing_rate)+'/s',"estimated time remaining:", estimated_time_remaining)
            last_report_time = now
            last_report_newly_processed_size = newly_processed_size
        else:
          time.sleep(.05)
    for r in results_list:
      r.wait()
  else:

    for f in need_processing_files:
      processFile(f, metadata[f])
      newly_processed_count += 1
      newly_processed_size += metadata[f]['size']
      now = datetime.datetime.now()
      since_last_report = now-last_report_time
      if since_last_report > datetime.timedelta(seconds=5):
        processing_rate = (newly_processed_size-last_report_newly_processed_size)/since_last_report.total_seconds()
        estimated_time_remaining = datetime.timedelta(seconds=(need_processing_size - newly_processed_size) / processing_rate)
        percentage_complete = 100*newly_processed_size/need_processing_size
        print("percent complete:", percentage_complete,"rate:",human_readable_size(processing_rate)+'/s',"estimated time remaining:", estimated_time_remaining)
        last_report_time = now
        last_report_newly_processed_size = newly_processed_size

  end_time = datetime.datetime.now()

  print("processed", newly_processed_count, 'files totaling', human_readable_size(newly_processed_size), ' duration:', (end_time-start_process_time))


  for p in platforms:
    all_nav = {}

    nav_files = (top_level/p).glob('**/*.bag.nav.txt')
    for nf in nav_files:
      with nf.open() as f:
        for l in f.readlines():
          time, position = l.strip().split(',',1)
          time = datetime.datetime.fromisoformat(time)
          all_nav[time]=l.strip()
    
    time_sorted_nav = {k: all_nav[k] for k in sorted(all_nav)}

    deployments_file = top_level/p/'01-catalog/deployments.json'
    deployment_nav_files = []
    if deployments_file.is_file():
      deployments_info = json.load(deployments_file.open())
      for di in deployments_info:
        print(di)
        for dr in deployments_info[di]:
          print (' ',dr)
          for d in deployments_info[di][dr]:
            for deployment_id in d:
              print ('  ',deployment_id)
              start_time = datetime.datetime.fromisoformat(d[deployment_id][0])
              end_time = datetime.datetime.fromisoformat(d[deployment_id][1])
              print(start_time,end_time)
              deployment_nav_file = top_level/p/('01-catalog/deployment_'+deployment_id+'.nav.txt')
              deployment_nav = open(deployment_nav_file,'w')
              for t in time_sorted_nav:
                if t >= start_time and t <= end_time:
                  deployment_nav.write(time_sorted_nav[t]+'\n')
              deployment_nav_files.append(deployment_nav_file)
    for f in deployment_nav_files:
      toKML(f)


  for f in files:
    meta_reader.process(f, metadata[f])

  manifest = []
  for f in sorted(files):
    try:
      manifest.append((f.relative_to(top_level),metadata[f]['saved']['hash']))
    except KeyError:
      print ('missing hash',f.relative_to(top_level))
      print (metadata[f])

  manifest_file = open(top_level/manifest_path,'w')
  for f in manifest:
    if f[0] != manifest_file:
      manifest_file.write(str(f[1])+'  '+str(f[0])+'\n')
