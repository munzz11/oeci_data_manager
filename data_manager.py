#!/usr/bin/env python3

import sys
import pathlib
import hashlib
import datetime
import json

def usage():
  print ("Usage: data_manager.py top_level_dir")
  sys.exit(1)

def get_hash(filename):
  hash = hashlib.sha256()
  datasize = 0
  #hash = hashlib.md5()
  with open(filename, 'rb') as f:
    for chunk in iter(lambda: f.read(4096), b""):
      hash.update(chunk)
      datasize += len(chunk)
  return hash.hexdigest(), datasize

# from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
def human_readable_size(size, decimal_places=3):
    for unit in ['B','KiB','MiB','GiB','TiB']:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{unit}"

if len(sys.argv) < 2:
  usage()

top_level = pathlib.Path(sys.argv[-1]).absolute() # last item, stuff before could be options

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

manifest = []

for potential_file in top_level.glob("**/*"):
  if not data_manager_dir in potential_file.parents:
    if potential_file.is_file():
      files.append(potential_file)

configs = {}
total_size = 0
need_hash_size = 0
need_hash_count = 0


for f in files:
  configs[f] = {}
  configs[f]['json_file'] = data_manager_dir/f.relative_to(top_level).parent/(f.name+'.json')
  if configs[f]['json_file'].is_file():
    config = json.load(open(configs[f]['json_file']))
  else:
    config = {}

  file_size = f.stat().st_size
  mod_time = f.stat().st_mtime

  need_hash = not ('size' in config and config['size'] == file_size and 'modify_time' in config and config['modify_time'] == mod_time)
  if need_hash or not 'hash' in config:
    config['hash'] = None

  config['size'] = file_size
  config['modify_time'] = mod_time
  total_size += file_size
  if config['hash'] is None:
    need_hash_size += file_size
    need_hash_count += 1

  configs[f]['config'] = config

start_hash_time = datetime.datetime.now()
print ('scan time:', start_hash_time-start_time)

print (len(files),'files totaling',human_readable_size(total_size),'bytes')
print (need_hash_count,'need hashing totaling',human_readable_size(need_hash_size),'bytes')

newly_hashed_count = 0
newly_hashed_size = 0

last_report_time = start_hash_time
last_report_newly_hashed_size = 0

for f in files:
  if configs[f]['config']['hash'] is None:
    h,s = get_hash(f)
    configs[f]['config']['hash'] = h
    configs[f]['json_file'].parent.mkdir(parents=True, exist_ok=True)
    json.dump(configs[f]['config'], open(configs[f]['json_file'],"w"))
    newly_hashed_count += 1
    newly_hashed_size += configs[f]['config']['size']
    now = datetime.datetime.now()
    since_last_report = now-last_report_time
    if since_last_report > datetime.timedelta(seconds=5):
      hash_rate = (newly_hashed_size-last_report_newly_hashed_size)/since_last_report.total_seconds()
      estimated_time_remaining = (need_hash_size - newly_hashed_size) / hash_rate
      percentage_complete = 100*newly_hashed_size/need_hash_size
      print("percent complete:", percentage_complete,"rate:",human_readable_size(hash_rate)+'/s',"estimated seconds remaining:", estimated_time_remaining)
      last_report_time = now
      last_report_newly_hashed_size = newly_hashed_size
  manifest.append((f.relative_to(top_level),configs[f]['config']['hash']))


end_time = datetime.datetime.now()

print("hashed", newly_hashed_count, 'files totaling', human_readable_size(newly_hashed_size), ' duration:', (end_time-start_hash_time))

manifest_file = open(top_level/"manifest_temp.txt",'w')
for f in manifest:
  if f[0] != manifest_file:
    manifest_file.write(str(f[0])+' '+f[1]+'\n')
