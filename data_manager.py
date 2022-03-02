#!/usr/bin/env python3

import sys
import pathlib
import hashlib
import datetime

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

if len(sys.argv) < 2:
  usage()

top_level = pathlib.Path(sys.argv[-1]).absolute() # last item, stuff before could be options

if not top_level.is_dir():
  print(top_level,"does not seem to be a directory")
  usage()

print (top_level)

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
file_info = []
total_size = 0
start_time = datetime.datetime.now()

for potential_file in top_level.glob("**/*"):
  if potential_file.is_file():
    files.append(potential_file)

count = 0
next_report = 0
for i in range(len(files)):
  h,s = get_hash(files[i])
  file_info.append((files[i],h,s))
  total_size += s
  count += 1
  if count >= next_report:
    next_report += 50
    time_so_far = datetime.datetime.now()-start_time
    print (count,'of',len(files),' files, seconds elapsed:', time_so_far.total_seconds(), 'rate:', total_size/time_so_far.total_seconds(),'bytes per second')

end_time = datetime.datetime.now()

print("hashed",len(file_info), 'files totaling', total_size, 'bytes in', (end_time-start_time).total_seconds(), ' seconds.')
