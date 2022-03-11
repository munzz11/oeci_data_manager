#!/usr/bin/env python3

import rosbag
import rospy
import datetime

class RosBagHandler:
  def __init__(self):
    pass


  def needsProcessing(self, filename, meta):
    if filename.suffix == '.bag':
      meta['ros_nav_file'] = filename.parent/(filename.name+'.nav.txt')
      if meta['needs_update']:
        return True
      if meta['ros_nav_file'].is_file():
        nav_mod_time = meta['ros_nav_file'].stat().st_mtime
        if nav_mod_time > meta['modify_time']:
          return False
      return True
    return False

  def process(self, filename, meta):
    try:
      bag = rosbag.Bag(filename)
      meta['saved']['ros_start_time'] = bag.get_start_time()
      meta['saved']['ros_end_time'] = bag.get_end_time()
      tt = bag.get_type_and_topic_info()
      if not '/gps' in tt.topics:
        return
    except Exception:
      print("error processing",filename.absolute())
      return

    track = []
    last_report_time = None
    interval = rospy.Duration(secs=1.0)
    try:
      for topic, msg, t in bag.read_messages(topics=['/gps']):
        if msg.fix_quality > 0:
          if last_report_time is None or msg.header.stamp - last_report_time >= interval:
            fix = {'timestamp': msg.header.stamp}
            fix['latitude'] = msg.latitude
            fix['longitude'] = msg.longitude
            track.append(fix)
            last_report_time = msg.header.stamp
    except Exception:
      print("error processing",filename.absolute())

    if len(track):
      min_lat = max_lat = track[0]['latitude']
      min_lon = max_lon = track[0]['longitude']
      for fix in track:
        min_lat = min(min_lat, fix['latitude'])
        max_lat = max(max_lat, fix['latitude'])
        min_lon = min(min_lon, fix['longitude'])
        max_lon = max(max_lon, fix['longitude'])
      meta['saved']['bounds'] = {'min': {'latitude': min_lat, 'longitude': min_lon}, 'max': {'latitude': max_lat, 'longitude': max_lon}}
    
    with meta['ros_nav_file'].open(mode="w") as nf:
      for fix in track:
        nf.write(datetime.datetime.utcfromtimestamp(fix['timestamp'].to_sec()).isoformat())
        nf.write(', ')
        nf.write(str(fix['latitude']))
        nf.write(', ')
        nf.write(str(fix['longitude']))
        nf.write('\n')



if __name__ == '__main__':
  import sys
  import pathlib
  bagfile = pathlib.Path(sys.argv[1])
  c = {}
  rbh = RosBagHandler()
  rbh.process(bagfile, c)
  print (c)