#!/usr/bin/env python3

import rosbag
import rospy
import datetime

from file_info import FileInfo

class RosBagHandler:
  def __init__(self):
    pass


  def needsProcessing(self, file: FileInfo):
    if file.local_path.suffix == '.bag':
      if file.has_meta_value(self, 'start_time') and not file.is_modified():
        return False
      if file.meta is not None and 'RosBagIndexHandler' in file.meta:
        if 'indexed' in file.meta['RosBagIndexHandler'] and not file.meta['RosBagIndexHandler']['indexed']:
          return False
      if file.has_meta_value(self, 'message_count') and file.get_meta_value(self, 'message_count') == 0:
        return False
      return True
    return False

  def process(self, file: FileInfo) -> FileInfo:
    try:
      bag = rosbag.Bag(file.source_path())
    except Exception as e:
      print("error opening bag file",file.local_path,e)
      print(type(e))
      return
    try:
      file.update_meta_value(self, 'message_count', bag.get_message_count())
      if bag.get_message_count() == 0:
        return
      file.update_meta_value(self, 'start_time', bag.get_start_time())
      file.update_meta_value(self, 'end_time', bag.get_end_time())
      tt = bag.get_type_and_topic_info()
      if not '/gps' in tt.topics:
        return
    except Exception as e:
      print("error getting times from bag file",file.local_path,e)
      print(type(e))
      return

    track = []
    last_report_time = None
    interval = rospy.Duration(secs=1.0)
    try:
      for topic, msg, t in bag.read_messages(topics=['/gps']):
        if msg.fix_quality > 0:
          if last_report_time is None or msg.header.stamp - last_report_time >= interval:
            fix = {'timestamp': msg.header.stamp.to_sec()}
            fix['latitude'] = msg.latitude
            fix['longitude'] = msg.longitude
            track.append(fix)
            last_report_time = msg.header.stamp
    except Exception as e:
      print("error extracting nav from bag file",file.local_path, e)

    if len(track):
      min_lat = max_lat = track[0]['latitude']
      min_lon = max_lon = track[0]['longitude']
      for fix in track:
        min_lat = min(min_lat, fix['latitude'])
        max_lat = max(max_lat, fix['latitude'])
        min_lon = min(min_lon, fix['longitude'])
        max_lon = max(max_lon, fix['longitude'])
      file.update_meta_value(self, 'bounds', {'min': {'latitude': min_lat, 'longitude': min_lon}, 'max': {'latitude': max_lat, 'longitude': max_lon}})
      file.update_meta_value(self, 'track', track)
