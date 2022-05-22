#!/usr/bin/env python3

from numpy import empty
import rosbag
import rospy
import datetime

from file_info import FileInfo

class RosBagHandler:
  position_topics = {'/gps':'DriX',
    '/project11/mesobot/nav/position':'Mesobot',
    '/project11/nui/nav/position':'nui',
    '/project11/nautilus/position':'Nautilus',
    '/mothership_gps':'Mothership'
  }

  def __init__(self):
    pass


  def needsProcessing(self, file: FileInfo):
    if file.local_path.suffix == '.bag':
      return True
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
      topics = []
      for t in RosBagHandler.position_topics:
        if t in tt.topics:
          topics.append(t)
      if len(topics) == 0:
        return
    except Exception as e:
      print("error getting times from bag file",file.local_path,e)
      print(type(e))
      return

    tracks = {}
    last_report_times = {}
    interval = rospy.Duration(secs=1.0)
    try:
      for topic, msg, t in bag.read_messages(topics=topics):
        vehicle = RosBagHandler.position_topics[topic]
        if not vehicle in tracks:
          tracks[vehicle] = []
          last_report_times[vehicle] = None
        if topic in ('/gps','/mothership_gps'):
          if msg.fix_quality > 0:
            if last_report_times[vehicle] is None or msg.header.stamp - last_report_times[vehicle] >= interval:
              fix = {'timestamp': msg.header.stamp.to_sec()}
              fix['latitude'] = msg.latitude
              fix['longitude'] = msg.longitude
              fix['altitude'] = 0.0
              tracks[vehicle].append(fix)
              last_report_times[vehicle] = msg.header.stamp
        elif topic in ('/project11/mesobot/nav/position', '/project11/nui/nav/position'):
          if last_report_times[vehicle] is None or msg.header.stamp - last_report_times[vehicle] >= interval:
              fix = {'timestamp': msg.header.stamp.to_sec()}
              fix['latitude'] = msg.pose.position.latitude
              fix['longitude'] = msg.pose.position.longitude
              fix['altitude'] = msg.pose.position.altitude
              tracks[vehicle].append(fix)
              last_report_times[vehicle] = msg.header.stamp
        else:
          if msg.status.status >= 0:
            if last_report_times[vehicle] is None or msg.header.stamp - last_report_times[vehicle] >= interval:
              fix = {'timestamp': msg.header.stamp.to_sec()}
              fix['latitude'] = msg.latitude
              fix['longitude'] = msg.longitude
              fix['altitude'] = msg.altitude
              tracks[vehicle].append(fix)
              last_report_times[vehicle] = msg.header.stamp

          
    except Exception as e:
      print("error extracting nav from bag file",file.local_path, e)

    bounds = {}
    tracks_for_meta = {}
    for v in tracks:
      if len(tracks[v]):
        min_lat = max_lat = tracks[v][0]['latitude']
        min_lon = max_lon = tracks[v][0]['longitude']
        for fix in tracks[v]:
          min_lat = min(min_lat, fix['latitude'])
          max_lat = max(max_lat, fix['latitude'])
          min_lon = min(min_lon, fix['longitude'])
          max_lon = max(max_lon, fix['longitude'])
        bounds[v] = {'min': {'latitude': min_lat, 'longitude': min_lon}, 'max': {'latitude': max_lat, 'longitude': max_lon}}
        tracks_for_meta[v] = tracks[v]

    if len(bounds):
      file.update_meta_value(self, 'bounds', bounds)
      file.update_meta_value(self, 'tracks', tracks_for_meta)
