#!/usr/bin/env python3

from numpy import empty
import rosbag
import rospy
import datetime
import json
from pathlib import Path
from file_info import FileInfo

class RosBagHandler:
  
  # Load position topics from JSON file
    _position_topics_path = Path(__file__).parent / "position_topics.json"
    
    @classmethod
    def _load_position_topics(cls):
        try:
            with open(cls._position_topics_path) as f:
                json_data = json.load(f)
            return {
                entry[0]["topic"]: entry[1]["platform"] 
                for entry in json_data
            }
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            raise RuntimeError(f"Error loading position topics: {str(e)}") from e

    position_topics = _load_position_topics()

  def __init__(self):
    pass

  def get_msg_types(self,tt):
    '''Returns a dictionary of of message topics (k) and types (v) found in a bag file.''' 
    d = {}
    for k,v in tt[1].items():
      d[k]=v[0]
    print(d)
    return d

  def needsProcessing(self, file: FileInfo):
    if file.local_path.suffix == '.bag' and "mbes" not in file.local_path.parts:
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
      
      # Get all the message types in this bag.
      msg_types = self.get_msg_types(tt)

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
        #print("Found vehicle %s in log %s" % (vehicle,file.source_path()))
        # Initialize a new track for this vehicle.
        if not vehicle in tracks:
          tracks[vehicle] = []
          last_report_times[vehicle] = None
          # TODO: These if/then statements handle various ways to determine if the position
          # information is valid. This should be done by message type not by topic name, so the
          # topics are not hard coded here.
        if msg_types[topic] == 'mdt_msgs/Gps':
          if msg.fix_quality > 0:
            if last_report_times[vehicle] is None or msg.header.stamp - last_report_times[vehicle] >= interval:
              fix = {'timestamp': msg.header.stamp.to_sec()}
              fix['latitude'] = msg.latitude
              fix['longitude'] = msg.longitude
              fix['altitude'] = 0.0
              tracks[vehicle].append(fix)
              last_report_times[vehicle] = msg.header.stamp
        #elif topic in ('/project11/mesobot/sensors/nav/pose','/project11/nui/nav/position'):
        elif msg_types[topic] == 'geographic_msgs/GeoPoseStamped':
          if last_report_times[vehicle] is None or msg.header.stamp - last_report_times[vehicle] >= interval:
              fix = {'timestamp': msg.header.stamp.to_sec()}
              fix['latitude'] = msg.pose.position.latitude
              fix['longitude'] = msg.pose.position.longitude
              fix['altitude'] = msg.pose.position.altitude
              tracks[vehicle].append(fix)
              last_report_times[vehicle] = msg.header.stamp
        # elif topic in ('/project11/nui/nav/position',):
        #   if last_report_times[vehicle] is None or msg.header.stamp - last_report_times[vehicle] >= interval:
        #       fix = {'timestamp': msg.header.stamp.to_sec()}
        #       fix['latitude'] = msg.position.latitude
        #       fix['longitude'] = msg.position.longitude
        #       fix['altitude'] = msg.position.altitude
        #       tracks[vehicle].append(fix)
        #       last_report_times[vehicle] = msg.header.stamp
        elif msg_types[topic] == 'sensor_msgs/NavSatFix':
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
        min_lat = tracks[v][0]['latitude']
        max_lat = tracks[v][0]['latitude']
        min_lon = tracks[v][0]['longitude']
        max_lon = tracks[v][0]['longitude']
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
