#!/usr/bin/env python3

# run this in 01-catalog

import glob
import pathlib
import json

for bounds_file in glob.glob("D*/*/*_bounds.json"):
  print(bounds_file)
  bounds_path = pathlib.Path(bounds_file)
  track_path = bounds_path.parent / pathlib.Path(bounds_path.name[:-12]+'.txt')
  print(track_path)
  positions = []
  for line in open(track_path).readlines():
    parts = line.strip().split(',')
    lat = float(parts[2])
    lon = float(parts[3])
    if lat != 0:
      positions.append((lat,lon))
  min_lat = lat
  max_lat = lat
  min_lon = lon
  max_lon = lon
  for p in positions:
    min_lat = min(min_lat, p[0])
    max_lat = max(max_lat, p[0])
    min_lon = min(min_lon, p[1])
    max_lon = max(max_lon, p[1])
  out = {"min":{"latitude": min_lat, "longitude": min_lon}, "max":{"latitude": max_lat, "longitude": max_lon}}
  json.dump(out, open(bounds_file,'w'), indent=2)
  print(json.dumps(out, indent=2))
