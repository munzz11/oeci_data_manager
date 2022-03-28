#!/usr/bin/env python3

import pathlib


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
  kml_out.close()

def resolvePath(path: pathlib.Path):
  ret = path.expanduser().resolve()
  if not ret.is_absolute():
    ret = (pathlib.Path('.')/ret).resolve()
  return ret
