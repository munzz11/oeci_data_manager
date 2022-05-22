#!/usr/bin/env python3

import pathlib
import math

# from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
def human_readable_size(size, decimal_places=3):
    for unit in ['B','KiB','MiB','GiB','TiB']:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{unit}"


kml_template = '''<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>{name}</name>
{style}
    <Placemark>
      <name>{name}</name>
      <styleUrl>#{style_id}</styleUrl>
      <LineString>
        <extrude>0</extrude>
        <tessellate>1</tessellate>
        <altitudeMode>absolute</altitudeMode>
        <coordinates>{coordinates}</coordinates>
      </LineString>
    </Placemark>
  </Document>
</kml>
'''

kml_style_template = '''
    <Style id="{style_id}">
      <LineStyle>
        <color>{line_color}</color>
        <width>4</width>
      </LineStyle>
      <PolyStyle>
        <color>{poly_color}</color>
      </PolyStyle>
    </Style>
'''

def toKML(output_file: pathlib.Path, track, label, style):
  styles = {}
  styles['drix'] = kml_style_template.format(style_id='drix', line_color='FF0000FF', poly_color='FF00007F')
  styles['mesobot'] = kml_style_template.format(style_id='mesobot', line_color='FF00FFFF', poly_color='7FFF00FF')
  styles['nui'] = kml_style_template.format(style_id='nui', line_color='FF00A5FF', poly_color='7F00A5FF')
  styles['mothership'] = kml_style_template.format(style_id='mothership', line_color='FFFF0000', poly_color='7FFF0000')

  coordinates = ''
  skip = 1
  max_points = 65536
  if len(track) > max_points:
    skip = math.ceil(len(track)/float(max_points))
  for i in range(0,len(track),skip):
    parts = track[i].split(',')
    coordinates+= parts[3]+','+parts[2]+','+parts[4]+'\n'

  output_file.parent.mkdir(parents=True, exist_ok=True)
  kml_out = output_file.open(mode='w')
  kml_out.write(kml_template.format(name=label,style=styles[style],style_id=style, coordinates=coordinates))

  kml_out.close()

def resolvePath(path: pathlib.Path):
  ret = path.expanduser().resolve()
  if not ret.is_absolute():
    ret = (pathlib.Path('.')/ret).resolve()
  return ret
