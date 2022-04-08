#!/usr/bin/env python3

from logging import handlers
from operator import index
import sys
import pathlib
import datetime
import json
import time

from hash_handler import HashHandler
from ros_bag_handler import RosBagHandler
from ros_bag_index_handler import RosBagIndexHandler

from config import ConfigPath
from project import Project

from odm_utils import human_readable_size

def usage(error = None):
  if error is not None:
    print("\nERROR:",error)
    print()
  print ("Usage: oeci_data_manager.py [options] command [command options]")
  print ("  options:")
  print ("    --config-dir conf_dir   (default: ~/.oeci_data_manager)")
  print ("    --verbose               (default: False)")
  print ("  commands:")
  print ("    list     List existing projects")
  print ("    init     Initialize a new project")
  print ("      --source source_dir       (required)")
  print ("      --label project_label     (default: top level of source dir)")
  print ("      --output output_dir       (default: source dir)")
  print ("    scan     Scan for files needing processing")
  print ("      --project project         (required)")
  print ("      --process_count job_count (default: 1)")
  print ("    process  Process files")
  print ("      --project project         (required)")
  print ("      --process_count job_count (default: 1)")
  print ("    gui      Launch the user interface")
  print()
  sys.exit(1)

class ScanProgress:
  def __init__(self) -> None:
    self.report_interval = datetime.timedelta(seconds=5)
    self.last_report_time = datetime.datetime.now()

  def __call__(self, file_count):
    now = datetime.datetime.now()
    if now - self.last_report_time >= self.report_interval:
      print(file_count,'files scanned')
      self.last_report_time = now

class ProcessProgress:
  def __init__(self, need_processing_count, need_processing_size) -> None:
    self.report_interval = datetime.timedelta(seconds=5)
    self.start_time = datetime.datetime.now()
    self.last_report_time = self.start_time
    self.last_report_processed_count = 0
    self.last_report_processed_size = 0
    self.averaging_inteval = datetime.timedelta(seconds=30)
    self.latest_processed_sizes = []
    self.need_processing_count = need_processing_count
    self.need_processing_size = need_processing_size

  def __call__(self, processed_size):
    now = datetime.datetime.now()
    self.latest_processed_sizes.append((now,processed_size))
    while len(self.latest_processed_sizes)>1 and now - self.latest_processed_sizes[1][0] > self.averaging_inteval:
      self.latest_processed_sizes.remove(self.latest_processed_sizes[0])
    since_last_report = now-self.last_report_time
    if since_last_report > self.report_interval:
      time_since_start = now-start_time_processing
      average_processing_rate = processed_size/time_since_start.total_seconds()
      if average_processing_rate > 0:
        estimated_time_remaining = datetime.timedelta(seconds=(self.need_processing_size - processed_size) / average_processing_rate)
      else:
        estimated_time_remaining = "?"
      percentage_complete = int(1000*processed_size/self.need_processing_size)/10.0
      #print("percent complete:", percentage_complete,"rate:",human_readable_size(average_processing_rate)+'/s',"estimated time remaining:", estimated_time_remaining)
      self.last_report_time = now
      if len(self.latest_processed_sizes) > 0 and now > self.latest_processed_sizes[0][0]:
        time_since_avg_sample = now-self.latest_processed_sizes[0][0]
        short_term_processing_rate = (processed_size-self.latest_processed_sizes[0][1])/time_since_avg_sample.total_seconds()
        if short_term_processing_rate > 0:
          short_term_estimated_time_remaining = datetime.timedelta(seconds=(self.need_processing_size - processed_size) / short_term_processing_rate)
        else:
          short_term_estimated_time_remaining = "?"
        print("percent complete:", percentage_complete,"rate:",human_readable_size(short_term_processing_rate)+'/s',"estimated time remaining:", short_term_estimated_time_remaining, ' long term rate:', human_readable_size(average_processing_rate)+'/s',' est time rem.:', estimated_time_remaining)
    return False

if __name__ == '__main__':

  verbose = False
  config_dir = pathlib.Path('~/.oeci_data_manager')
  
  i = 1

  while i < len(sys.argv) and sys.argv[i].startswith('--'):
    if sys.argv[i] == '--verbose':
      verbose = True
      i += 1
    elif sys.argv[i] == '--config-dir':
      config_dir = pathlib.Path(sys.argv[i+1])
      i += 2
    else:
      usage("Can't parse options")

  try:
    config = ConfigPath(config_dir)
  except RuntimeError:
    usage("Can't resolve config path "+str(config_dir))
  except Exception as e:
    usage(e)

  config_dir = None

  if verbose:
    print ('Configuration directory:',config.path,'exists:',config.exists())

  try:
    command = sys.argv[i]
    i += 1
  except IndexError:
    usage("Command not found")

  command_options = {}

  while i < len(sys.argv):
    if sys.argv[i].startswith('--'):
      key = sys.argv[i]
      i += 1
      if i == len(sys.argv) or sys.argv[i].startswith('--'):
        value = None
      else:
        value = sys.argv[i]
        i += 1
      command_options[key] = value
    else:
      usage("Can't parse command options")

  if verbose:
    print ('command:', command)
    for k in command_options:
      print('  ',k,command_options[k])


  if command == 'list':
    if not config.exists():
      print('No projects found, configuration directory does not exist:', config.path)
    projects = config.get_projects()
    for p in projects:
      print(p.label, '('+str(p.source)+')')
    if len(projects) == 0:
      print ('No projects found')
 
  if command == 'init':
    if not '--source' in command_options or command_options['--source'] is None:
      usage('Missing source')
    source = pathlib.Path(command_options['--source'])

    label = source.parts[-1]
    if '--label' in command_options:
      label = command_options['--label']
    if label is None:
      usage("invalid label")
    output = source
    if '--output' in command_options:
      if command_options['--output'] is None:
        usage('Missing output')
      output = pathlib.Path(command_options['--output'])

    try:
      p = config.create_project(label, source, output)
    except Exception as e:
      usage(e)
    
    if verbose:
      print('label:', p.label)
      print('source:', p.source)
      print('output:', p.output)
      print ('config file:', p.config_file)

  if command == 'scan' or command == 'process':
    if not '--project' in command_options or command_options['--project'] is None:
      usage('Missing project')

    project = config.get_project(command_options['--project'])
    if not project.valid():
      usage("Invalid project: "+command_options['--project'])

    process_count = 1
    if '--process_count' in command_options:
      try:
        process_count = int(command_options['--process_count'])
      except Exception as e:
        usage('Invalid process count: '+str(e))

    if command == 'scan':
      ps = project.structure()
      for e in ps:
        print('Expedition:',e)
        for ep in ps[e]:
          if ep == 'platforms':
            print (' ',ep)
            for p in ps[e][ep]:
              print ('   ',p)
              for ds in ps[e][ep][p]:
                print ('      ',ds)
                for sensor in ps[e][ep][p][ds]:
                  print ('         ',sensor)
          else:
            print (' ',ep,ps[e][ep])
    
    start_time_scanning = datetime.datetime.now()

    if verbose:
      print ('loading existing info...')
    project.load()
    prog = None
    if verbose:
      prog = ScanProgress()
      print ('scanning for new files...')
    project.scan_source(prog)

    handlers = [HashHandler, RosBagIndexHandler]
    prog = None
    if verbose:
      prog = ScanProgress()
      print ('scanning for files needing processing...')
    project.scan(handlers, 1, prog)

    end_time_scanning = datetime.datetime.now()

    if verbose:
      print ('scan time:', end_time_scanning-start_time_scanning)
      print ('collecting stats...')

    if verbose or command == 'scan':
      stats = project.generate_file_stats()

      for label in stats:
        if 'size' in stats[label]:
          print(label,stats[label]['count'],'('+human_readable_size(stats[label]['size'])+')')
        else:
          print(label,stats[label]['count'])

    if command == 'process':


      if verbose:
        print ('scanned',stats['total']['count'],'in',(end_time_scanning-start_time_scanning).total_seconds(),'seconds')
        print (stats['needs_processing']['count'],'files ('+human_readable_size(stats['needs_processing']['size'])+')','need processing')

      start_time_processing = datetime.datetime.now()
      last_report_time = start_time_processing

      prog = None
      if verbose:
        prog = ProcessProgress(stats['needs_processing']['count'],stats['needs_processing']['size'])

      project.process(handlers, process_count, prog)
      
      end_time_processing = datetime.datetime.now()
      if verbose:
        print('processing time:', end_time_processing - start_time_processing)
        print('total time:', end_time_processing - start_time_scanning)



  if command == 'gui':
    import odm_ui
    odm_ui.launch(config)


  exit(0)

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
      while len(results_list) >= process_count*2:
        done_list = []
        for r in results_list:
          if r.ready():
            done_list.append(r)
        if len(done_list):
          for d in done_list:
            fname, meta = d.get()
            metadata[fname] = meta
            newly_processed_count += 1
            newly_processed_size += meta['size']
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
      #print ('  processing',f.relative_to(top_level))
      results_list.append(pool.apply_async(processFile,(f,top_level)))

    for r in results_list:
      r.wait()
      f,meta = r.get()
      metadata[f]=meta
      newly_processed_count += 1
      newly_processed_size += meta['size']
  else:

    for f in need_processing_files:
      fname, meta = processFile(f, top_level)
      metadata[f] = meta
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

  saver = MetaSaver()
  for f in files:
    saver.process(f,metadata[f])


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
              deployment_nav.close()
              deployment_nav_files.append(deployment_nav_file)
    for f in deployment_nav_files:
      toKML(f)

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
  manifest_file.close()
