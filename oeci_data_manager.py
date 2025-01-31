#!/usr/bin/env python3
import argparse
import pathlib
import datetime
import json

from hash_handler import HashHandler
from ros_bag_handler import RosBagHandler
from ros_bag_index_handler import RosBagIndexHandler
from drix_deployments import DrixDeployments

from config import ConfigPath
from project import Project

from odm_utils import human_readable_size

# Class to track and report the progress of file scanning
class SourceScanProgress:
    def __init__(self):
        self.report_interval = datetime.timedelta(seconds=5)  # Time interval for progress reporting
        self.last_report_time = datetime.datetime.now()  # Timestamp of the last report

    def __call__(self, file_count):
        now = datetime.datetime.now()
        # Check if enough time has passed to print a progress update
        if now - self.last_report_time >= self.report_interval:
            print(file_count, 'files scanned')
            self.last_report_time = now
        return False

# Class to track and report the progress of file scanning with total count
class ScanProgress:
    def __init__(self, need_processing_count):
        self.report_interval = datetime.timedelta(seconds=5)
        self.last_report_time = datetime.datetime.now()
        self.need_processing_count = need_processing_count  # Total number of files needing processing

    def __call__(self, file_count):
        now = datetime.datetime.now()
        # Print progress update if enough time has passed
        if now - self.last_report_time >= self.report_interval:
            print(file_count, 'files scanned of', self.need_processing_count)
            self.last_report_time = now
        return False

# Class to track and report the progress of file processing
class ProcessProgress:
    def __init__(self, need_processing_size):
        self.report_interval = datetime.timedelta(seconds=5)
        self.start_time = datetime.datetime.now()  # Timestamp of when processing started
        self.last_report_time = self.start_time
        self.latest_processed_sizes = []  # List to track processed sizes for rate calculation
        self.need_processing_size = need_processing_size  # Total size of data needing processing

    def __call__(self, processed_size):
        now = datetime.datetime.now()
        self.latest_processed_sizes.append((now, processed_size))
        # Remove outdated entries from the size tracking list
        while len(self.latest_processed_sizes) > 1 and now - self.latest_processed_sizes[1][0] > datetime.timedelta(seconds=30):
            self.latest_processed_sizes.pop(0)
        # Print progress update if enough time has passed
        if now - self.last_report_time > self.report_interval:
            time_since_start = now - self.start_time
            avg_rate = processed_size / time_since_start.total_seconds() if time_since_start.total_seconds() > 0 else 0
            est_time_remaining = datetime.timedelta(seconds=(self.need_processing_size - processed_size) / avg_rate) if avg_rate > 0 else "?"
            percent_complete = (processed_size / self.need_processing_size) * 100
            print(f"Progress: {percent_complete:.1f}% | Avg Rate: {human_readable_size(avg_rate)}/s | Remaining: {est_time_remaining}")
            self.last_report_time = now
        return False

# Function to parse command-line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="OECI Data Manager")
    subparsers = parser.add_subparsers(dest='command', title='commands', required=True, help='Command to execute')

    # Common parent parser for shared arguments
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--config-dir", default=str(pathlib.Path("~/.oeci_data_manager")), help="Path to config directory")
    parent_parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    # List command (no additional arguments)
    subparsers.add_parser("list", parents=[parent_parser], help="List available projects")

    # Init command
    init_parser = subparsers.add_parser("init", parents=[parent_parser], help="Initialize a new project")
    init_parser.add_argument("--source", required=True, help="Source directory")
    init_parser.add_argument("--label", help="Project label")
    init_parser.add_argument("--output", help="Output directory")
    # Scan command
    scan_parser = subparsers.add_parser("scan", parents=[parent_parser], help="Scan for files needing processing")
    scan_parser.add_argument("--project", required=True, help="Project to scan")
    scan_parser.add_argument("--process_count", type=int, default=1, help="Number of jobs for processing")
    # Process command
    process_parser = subparsers.add_parser("process", parents=[parent_parser], help="Process files")
    process_parser.add_argument("--project", required=True, help="Project to process")
    process_parser.add_argument("--process_count", type=int, default=1, help="Number of jobs for processing")
    # GUI command (no additional arguments)
    subparsers.add_parser("gui", parents=[parent_parser], help="Launch graphical interface")

    return parser.parse_args()       

# Main function handling the core logic
def main():
    args = parse_args()

    print(args)

    config_dir = pathlib.Path(args.config_dir).expanduser()
    verbose = args.verbose

    try:
        # Load configuration from the specified directory
        config = ConfigPath(config_dir)
    except Exception as e:
        print(f"Error loading config: {e}")
        exit(1)

    if verbose:
        print(f"Config Directory: {config.path} (Exists: {config.exists()})")

    
    command = args.command

    if command == "list":
        # Handle "list" command to show available projects
        if not config.exists():
            print("No projects found. Configuration directory does not exist.")
        else:
            projects = config.get_projects()
            for p in projects:
                print(p.label, f"({p.source})")
            if not projects:
                print("No projects found.")

    elif command == "init":
        # Handle "init" command to create a new project
        source = pathlib.Path(args.source)
        label = args.label or source.parts[-1]  # Use the last part of the source path as default label
        output = pathlib.Path(args.output) if args.output else source

        try:
            project = config.create_project(label, source, output)
            if verbose:
                print(f"Project created: {project.label} (Source: {project.source}, Output: {project.output})")
        except Exception as e:
            print(f"Error initializing project: {e}")

    elif command in ["scan", "process"]:
        # Handle "scan" and "process" commands
        project = config.get_project(args.project)
        if not project.valid():
            print(f"Invalid project: {args.project}")
            exit(1)

        process_count = args.process_count

        if command == "scan":
            project.load()
            if verbose:
                print("Scanning source...")
            # Scan source files and print progress if verbose
            project.scan_source(SourceScanProgress() if verbose else None)

            if verbose:
                print("Scanning for files needing processing...")
            project.scan([HashHandler, RosBagIndexHandler, RosBagHandler], 1, ScanProgress(len(project.files)) if verbose else None)

            if verbose:
                # Generate and display statistics about the scanned files
                stats = project.generate_file_stats()
                for label, stat in stats.items():
                    size = stat.get("size", 0)
                    print(f"{label}: {stat['count']} files ({human_readable_size(size)})")

        elif command == "process":
            # Process files and generate deployment manifest
            stats = project.generate_file_stats()
            if verbose:
                print(f"Files to process: {stats['needs_processing']['count']} ({human_readable_size(stats['needs_processing']['size'])})")

            project.process([HashHandler, RosBagIndexHandler, RosBagHandler], process_count, ProcessProgress(stats['needs_processing']['size']) if verbose else None)
            try:
                project.generate_manifest()
            except Exception as e:
                print(f"Error generating manifest: {e}")
            dgen = DrixDeployments(project)
            dgen.generate()

    elif command == "gui":
        # Launch the GUI if "gui" command is issued
        import odm_ui
        odm_ui.launch(config)

if __name__ == "__main__":
    main()
