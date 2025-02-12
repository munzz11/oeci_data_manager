#!/usr/bin/env python3

import argparse
from foxglove_data_platform.client import Client
from pathlib import Path
from tqdm import tqdm  # For progress tracking

def upload_bag_files(token, device_id, root_dir, verbose=False):
    root_path = Path(root_dir)
    if not root_path.exists() or not root_path.is_dir():
        print(f"Error: Directory '{root_dir}' does not exist or is not a valid directory.")
        return

    client = Client(token=token)
    bag_files = list(root_path.rglob("*.bag"))  # Get all .bag files

    if not bag_files:
        print("No .bag files found in the specified directory.")
        return

    print(f"Found {len(bag_files)} .bag files. Starting upload...")

    for bag_file in tqdm(bag_files, desc="Uploading files", unit="file"):
        if verbose:
            print(f"Uploading: {bag_file}")

        with bag_file.open("rb") as byte_stream:
            def progress_callback(size, progress):
                percent = (progress / size) * 100 if size > 0 else 0
                tqdm.write(f"{bag_file} -> {progress}/{size} bytes ({percent:.2f}%) uploaded")

            client.upload_data(
                device_id=device_id,
                filename=str(bag_file.relative_to(root_path)),  # Preserve folder structure
                data=byte_stream,
                callback=progress_callback,
            )

    print("Upload complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload ROS .bag files to Foxglove.")
    parser.add_argument("token", help="Foxglove API token")
    parser.add_argument("device_id", help="Device ID for upload")
    parser.add_argument("directory", help="Root directory to search for .bag files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")

    args = parser.parse_args()
    upload_bag_files(args.token, args.device_id, args.directory, args.verbose)
