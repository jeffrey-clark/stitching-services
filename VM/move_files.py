# --- SET PROJECT ROOT 

import os
import sys

# Find the project root directory dynamically
root_dir = os.path.abspath(__file__)  # Start from the current file's directory

# Traverse upwards until the .project_root file is found or until reaching the system root
while not os.path.exists(os.path.join(root_dir, '.project_root')) and root_dir != '/':
    root_dir = os.path.dirname(root_dir)

# Make sure the .project_root file is found
assert root_dir != '/', "The .project_root file was not found. Make sure it exists in your project root."

sys.path.append(root_dir)

# ---

import argparse
import shutil
import Functions.utilities as u
import re

cfg = u.read_config()


def move_files_by_pattern(source_dir, dest_dir, pattern):
    """
    Move files from source_dir to dest_dir based on a regex pattern.
    
    Parameters:
    - source_dir: The directory to search for files.
    - dest_dir: The destination directory where matching files will be moved.
    - pattern: The regex pattern to match filenames.
    """
    
    # Ensure the destination directory exists
    os.makedirs(dest_dir, exist_ok=True)
    
    # Compile the regex pattern for filename matching
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    
    # List all files in the source directory
    for filename in os.listdir(source_dir):
        if compiled_pattern.match(filename):
            # Construct full paths
            source_path = os.path.join(source_dir, filename)
            dest_path = os.path.join(dest_dir, filename)
            
            # Move file
            print(f"Moving {filename} to {dest_dir}...")
            shutil.move(source_path, dest_path)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move images to folder in results folder.")
    parser.add_argument('--source', type=str, help='Source Directory', required=True)
    parser.add_argument('--destination', type=str, help='Destination Directory', required=True)
    parser.add_argument('--pattern', type=str, help='Regex Pattern', required=True)
    
    args = parser.parse_args()
    

    move_files_by_pattern(args.source, args.destination, args.pattern)



    ## What to run in the command line?
    #
    #   bash VM/run.sh <filepath> <container_name> <source folder> <destination folder> <pattern>
    #
    #  Example:
    #   bash VM/run.sh VM/move_files_new.py move_files --source "/app/bucket/results/NCAP_DOS_CAS" --destination "/app/bucket/results/NCAP_DOS_CAS/clusters" --pattern "cluster.*\.tif$"
    #