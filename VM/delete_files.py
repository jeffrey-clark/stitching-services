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
import re
import Functions.utilities as u

cfg = u.read_config()

def delete_files_by_pattern(source_dir, pattern):
    """
    Delete files from source_dir based on a regex pattern.
    
    Parameters:
    - source_dir: The directory to search for files.
    - pattern: The regex pattern to match filenames.
    """
    
    # Compile the regex pattern for filename matching
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    
    # List all files in the source directory
    for filename in os.listdir(source_dir):
        if compiled_pattern.match(filename):
            # Construct full path
            file_path = os.path.join(source_dir, filename)
            
            # Delete file
            os.remove(file_path)
            print(f"Deleted {filename} from {source_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete files matching a regex pattern in a specified directory.")
    parser.add_argument('--source', type=str, help='Source Directory', required=True)
    parser.add_argument('--pattern', type=str, help='Regex Pattern', required=True)
    
    args = parser.parse_args()
    
    delete_files_by_pattern(args.source, args.pattern)