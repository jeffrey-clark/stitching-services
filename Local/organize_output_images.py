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
from Models.GoogleBucket import GoogleBucket

import Functions.utilities as u

cfg = u.read_config()

def organize_files(contract_alias, machine):

    if machine.lower() == "savio":
        # skip savio for now
        pass
    elif machine.lower() == "google_vm":
        b = GoogleBucket()
        bucket_home = cfg['bucket']['root']  # gs://jeffrey_stitching_bucket

        # first list the dir of the results folder
        results = os.path.join(bucket_home, "results", contract_alias)
        swaths = os.path.join(results, "swaths")
        swaths_geojson = os.path.join(swaths, "geojson")

        results_files = [x for x in b.listdir_bucket_detailed(results) if "swath" in x['path']]

        try:
            swath_files = {os.path.basename(x['path']): x for x in b.listdir_bucket_detailed(swaths)}
        except:
            swath_files = {}

        for result_file in results_files:
            result_filename = os.path.basename(result_file['path'])
            if "geojson" in result_filename:
                dest = swaths_geojson
            else:
                dest = swaths
            if result_filename in swath_files:
                # Compare file sizes
                if result_file['size'] != swath_files[result_filename]['size']:
                    # Sizes are different, replace the file in swaths
                    print(f"Replacing {result_filename} in swaths...")
                    b.move_file_in_bucket(result_file['path'], dest)
            else:
                # File does not exist in swaths, move it there
                print(f"Moving {result_filename} to swaths...")
                b.move_file_in_bucket(result_file['path'], dest)

        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move images to folder in results folder.")
    parser.add_argument('--contract_alias', type=str, help='Contract alias', required=True)
    parser.add_argument('--machine', type=str, help='Machine', required=True)
    
    args = parser.parse_args()
    
    organize_files(args.contract_alias, args.machine)
