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

import glob
import argparse
from paramiko import SSHClient
from tqdm import tqdm
from paramiko import SSHClient
import getpass
import subprocess
from Models.Savio import SavioClient

import Functions.utilities as u

cfg = u.read_config()



def upload_to_bucket(directories, country):
    bucket_path = cfg['google_vm']['gsutil_paths']['bucket'].rstrip('/')
    bucket_images = cfg['google_vm']['gsutil_paths']['images_folder'].strip('/')
    
    for directory in directories:
        if os.path.isdir(directory):
            folder_name = os.path.basename(directory.rstrip('/'))
            full_dest_path = os.path.join(bucket_images, country, folder_name)
            print(f"Uploading {directory} to {full_dest_path}...")
            subprocess.run(["gcloud", "storage", "cp", directory, full_dest_path, "--recursive"])
        else:
            print(f"Skipping {directory}, not a directory.")

    print("Upload complete.")



def main(paths, country, destination):
    if destination.lower() == "savio":
        s = SavioClient()
        s.upload_image_folders(paths, country)

    elif destination.lower() == "bucket":
        upload_to_bucket(paths, country)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload directories to Savio")
    parser.add_argument('--paths', nargs='+', help='List of folder paths to upload', required=True)
    parser.add_argument('--country', type=str, help='Country name for the destination path', required=True)
    parser.add_argument('--destination', type=str, help='Savio or Bucket', required=True)

    args = parser.parse_args()

    main(args.paths, args.country, args.destination)





# # PARAMETERS
# country = "Nigeria"
# for i in [43, 44]:
#     source_pattern = f"/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_00{i}/"
#     upload_to_savio(source_pattern, country)