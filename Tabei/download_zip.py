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
from Models.GoogleVM import VMClient


import Functions.utilities as u

cfg = u.read_config()




def main(contract_alias, origin):

    tabei_zip = cfg['tabei']['georef_archive_folder']
    tabei_dest = os.path.join(tabei_zip, contract_alias)
    os.makedirs(tabei_dest, exist_ok=True)

    if origin.lower() == "savio":
        s = SavioClient()

        results_folder = cfg['savio']['results_folder']
        contract_results = os.path.join(results_folder, contract_alias)
        zip_files = [x for x in s.listdir(contract_alias) if x.endswith(".zip")]
        zip_fps = [os.path.join(contract_results, x) for x in zip_files]

        tabei_fps = [os.path.join(tabei_dest, x) for x in zip_files]

        s.download_files_sftp(zip_fps, tabei_fps)

    elif origin.lower() == "bucket":
        vm = VMClient()
        bucket_path = cfg['google_vm']['gsutil_paths']['bucket'].rstrip('/')
        contract_results = os.path.join(bucket_path, "results", contract_alias)
        zip_fps = [x for x in vm.listdir_bucket(contract_results) if x.endswith(".zip")]

        vm.download_from_bucket(zip_fps, tabei_dest)

    print("Download complete.")
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload directories to Savio")
    parser.add_argument('--contract_alias', type=str, help='Contract Alias', required=True)
    parser.add_argument('--origin', type=str, help='Savio or Bucket', required=True)

    args = parser.parse_args()
    main(args.contract_alias, args.origin)

    # main("NCAP_DOS_126_NG", "Nigeria", "bucket")



