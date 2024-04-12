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
from Models.GoogleDrive import GoogleDriveService
import Functions.utilities as u

cfg = u.read_config()


def upload_archive(contract_name, country):
    # compile the local filepath
    zip_fp = os.path.join(cfg['tabei']['results_folder'], contract_name, f"raws_to_georef_{contract_name}.zip")
    
    # now let us create the necessary folders in Google Drive
    drive = GoogleDriveService("personal_account")
    uploading_folder_id = "1YyTi9O-HTr4cUHPBrxr2WtfaxrHic3z6"

    # create contract folder if it does not exist
    contract_folder_id = drive.create_folder(contract_name, uploading_folder_id)
    images_folder_id = drive.create_folder("Images", contract_folder_id)
    sortie_plot_id = drive.create_folder("Sortie Plots", contract_folder_id)

    # now upload the archive the the images_folder
    drive.upload_file(f"raws_to_georef_{contract_name}.zip", zip_fp, "application/zip", images_folder_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload archive ZIP file to iMerit for georeferencing")
    parser.add_argument('--contract', type=str, help='Contract name', required=True)
    parser.add_argument('--country', type=str, help='Country name', required=True)

    args = parser.parse_args()

    upload_archive(args.contract, args.country)

