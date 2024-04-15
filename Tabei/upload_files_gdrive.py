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


def upload_file(fps, destination_folder):

    drive = GoogleDriveService("personal_account")

    for fp in fps:
        filename = os.path.basename(fp)

        # now upload the archive the the images_folder
        drive.upload_file(filename, fp, "application/zip", destination_folder)


if __name__ == "__main__":
    
    Botswana_543A = "17Y6Xxqm8DTM1PWZd8PeONCsCkpgJgAAD"  # the folder ID to be uploaded to
    filepaths = [
        "/shares/lab/aerial_history_project/georef/archives/NCAP_DOS_543A_RAF_0525_V1.zip",
        "/shares/lab/aerial_history_project/georef/archives/NCAP_DOS_543A_RAF_0525_V2.zip",
        "/shares/lab/aerial_history_project/georef/archives/NCAP_DOS_543A_RAF_0503_V1.zip"
    ]

    # upload_file(filepaths, Botswana_543A)



