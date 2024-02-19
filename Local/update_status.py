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
from Models.GoogleDrive import StatusSheet, Status

import Functions.utilities as u

cfg = u.read_config()


status_db = StatusSheet(cfg['google_drive']['config_files']['id'], "status")


def update_status(contract_alias, machine, column, value):

    contract_status = Status(status_db, contract_alias, machine, cfg['savio']['username'])
    contract_status.update_status(column, value)

        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update status in Google Sheet")
    parser.add_argument('--contract_alias', type=str, help='Contract alias of row to be updated', required=True)
    parser.add_argument('--machine', type=str, help='Machine of row to be updated', required=True)
    parser.add_argument('--column', type=str, help='Column to be updated', required=True)
    parser.add_argument('--value', type=str, help='Value to be set', required=True)

    args = parser.parse_args()

    update_status(args.contract_alias, args.machine, args.column, args.value)

    