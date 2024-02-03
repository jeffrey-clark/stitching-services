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

import os
import yaml
import getpass
from tqdm import tqdm
from paramiko import SSHClient
import time

import Functions.utilities as u
from Models.Savio import SavioClient

# Load configuration
cfg = u.read_config()


if __name__ == "__main__":
    savio = SavioClient()
    jobs = savio.get_job_list()
    print(jobs)