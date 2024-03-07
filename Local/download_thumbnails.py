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

from Models.Tabei import TabeiClient
import Functions.utilities as u
from Models.Contract import Country
import zipfile
import shutil


cfg = u.read_config()

def tabei_create_thumbnails(country, contract_name):
    t = TabeiClient()
    env_interpreter = os.path.join(cfg['tabei']['conda_env'], "bin", "python")
    cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/create_thumbnails.py") 
    command = f"{env_interpreter} {cmd_path} --country {country} --contract_name {contract_name}"
    print("sending command to create thumbnails...")
    t.execute_command(command, cfg['tabei']['stitching-services'])


def download_thumbnails(country, contract_name):
    t = TabeiClient()
    thumb_zip_remote = os.path.join(cfg['tabei']['thumbnails_folder'], country, contract_name, "thumbnails.zip")
    thumb_local = os.path.join(cfg['local']['thumbnails_folder'], country)
    thumb_zip_local = os.path.join(thumb_local, f"{contract_name}.zip")
    

    # ensure that we have the local folders
    os.makedirs(os.path.dirname(thumb_local), exist_ok=True)

    # download the zip file
    t.download_files_sftp([thumb_zip_remote], [thumb_zip_local])

    # Extract the zip file
    extract_folder = os.path.join(thumb_local, contract_name)
 
    # Check if the folder exists
    if os.path.exists(extract_folder):
        shutil.rmtree(extract_folder)  # Deletes the folder and all its contents
        print(f"Deleted existing folder {extract_folder}.")

    os.makedirs(extract_folder)  # Creates the folder
    with zipfile.ZipFile(thumb_zip_local, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
    print(f"Extracted thumbnails to {extract_folder}.")

    # Delete the zip file
    os.remove(thumb_zip_local)
    print(f"Extracted thumbnails to {extract_folder} and removed the zip file.")


if __name__ == "__main__":
    nigeria = Country("Nigeria", refresh=False)
    contract_name = nigeria.contract_names[15]
    tabei_create_thumbnails("Nigeria", contract_name)
    download_thumbnails("Nigeria", contract_name)