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

def tabei_create_thumbnails_contract(tabei_folders, country, contract_alias):
    t = TabeiClient()
    env_interpreter = os.path.join(cfg['tabei']['conda_env'], "bin", "python")
    cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/create_thumbnails.py")
    folders_str = " ".join(tabei_folders)
    command = f"{env_interpreter} {cmd_path} --tabei_folders {folders_str } --country {country} --contract_alias {contract_alias}"
    print("sending command to create thumbnails...")
    t.execute_command(command, cfg['tabei']['stitching-services'])


def tabei_create_thumbnails_filepaths(filepaths, output_dir):
    t = TabeiClient()
    env_interpreter = os.path.join(cfg['tabei']['conda_env'], "bin", "python")
    cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/create_thumbnails_filepaths.py")
    filepaths_str = " ".join(filepaths)
    command = f"{env_interpreter} {cmd_path} --filepaths {filepaths_str} --outputdir {output_dir}"
    print("sending command to create duplicate thumbnails...")
    t.execute_command(command, cfg['tabei']['stitching-services'])




def download_thumbnails(remote_zip_path, local_zip_path):

    # raise an error of not both zip files end with ".zip"
    if not remote_zip_path.endswith(".zip") or not local_zip_path.endswith(".zip"):
        raise ValueError("Both remote and local zip paths must end with '.zip'.")
 
    t = TabeiClient()
    # ensure that we have the local folders
    os.makedirs(os.path.dirname(local_zip_path), exist_ok=True)

    # download the zip file
    t.download_files_sftp([remote_zip_path], [local_zip_path])

    # Extract the zip file
    extract_folder = local_zip_path[:-4]
 
    # Check if the folder exists
    if os.path.exists(extract_folder):
        shutil.rmtree(extract_folder)  # Deletes the folder and all its contents
        print(f"Deleted existing folder {extract_folder}.")

    os.makedirs(extract_folder)  # Creates the folder
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
    print(f"Extracted thumbnails to {extract_folder}.")

    # Delete the zip file
    os.remove(local_zip_path)
    print(f"Extracted thumbnails to {extract_folder} and removed the zip file.")





    



if __name__ == "__main__":
    nigeria = Country("Nigeria", refresh=False)
    contract_name = nigeria.contract_names[15]
    tabei_create_thumbnails("Nigeria", contract_name)
    download_thumbnails("Nigeria", contract_name)