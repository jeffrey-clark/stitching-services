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
from paramiko import SSHClient
import getpass
from tqdm import tqdm
from Models.Savio import SavioClient

import Functions.utilities as u

cfg = u.read_config()

# Define a callback function for tqdm
def tqdm_callback(t):
    def inner_callback(bytes_transferred, total_bytes):
        t.update(bytes_transferred - t.n)  # Update progress bar with incremental bytes transferred
    return inner_callback

def download_results(config_filename, name=None):

    savio = SavioClient()
    savio.connect('ftp')

    # first read the config file
    cfg_file = {}
    with open(os.path.join("Files/config_files", config_filename), 'r') as f:
        cfg_file.update(yaml.safe_load(f))

    cache = cfg_file['checkpoint_cache_folder']
    cache_files = ["links_df.p", "optim_links.p", "img_df.geojson"]
    remote_cache_paths = [os.path.join(cache, f) for f in cache_files]
    
    results = cfg_file['raster_output_folder']
    results_files = savio.listdir(os.path.join(results))
    remote_results_paths = [os.path.join(results, f) for f in results_files]
    
    # Ensure the local directory exists
    contract_name = os.path.basename(results)
    local_destination = os.path.join(cfg['local']['prod'], contract_name)
    os.makedirs(local_destination, exist_ok=True)
 
    # now create the remote and local fps
    remote_paths = remote_cache_paths + remote_results_paths
    local_paths = [os.path.join(local_destination, os.path.basename(x)) for x in remote_paths]
    
    for x, y in zip(remote_paths, local_paths):
        print(x, "-->", y)
    
    savio.download_files_sftp(remote_paths, local_paths)
    
    savio.close()
    print("Download complete.")

if __name__ == "__main__":
    config_filename = "lake_chad.yml"
    download_results(config_filename)