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

import Functions.utilities as u

cfg = u.read_config()
pwd = getpass.getpass("  Enter decryption password: ")


# Define a callback function for tqdm
def tqdm_callback(t):
    def inner_callback(bytes_transferred, total_bytes):
        t.update(bytes_transferred - t.n)
    return inner_callback


def upload_to_savio_pattern(source_pattern, country):
    directories = glob.glob(source_pattern)
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(hostname='dtn.brc.berkeley.edu', username='jeffreyclark', password=u.get_savio_password(pwd))

    with ssh.open_sftp() as sftp:
        for directory in directories:
            if os.path.isdir(directory):
                folder_name = os.path.basename(directory.rstrip('/'))
                full_dest_path = os.path.join(cfg['images_folder_paths']['savio'], country, folder_name)
                print(f"Uploading {directory} to {full_dest_path}...")
                
                for file in os.listdir(directory):
                    local_path = os.path.join(directory, file)
                    remote_path = os.path.join(full_dest_path, file)
                    file_size = os.path.getsize(local_path)

                    with tqdm(total=file_size, unit='B', unit_scale=True, desc=file) as t:
                        sftp.put(local_path, remote_path, callback=tqdm_callback(t))
            else:
                print(f"Skipping {directory}, not a directory.")

    print("Upload complete.")


def upload_to_savio(directories, country):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(hostname='dtn.brc.berkeley.edu', username='jeffreyclark', password=u.get_savio_password(pwd))

    with ssh.open_sftp() as sftp:
        for directory in directories:
            if os.path.isdir(directory):
                folder_name = os.path.basename(directory.rstrip('/'))
                full_dest_path = os.path.join(cfg['images_folder_paths']['savio'], country, folder_name)
                print(f"Uploading {directory} to {full_dest_path}...")
                
                for file in os.listdir(directory):
                    local_path = os.path.join(directory, file)
                    remote_path = os.path.join(full_dest_path, file)
                    file_size = os.path.getsize(local_path)

                    with tqdm(total=file_size, unit='B', unit_scale=True, desc=file) as t:
                        sftp.put(local_path, remote_path, callback=tqdm_callback(t))
            else:
                print(f"Skipping {directory}, not a directory.")

    print("Upload complete.")



def upload_to_bucket(source_pattern, country):
    directories = glob.glob(source_pattern)

    for directory in directories:
        if os.path.isdir(directory):
            folder_name = os.path.basename(directory.rstrip('/'))
            full_dest_path = os.path.join(cfg['images_folder_paths']['google_bucket'], country, folder_name)

            print(f"Uploading {directory} to {full_dest_path}...")
            subprocess.run(["gcloud", "storage", "cp", directory, full_dest_path, "--recursive"])
        else:
            print(f"Skipping {directory}, not a directory.")

    print("Upload complete.")




def main(paths, country, machine):
    if machine.lower() == "savio":
        upload_to_savio(paths, country)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload directories to Savio")
    parser.add_argument('--paths', nargs='+', help='List of folder paths to upload', required=True)
    parser.add_argument('--country', type=str, help='Country name for the destination path', required=True)
    parser.add_argument('--machine', type=str, help='Savio or Google VM', required=True)

    args = parser.parse_args()

    main(args.paths, args.country, args.machine)



# # PARAMETERS
# country = "Nigeria"
# for i in [43, 44]:
#     source_pattern = f"/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_00{i}/"
#     upload_to_savio(source_pattern, country)
