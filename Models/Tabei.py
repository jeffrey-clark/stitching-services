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
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from paramiko.rsakey import RSAKey
import paramiko
import time
import glob
import io

import Functions.utilities as u

# Load configuration
cfg = u.read_config()
pwd = getpass.getpass("  Enter Tabei decryption password: ")


# Define the tqdm callback function
def tqdm_callback(t):
    def inner_callback(bytes_transferred, total_bytes):
        t.update(bytes_transferred - t.n)
    return inner_callback


# Decorator function to ensure connection for SavioClient methods
# as I understand it only works for shell scripts
def ensure_connection(host_type="shell", verbose=False):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            _verbose = verbose  # Use the verbose setting provided to the decorator

            was_connected = self.connected
            should_reconnect = was_connected and self.host_type != host_type

            if not was_connected or should_reconnect:
                if was_connected and _verbose:
                    print(f"Conflicting host type, closing {self.host_type}, and opening {host_type}")
                self.close()
                self.connect(host_type)
                self.connected = True
            else:
                if _verbose:
                    print("Using existing connection...")

            try:
                return func(self, *args, **kwargs)
            finally:
                if not was_connected:
                    self.close()
                    self.connected = False
        return wrapper
    return decorator

class TabeiClient:
    def __init__(self):
        self.username = cfg['tabei']['username']
        self.ssh_key_path = cfg['tabei']['key_path']
        self.ssh = SSHClient()
        self.sftp = None
        self.connected = False
        self.host_type = None

    def connect(self, host_type="shell"):
        print("Opening Tabei connection")
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if not self.connected or self.host_type != host_type:
            self._connect_ssh()
            self.connected = True
            self.host_type = host_type
            if host_type == 'ftp':
                self.sftp = self.ssh.open_sftp()

    def _connect_ssh(self):
        max_attempts = 5
        attempts = 0
        while attempts < max_attempts:
            try:
                self.ssh.connect(hostname="tabei.gspp.berkeley.edu", 
                                 username=self.username, 
                                 key_filename=self.ssh_key_path, 
                                 passphrase=pwd)
                self.connected = True
                return
            except Exception as e:
                attempts += 1
                print(f"Failed to connect (Attempt {attempts}/{max_attempts}): {e}")
                if attempts < max_attempts:
                    print("Retrying in 10 seconds...")
                    time.sleep(10)
                else:
                    print("Maximum connection attempts reached. Aborting.")
                    raise

    def close(self):
        if self.sftp and self.host_type == 'ftp':
            self.sftp.close()
            self.sftp = None

        self.ssh.close()
        self.connected = False
        self.host_type = None
        print("closed Tabei connection")



    @ensure_connection('shell')
    def execute_command(self, command):
        stdin, stdout, stderr = self.ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        if error:
            print("Error:", error)
        return output
    
    @ensure_connection('ftp')
    def listdir(self, path):
        return [x for x in self.sftp.listdir(path) if not x.startswith('.')]

    @ensure_connection('ftp')
    def upload_files_sftp(self, file_paths, remote_paths):
        try:
            for local_path, remote_path in zip(file_paths, remote_paths):
                print(f"Uploading {local_path} to {remote_path}...")
                file_size = os.path.getsize(local_path)
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=os.path.basename(local_path)) as t:
                    self.sftp.put(local_path, remote_path, callback=tqdm_callback(t))
            print("Upload complete.")
        except Exception as e:
            print(f"An error occurred during file upload: {e}")
            raise e

    @ensure_connection('ftp')
    def download_files_sftp(self, remote_paths, local_paths):
        try:
            for remote_path, local_path in zip(remote_paths, local_paths):
                # Ensure the local directory exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                # Perform the file download
                file_size = self.sftp.stat(remote_path).st_size
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=os.path.basename(remote_path)) as t:
                    self.sftp.get(remote_path, local_path, callback=tqdm_callback(t))
            print("Download complete.")
        except Exception as e:
            print(f"An error occurred during file download: {e}")
            raise e
        
    @ensure_connection('ftp')
    def get_file_sizes_in_folders(self, folder_paths):
        """
        Returns the file sizes of all files within the specified folders.

        :param folder_paths: List of paths to the folders
        :return: Dictionary where keys are folder paths and values are lists of tuples (file name, file size)
        """
        folder_sizes = {}
        for folder_path in folder_paths:
            try:
                files = self.sftp.listdir(folder_path)
                folder_sizes[folder_path] = [(file, self.sftp.stat(os.path.join(folder_path, file)).st_size) for file in files if not file.startswith('.')]
            except Exception as e:
                print(f"An error occurred while accessing {folder_path}: {e}")
        return folder_sizes


    @ensure_connection('shell', False)
    def get_folder_total_sizes(self, folder_paths):
        """
        Returns the total file sizes for each specified folder by executing a command on the server.

        :param folder_paths: List of paths to the folders
        :return: Dictionary where keys are folder paths and values are total sizes of the folders in bytes
        """
        folder_total_sizes = {}
        with tqdm(total=len(folder_paths), desc="Getting folder sizes", file=sys.stdout) as pbar:
            for folder_path in folder_paths:
                try:
                    command = f"du -sb {folder_path} | cut -f1"
                    output = self.execute_command(command)
                    size = int(output.strip())  # Convert the output to an integer
                    folder_total_sizes[folder_path] = size
                except Exception as e:
                    print(f"An error occurred while accessing {folder_path}: {e}")
                finally:
                    time.sleep(3)  # Include the sleep as per your requirement
                    pbar.update(1)  # Update progress for each processed folder path
        return folder_total_sizes
    
        
    @ensure_connection('shell')
    def send_tmux_command(self, session_name, command):
        # Using bash -c to execute the tmux command
        full_command = f"bash -c \"tmux new-session -d -s {session_name} '{command}'\""
        output = self.execute_command(full_command)
        if output:
            print(f"Output from tmux command: {output}")
        else:
            print(f"Started tmux session '{session_name}' with command: {command}")

    @ensure_connection('shell')
    def list_tmux_sessions(self):
        output = self.execute_command("tmux list-sessions")
        
        # if there are no tmux sessions running it will return a string starting with error
        if "error" in output.lower()[:10]:
            return None
        
        return output

    


    

if __name__ == "__main__":
    t = TabeiClient()
    t.connect('ftp')
    x = t.listdir("/home/jeffrey.clark/")
    print(x)
    filesizes = t.get_folder_total_sizes(["/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_101_NG_0007/", "/global/scratch/users/jeffreyclark/Images/Nigeria/NCAP_DOS_SHELL_BP_0049"])
    print(filesizes)
    t.close()