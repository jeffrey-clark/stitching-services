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
import glob

import Functions.utilities as u

# Load configuration
cfg = u.read_config()

# Try to get the password from an environment variable (used on Tabei file uploads)
pwd = os.getenv('SAVIO_DECRYPTION_PASSWORD')

if not pwd:
    pwd = getpass.getpass("  Enter Savio decryption password: ")


# Define the tqdm callback function
def tqdm_callback(t):
    def inner_callback(bytes_transferred, total_bytes):
        t.update(bytes_transferred - t.n)
    return inner_callback


# Decorator function to ensure connection for SavioClient methods
# as I understand it only works for shell scripts
def ensure_connection(host_type="shell"):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            was_connected = self.connected
            should_reconnect = was_connected and self.host_type != host_type

            if not was_connected or should_reconnect:
                if was_connected:
                    print(f"Conflicting host type, closing {self.host_type}, and opening {host_type}")
                    self.close()  # Close existing connection if the types don't match
                self.connect(host_type)
                self.connected = True
            else:
                print("Using existing connection...")

            try:
                return func(self, *args, **kwargs)
            finally:
                if not was_connected:
                    self.close()
                    self.connected = False
        return wrapper
    return decorator


# Define the SavioClient class
class SavioClient:
    def __init__(self):
        self.username = cfg['savio']['username']
        self.ssh = SSHClient()
        self.sftp = None
        self.connected = False
        self.host_type = None

    def connect(self, host_type="shell"):

        if host_type == 'shell':
            hostname = 'hpc.brc.berkeley.edu'
        elif host_type == 'ftp':
            hostname = 'dtn.brc.berkeley.edu'
        else:
            raise ValueError("Invalid host_type. Expected 'shell' or 'ftp'.")
        print(f"opening Savio connection: {host_type}")
        self.ssh.load_system_host_keys()
        if not self.connected or self.host_type != host_type:
            self._connect_ssh(hostname)
            self.connected = True
            self.host_type = host_type

            if host_type == 'ftp':
                self.sftp = self.ssh.open_sftp()


    def _connect_ssh(self, hostname):
        max_attempts = 5
        attempts = 0

        while attempts < max_attempts:
            try:
                self.ssh.connect(hostname=hostname, username=self.username, password=u.get_savio_password(pwd))
                self.connected = True
                return  # Successfully connected, exit the method
            except Exception as e:
                attempts += 1
                print(f"Failed to connect (Attempt {attempts}/{max_attempts}): {e}")
                if attempts < max_attempts:
                    print("Retrying in 10 seconds...")
                    time.sleep(10)
                else:
                    print("Maximum connection attempts reached. Aborting.")
                    raise  # Or handle the exception as needed


    def close(self):
        if self.sftp and self.host_type == 'ftp':
            self.sftp.close()
            self.sftp = None

        self.ssh.close()
        self.connected = False
        self.host_type = None
        print("closed Savio connection")



    @ensure_connection('shell')
    def execute_command(self, command):
        stdin, stdout, stderr = self.ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        if error:
            print("Error:", error)
        return output
    
    @ensure_connection('shell')
    def get_job_list(self):
        sq = self.execute_command("sq")
        return sq
    
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


    def upload_image_folders(self, source_pattern, country):
        directories = glob.glob(source_pattern)
        for directory in directories:
            if os.path.isdir(directory):
                folder_name = os.path.basename(directory.rstrip('/'))
                full_dest_path = os.path.join(cfg['savio']['images_folder'], country, folder_name)
                
                file_paths = [os.path.join(directory, file) for file in os.listdir(directory)]
                remote_paths = [os.path.join(full_dest_path, file) for file in os.listdir(directory)]

                self.upload_files_sftp(file_paths, remote_paths)
            else:
                print(f"Skipping {directory}, not a directory.")


    

if __name__ == "__main__":
    s = SavioClient()
    x = s.listdir("/global/home/users/jeffreyclark")
    print(x)