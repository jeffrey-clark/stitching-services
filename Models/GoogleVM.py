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
import subprocess
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
import stat

import Functions.utilities as u


# Load configuration
cfg = u.read_config()

# Try to get the password from an environment variable (used on Tabei file uploads)
pwd = os.getenv('VM_DECRYPTION_PASSWORD')

if not pwd:
    pwd = getpass.getpass("  Enter Google VM decryption password: ")
    os.environ['VM_DECRYPTION_PASSWORD'] = pwd


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



# Define the SavioClient class
class VMClient:
    def __init__(self):
        self.username = cfg['google_vm']['username']
        self.instance_name = cfg['google_vm']['instance_name']
        self.zone = cfg['google_vm']['zone']
        self.ip = self.get_instance_external_ip()
        self.ssh_key_path = cfg['google_vm']['key_path']
        self.ssh = SSHClient()
        self.sftp = None
        self.connected = False
        self.host_type = None
        
    
    def get_instance_external_ip(self):
        command = [
            'gcloud', 'compute', 'instances', 'list', 
            '--filter=name={} AND zone:({})'.format(self.instance_name, self.zone), 
            '--format=get(networkInterfaces[0].accessConfigs[0].natIP)'
        ]
        try:
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            ip_address = result.stdout.strip()
            return ip_address
        except subprocess.CalledProcessError as e:
            print(f"Error: {e.stderr}")
            return None


    def connect(self, host_type="shell"):
        print("Opening Google VM connection")
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
                self.ssh.connect(hostname=self.ip, 
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
        print("closed Google VM connection")


    @ensure_connection('shell')
    def execute_command(self, command, directory=None, ignore_error=False):
        if directory:
            command = f"cd {directory} && {command}"
        stdin, stdout, stderr = self.ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        exit_status = stdout.channel.recv_exit_status()  # Get the exit status

        if exit_status != 0 and not ignore_error:
            error_message = f"Error executing command: {error}"
            print(error_message)
            raise Exception(error_message)

        return output
    
    @ensure_connection('shell')
    def _execute_command_capture_error(self, command):
        stdin, stdout, stderr = self.ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        return output, error
    
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


if __name__ == "__main__":
    x = VMClient()
    print(x.listdir("/home/jeffrey/"))

