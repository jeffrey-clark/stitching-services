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
    
    def convert_to_bucket_paths(self, directories, country):
        """
        Converts Tabei directory paths to Savio directory paths.

        :param directories: List of Tabei directory paths
        :param country: Country name for the Savio directory structure
        :return: List of tuples (local_directory, savio_directory)
        """

        directory_mappings = []
        for directory in directories:
            folder_name = os.path.basename(directory.rstrip('/'))
            bucket_path = os.path.join(cfg['google_vm']['gsutil_paths']['images_folder'], country, folder_name)
            directory_mappings.append((directory, bucket_path))
        return directory_mappings
    

    def get_bucket_folder_sizes(self, bucket_path):
        result = subprocess.run(["gsutil", "du", "-s", bucket_path], capture_output=True, text=True)
        output = result.stdout

        # Process the output to get sizes
        sizes = {}
        for line in output.splitlines():
            size, path = line.split()
            sizes[path] = int(size)
        return sizes


    def get_bucket_folders_total_sizes(self, bucket_paths):
        folder_total_sizes = {}
        with tqdm(total=len(bucket_paths), desc="Getting folder sizes", file=sys.stdout) as pbar:
            for bucket_path in bucket_paths:
                try:
                    # Ensure the path ends with '/*' to include all files in the folder
                    full_path = f"{bucket_path.rstrip('/')}/*"
                    result = subprocess.run(["gsutil", "du", "-s", full_path], capture_output=True, text=True)
                    output = result.stdout.strip()

                    if output:  # Check if there's any output
                        size, _ = output.split()
                        folder_total_sizes[bucket_path] = int(size)
                    else:
                        print(f"No size information available for {bucket_path}")
                except Exception as e:
                    print(f"An error occurred while accessing {bucket_path}: {e}")
                finally:
                    pbar.update(1)  # Update progress
        return folder_total_sizes
    

    def download_from_bucket(self, bucket_paths, local_dest, max_retries=3, wait_seconds=5):
        # Ensure that bucket_paths is a list
        if not isinstance(bucket_paths, list):
            raise ValueError("bucket_paths must be a list")

        # Ensure that local_dest is a string
        if not isinstance(local_dest, str):
            raise ValueError("local_dest must be a string")

        # Check if local destination exists
        if not os.path.exists(local_dest):
            raise FileNotFoundError(f"The local destination {local_dest} does not exist.")

        for gs_path in bucket_paths:
            if gs_path.startswith('gs://'):
                gs_basename = os.path.basename(gs_path.rstrip('/'))
                full_local_path = os.path.join(local_dest, gs_basename)

                print(f"Downloading {gs_path} to {full_local_path}...")

                for attempt in range(max_retries):
                    try:
                        subprocess.run(["gcloud", "storage", "cp", gs_path, full_local_path, "--recursive"], check=True)
                        print(f"Downloaded {gs_path} successfully.")
                        break  # Break out of the retry loop if successful
                    except subprocess.CalledProcessError as e:
                        print(f"Failed to download {gs_path}: {e}.")
                        if attempt < max_retries - 1:
                            print(f"Retrying in {wait_seconds} seconds...")
                            time.sleep(wait_seconds)
                        else:
                            print(f"Exceeded maximum retries for {gs_path}.")
            else:
                print(f"Skipping {gs_path}, not a valid Google Cloud Storage path.")

        print("Download complete.")


    def listdir_bucket(self, gs_dir):
    # Ensure gs_dir is a valid Google Cloud Storage directory path
        if not gs_dir.startswith('gs://'):
            raise ValueError("Invalid Google Cloud Storage path. Must start with 'gs://'.")

        # Ensure the path ends with a slash to list contents of the directory
        if not gs_dir.endswith('/'):
            gs_dir += '/'

        try:
            # Run the gsutil ls command
            result = subprocess.run(["gsutil", "ls", gs_dir], capture_output=True, text=True, check=True)
            # Split the output into lines and filter out empty lines
            files = [line for line in result.stdout.split('\n') if line]
            files.remove(gs_dir)
            return files
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"An error occurred while listing the directory: {e}")


    def delete_from_bucket(self, gs_paths):
        """
        Deletes folders or files from Google Cloud Storage.

        :param gs_paths: List of Google Cloud Storage paths (gs://bucket-name/path/to/folder)
        """
        if not isinstance(gs_paths, list):
            raise ValueError("gs_paths must be a list")

        for gs_path in gs_paths:
            if gs_path.startswith('gs://'):
                print(f"Deleting {gs_path}...")

                try:
                    # Add '-r' for recursive deletion and '-f' to ignore non-existent objects
                    subprocess.run(["gsutil", "-m", "rm", "-r", "-f", gs_path], check=True)
                    print(f"Deleted {gs_path} successfully.")
                except subprocess.CalledProcessError as e:
                    print(f"Failed to delete {gs_path}: {e}.")
            else:
                print(f"Skipping {gs_path}, not a valid Google Cloud Storage path.")

        print("Deletion complete.")


    @ensure_connection('shell')
    def send_tmux_command(self, session_name, command):
        # Using bash -c to execute the tmux command
        full_command = f"bash -c \"tmux new-session -d -s {session_name} '{command}'\""
        print("full command is")
        print(full_command)
        output = self.execute_command(full_command)
        if output:
            print(f"Output from tmux command: {output}")
        else:
            print(f"Started tmux session '{session_name}' with command: {command}")


    @ensure_connection('shell')
    def list_tmux_sessions(self):
        output, error = self._execute_command_capture_error("tmux list-sessions")

        # Handle the case where no tmux sessions are running
        if error and "no server running" in error.lower():
            # If the error is specifically about no tmux sessions, return None without printing the error
            return None
        elif error:
            # If there's some other error, print it
            print("Error:", error)

        return output


    def check_tmux_session(self, tmux_name):
        # first check if there is a tmux session going
        tmux_sessions_response = self.list_tmux_sessions()
        if tmux_sessions_response is not None:
            if tmux_name in tmux_sessions_response:
                print(f"\n{tmux_name} is still ongoing. Please wait until complete.\n")
                sys.exit(1)  # Exit the program with a status code of 1 to indicate an error



if __name__ == "__main__":
    x = VMClient()
    print(x.listdir("/home/jeffrey/"))

