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
    u.get_savio_password(pwd)
    os.environ['SAVIO_DECRYPTION_PASSWORD'] = pwd


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
    def execute_command(self, command, directory=None):
        if directory:
            command = f"cd {directory} && {command}"
        stdin, stdout, stderr = self.ssh.exec_command(command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        if error:
            print("Error:", error)
            raise error
        return output
    
    @ensure_connection('shell')
    def get_job_list(self):
        sq = self.execute_command("sq")
        return sq
    
    @ensure_connection('ftp')
    def listdir(self, path):
        return [x for x in self.sftp.listdir(path) if not x.startswith('.')]
    
    @ensure_connection('ftp')
    def makedirs(self, path):
        """Ensure that a directory exists on the remote server."""
        # Split the path and filter out empty parts to handle absolute paths
        dir_parts = [part for part in path.split('/') if part]
        current_dir = '/'

        for part in dir_parts:
            current_dir = os.path.join(current_dir, part)
            try:
                self.sftp.stat(current_dir)
            except IOError:
                print(f"Creating remote directory: {current_dir}")
                self.sftp.mkdir(current_dir)

                
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
         # Check if remote_paths and local_paths are lists
        if not isinstance(remote_paths, list) or not isinstance(local_paths, list):
            raise TypeError("remote_paths and local_paths must be lists.")

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


    def upload_image_folders_pattern(self, source_pattern, country):
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


    def convert_to_savio_paths(self, directories, country):
        """
        Converts Tabei directory paths to Savio directory paths.

        :param directories: List of Tabei directory paths
        :param country: Country name for the Savio directory structure
        :return: List of tuples (local_directory, savio_directory)
        """
        directory_mappings = []
        for directory in directories:
            folder_name = os.path.basename(directory.rstrip('/'))
            savio_path = os.path.join(cfg['savio']['images_folder'], country, folder_name)
            directory_mappings.append((directory, savio_path))
        return directory_mappings


    @ensure_connection('ftp')
    def upload_image_folders(self, directories, country):
        """
        Uploads image folders to Savio using the converted directory paths.

        :param directories: List of Tabei directory paths
        :param country: Country name for the Savio directory structure
        """
        directory_mappings = self.convert_to_savio_paths(directories, country)

        for local_directory, savio_directory in directory_mappings:
            # Ensure the remote directory exists
            self.makedirs(savio_directory)

            file_paths = [os.path.join(local_directory, file) for file in os.listdir(local_directory)]
            remote_paths = [os.path.join(savio_directory, file) for file in os.listdir(local_directory)]

            self.upload_files_sftp(file_paths, remote_paths)

        print("Upload complete.")

    @ensure_connection('ftp')
    def get_filepaths_in_folders(self, folder_paths):
        """
        Returns the file sizes of all files within the specified folders.

        :param folder_paths: List of paths to the folders
        :return: Dictionary where keys are folder paths and values are lists of tuples (file name, file size)
        """
        filepaths = []
        for folder_path in folder_paths:
            try:
                files = self.sftp.listdir(folder_path)
                fps = [os.path.join(folder_path, x) for x in files]
                filepaths.extend(fps)
            except Exception as e:
                print(f"An error occurred while accessing {folder_path}: {e}")
        return filepaths
    
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

    @ensure_connection('shell', verbose=False)
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
                    time.sleep(3)  # Sleep for 3 seconds
                    pbar.update(1)  # Update progress for each processed folder path
        return folder_total_sizes


if __name__ == "__main__":
    s = SavioClient()
    s.connect()
    x = s.listdir("/global/home/users/jeffreyclark")
    print(x)
    filesizes = s.get_file_sizes_in_folders(["/global/scratch/users/jeffreyclark/Images/Nigeria/NCAP_DOS_SHELL_BP_0043", "/global/scratch/users/jeffreyclark/Images/Nigeria/NCAP_DOS_SHELL_BP_0049"])
    print(filesizes)
    s.close()


