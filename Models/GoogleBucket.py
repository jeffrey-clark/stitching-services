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

import subprocess
from tqdm import tqdm
import time

import Functions.utilities as u

cfg = u.read_config()


class GoogleBucket:
    def __init__(self) -> None:
        pass


    def convert_to_bucket_paths(self, directories, country):
            """
            Converts Tabei directory paths to Savio directory paths.

            :param directories: List of Tabei directory paths
            :param country: Country name for the Bucket directory structure
            :return: List of tuples (local_directory, bucket_directory)
            """

            directory_mappings = []
            for directory in directories:
                folder_name = os.path.basename(directory.rstrip('/'))
                bucket_path = os.path.join(cfg['bucket']['images'], country, folder_name)
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


    def download_files_from_bucket(self, bucket_paths, local_dest, max_retries=3, wait_seconds=5):
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


    def download_folders_from_bucket(self, bucket_paths, local_dest, max_retries=3, wait_seconds=5):
        # Ensure that bucket_paths is a list
        if not isinstance(bucket_paths, list):
            raise ValueError("bucket_paths must be a list")

        # Ensure that local_dest is a string
        if not isinstance(local_dest, str):
            raise ValueError("local_dest must be a string")

        # Check if local destination exists and create if not
        os.makedirs(local_dest, exist_ok=True)

        for gs_path in bucket_paths:
            if gs_path.startswith('gs://'):

                # Create local directories if they don't exist
                os.makedirs(local_dest, exist_ok=True)

                print(f"Downloading directory {gs_path} to {local_dest}...")

                for attempt in range(max_retries):
                    try:
                        subprocess.run(["gcloud", "storage", "cp", gs_path, local_dest, "--recursive"], check=True)
                        print(f"Downloaded directory {gs_path} successfully.")
                        break  # Break out of the retry loop if successful
                    except subprocess.CalledProcessError as e:
                        print(f"Failed to download directory {gs_path}: {e}.")
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
            files = [x for x in files if x != gs_dir]
            return files
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"An error occurred while listing the directory: {e}")


    def listdir_bucket_detailed(self, gs_dir):
        if not gs_dir.startswith('gs://'):
            raise ValueError("Invalid Google Cloud Storage path. Must start with 'gs://'.")

        if not gs_dir.endswith('/'):
            gs_dir += '/'

        try:
            result = subprocess.run(["gsutil", "ls", "-l", gs_dir], capture_output=True, text=True, check=True)
            lines = result.stdout.strip().split('\n')
            
            files_details = []
            for line in lines[:-1]:  # Skip the last line which is a total size
                parts = line.split()
                if len(parts) >= 3:
                    size = int(parts[0])
                    created = parts[1] 
                    path = parts[2]
                    files_details.append({'size': size, 'created': created, 'path': path})
            files_details = [x for x in files_details if x['path'] != gs_dir]
            return files_details
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"An error occurred while listing the directory: {e}")



    def move_file_in_bucket(self, source_path, destination_folder):
        """
        Moves a file within a Google Cloud Storage bucket to a specified folder.

        :param source_path: The source path of the file in the bucket (e.g., 'gs://bucket_name/path/to/source_file')
        :param destination_folder: The destination folder path in the bucket (e.g., 'gs://bucket_name/path/to/destination_folder/')
        """
        if not source_path.startswith('gs://') or not destination_folder.startswith('gs://'):
            raise ValueError("Invalid Google Cloud Storage paths. Must start with 'gs://'.")

        # Ensure the destination folder path ends with a slash
        if not destination_folder.endswith('/'):
            destination_folder += '/'

        # Extract the basename (filename) from the source path
        filename = os.path.basename(source_path)
        destination_path = os.path.join(destination_folder, filename)

        try:
            # Run the gsutil mv command
            subprocess.run(["gsutil", "mv", source_path, destination_path], check=True)
            # print(f"Moved {source_path} to {destination_path} successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to move {source_path}: {e}.")
            raise

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
