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
from Models.Savio import SavioClient
import Functions.utilities as u

cfg = u.read_config()


def get_last_modified_time(file_path):
    """
    Returns the last modified time of the given file.
    """
    return os.path.getmtime(file_path)


def upload_image_to_savio():
    s = SavioClient()

    dockerimage_path = os.path.join(root_dir, "Files/stitching-services.tar")
    remote_home = os.path.join("/global/home/users", cfg['savio']['username'])
    remote_resources = os.path.join(remote_home, "resources")
    remote_path = os.path.join(remote_resources, "stiching-services.tar")

    s.connect('ftp')
    # make sure that we have the resources dir
    s.makedirs(os.path.join(remote_home, "resources"))

    # upload the Docker Image
    s.upload_files_sftp([dockerimage_path], [remote_path])
    s.close()

    # convert the docker image to Apptainer container
    o, e = s._execute_command_capture_error("apptainer build --force stitching-services.sif docker-archive://stiching-services.tar", remote_resources)


def update_docker():
    """
    Updates the Docker image if it is older than the Dockerfile.
    """
    dockerfile_path = os.path.join(root_dir, "Dockerfile")
    dockerimage_path = os.path.join(root_dir, "Files/stitching-services.tar")

    # Check if Dockerfile exists
    if not os.path.exists(dockerfile_path):
        raise FileNotFoundError(f"Dockerfile not found: {dockerfile_path}")

    # Check if Docker image exists
    if not os.path.exists(dockerimage_path):
        print("Docker image is missing. Creating new image...")
        subprocess.run(["docker", "build", "-t", "stitching-services", root_dir])
        # Save Docker image as a tar file
        print("Exporting Docker Image")
        with open(dockerimage_path, 'wb') as f:
            subprocess.run(["docker", "save", "stitching-services"], stdout=f)
        upload_image_to_savio()
        return

    # Get last modified times
    dockerfile_time = get_last_modified_time(dockerfile_path)
    dockerimage_time = get_last_modified_time(dockerimage_path)

    # Compare modification times
    if dockerimage_time < dockerfile_time:
        print("Docker image is older than Dockerfile. Updating...")
        subprocess.run(["docker", "build", "-t", "stitching-services", root_dir])
        # Save the updated Docker image as a tar file
        print("Exporting Docker Image")
        with open(dockerimage_path, 'wb') as f:
            subprocess.run(["docker", "save", "stitching-services"], stdout=f)
        upload_image_to_savio()
    else:
        print("Docker image is up-to-date.")

if __name__ == "__main__":
    update_docker()
