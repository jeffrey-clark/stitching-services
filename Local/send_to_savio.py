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



from Models.Savio import SavioClient


if __name__ == "__main__":
    config_files = [
        "Files/config_files/other_config.yml"
        ]
    config_remote_dir = "/global/home/users/jeffreyclark/repos/aerial-history-stitching/config/"
    remote_config_paths = [os.path.join(config_remote_dir, os.path.basename(f)) for f in config_files]

    shell_files = ["Files/job_shells/test.sh"]
    shell_remote_dir = "/global/home/users/jeffreyclark/slurm_scripts/"
    remote_shell_paths = [os.path.join(shell_remote_dir, os.path.basename(f)) for f in shell_files]

    local_paths = config_files + shell_files
    remote_paths = remote_config_paths + remote_shell_paths

    s = SavioClient()
    s.upload_files_sftp(local_paths, remote_paths)
