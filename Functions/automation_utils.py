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

import Functions.utilities as u
from Models.GoogleVM import VMClient

cfg = u.read_config()


def compare_folder_tabei_savio(folders, country, t, s):
    print("  Fetching filesizes from Tabei:")
    tabei_sizes = t.get_folder_total_sizes(folders)

    print("  Fetching filesizes from Savio:")
    savio_directory_mappings = s.convert_to_savio_paths(folders, country)
    savio_paths = [savio_path for _, savio_path in savio_directory_mappings]
    savio_sizes = s.get_folder_total_sizes(savio_paths)

    mismatched_folders = []

    for tabei_key, savio_key in savio_directory_mappings:
        tabei_size = tabei_sizes.get(tabei_key)
        savio_size = savio_sizes.get(savio_key)

        # Comparing sizes with a tolerance of 0.1%
        if tabei_size is not None and savio_size is not None:
            size_difference = abs(tabei_size - savio_size)
            allowed_difference = 0.001 * max(tabei_size, savio_size)  # 0.1% of the larger size

            if size_difference > allowed_difference:
                mismatched_folders.append({
                    "tabei_key": tabei_key,
                    "tabei_size": tabei_size,
                    "savio_key": savio_key,
                    "savio_size": savio_size,
                    "difference": size_difference
                })
        else:
            mismatched_folders.append({
                "tabei_key": tabei_key,
                "savio_key": savio_key,
                "issue": "Missing size information"
            })

    if mismatched_folders:
        print("There are mismatched folders.")
        return mismatched_folders
    else:
        print("All folder sizes match within tolerance.")
        return []



def upload_images_savio(contract_alias, folders, country, pwd, t, s):

    tmux_name = f"{contract_alias}_upload"  # set the tmux name
    
    # first check if there is a tmux session going
    tmux_sessions_response = t.list_tmux_sessions()
    if tmux_sessions_response is not None:
        if tmux_name in tmux_sessions_response:
            return "Upload is still ongoing. Please wait until complete."
            # print("Upload is still ongoing. Please wait until complete.")

    # if there is no tmux session, we do filesize comparisons to verify correct upload
    mismatched_folders_detailed = compare_folder_tabei_savio(folders, country, t, s)
    mismatched_folders = [x['tabei_key'] for x in mismatched_folders_detailed]

    if len(mismatched_folders) > 0:
        # prepare the tmux upload command
        env_interpreter = os.path.join(cfg['tabei']['conda_env'], "bin", "python")
        cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/upload_folders.py") 
        folder_string = " ".join(mismatched_folders)
        set_password = f"export SAVIO_DECRYPTION_PASSWORD={pwd}"  # need to send the password for SavioClient decryption.
        command = f"{set_password} && {env_interpreter} {cmd_path} --paths {folder_string} --country {country} --destination Savio"

        # send the command to upload
        t.send_tmux_command(f"{contract_alias}_upload", command)

        return "Images are being uploaded. Please wait until complete."
            
    return "Complete"



def compare_folder_tabei_bucket(folders, country, t, vm):
    print("  Fetching filesizes from Tabei:")
    tabei_sizes = t.get_folder_total_sizes(folders)

    print("  Fetching filesizes from Savio:")
    bucket_directory_mappings = vm.convert_to_bucket_paths(folders, country)
    bucket_paths = [bucket_path for _, bucket_path in bucket_directory_mappings]
    bucket_sizes = vm.get_bucket_folders_total_sizes(bucket_paths)

    mismatched_folders = []

    for tabei_key, bucket_key in bucket_directory_mappings:
        tabei_size = tabei_sizes.get(tabei_key)
        bucket_size = bucket_sizes.get(bucket_key)

        # Comparing sizes with a tolerance of 0.1%
        if tabei_size is not None and bucket_size is not None:
            size_difference = abs(tabei_size - bucket_size)
            allowed_difference = 0.001 * max(tabei_size, bucket_size)  # 0.1% of the larger size

            if size_difference > allowed_difference:
                mismatched_folders.append({
                    "tabei_key": tabei_key,
                    "tabei_size": tabei_size,
                    "bucket_key": bucket_key,
                    "bucket_size": bucket_size,
                    "difference": size_difference
                })
        else:
            mismatched_folders.append({
                "tabei_key": tabei_key,
                "bucket_key": bucket_key,
                "issue": "Missing size information"
            })

    if mismatched_folders:
        print("There are mismatched folders.")
        return mismatched_folders
    else:
        print("All folder sizes match within tolerance.")
        return []



def upload_images_bucket(contract_alias, folders, country, pwd, t, vm):

    tmux_name = f"{contract_alias}_upload"  # set the tmux name
    
    # first check if there is a tmux session going
    tmux_sessions_response = t.list_tmux_sessions()
    if tmux_sessions_response is not None:
        if tmux_name in tmux_sessions_response:
            return "Upload is still ongoing. Please wait until complete."
            # print("Upload is still ongoing. Please wait until complete.")

    # if there is no tmux session, we do filesize comparisons to verify correct upload
    mismatched_folders_detailed = compare_folder_tabei_bucket(folders, country, t, vm)
    
    print("DETAILS:",mismatched_folders_detailed)

    mismatched_folders = [x['tabei_key'] for x in mismatched_folders_detailed]

    if len(mismatched_folders) > 0:
        # prepare the tmux upload command
        env_interpreter = os.path.join(cfg['tabei']['conda_env'], "bin", "python")
        cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/upload_folders.py") 
        folder_string = " ".join(mismatched_folders)
        set_password = f"export SAVIO_DECRYPTION_PASSWORD={pwd}"  # need to send the password for SavioClient decryption.
        command = f"{set_password} && {env_interpreter} {cmd_path} --paths {folder_string} --country {country} --destination Bucket"

        # send the command to upload
        t.send_tmux_command(f"{contract_alias}_upload", command)

        return "Images are being uploaded. Please wait until complete."
            
    return "Complete"



if __name__ == "__main__":
    v = VMClient()

    folders = [
        "/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_0043/",
        "/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_0044/",
        "/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_0045/",
        "/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_0046/",
        "/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_0047/",
        "/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_0048/",
        "/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_0049/",
        "/mnt/shackleton_shares/lab/aerial_history_project/Images/Nigeria/NCAP_DOS_SHELL_BP_0051/",
    ]

    filepaths = v.convert_to_bucket_paths(folders, "Nigeria")

