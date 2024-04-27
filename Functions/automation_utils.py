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
import re

import Functions.utilities as u
from Models.GoogleBucket import GoogleBucket
import pandas as pd
from Levenshtein import distance as levenshtein_distance

cfg = u.read_config()


def compare_folder_tabei_savio(folders, country, t, s):
    print("  Fetching filesizes from Tabei:")
    tabei_sizes = t.get_folder_total_sizes(folders)

    print("  Fetching filesizes from Savio:")
    savio_directory_mappings = s.convert_to_savio_folderpaths(folders, country)
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
        stitch_env = "/home/jeffrey.clark/miniconda3/envs/stitch-service/bin/python3.11"

        cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/upload_folders.py") 
        folder_string = " ".join(mismatched_folders)
        set_password = f"export SAVIO_DECRYPTION_PASSWORD={pwd}"  # need to send the password for SavioClient decryption.

        update_status_command = f"{stitch_env} {cfg['tabei']['stitching-services']}/Local/update_status.py --machine savio --username {cfg['savio']['username']} --contract_alias {contract_alias} --column image_upload --value Done"

        command = f"{set_password} && {env_interpreter} {cmd_path} --paths {folder_string} --country {country} --destination Savio && {update_status_command}"

        # send the command to upload
        t.send_tmux_command(f"{contract_alias}_upload", command)

        return "Images are being uploaded. Please wait until complete."
            
    return "Complete"



def compare_folder_tabei_bucket(folders, country, t, b):
    print("  Fetching filesizes from Tabei:")
    tabei_sizes = t.get_folder_total_sizes(folders)

    print("  Fetching filesizes from Google Bucket:")
    bucket_directory_mappings = b.convert_to_bucket_paths(folders, country)
    bucket_paths = [bucket_path for _, bucket_path in bucket_directory_mappings]
    bucket_sizes = b.get_bucket_folders_total_sizes(bucket_paths)

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



def upload_images_bucket(contract_alias, folders, country, pwd, t, b, workers):

    tmux_name = f"{contract_alias}_upload"  # set the tmux name
    
    # first check if there is a tmux session going
    tmux_sessions_response = t.list_tmux_sessions()
    if tmux_sessions_response is not None:
        if tmux_name in tmux_sessions_response:
            return "Upload is still ongoing. Please wait until complete."
            # print("Upload is still ongoing. Please wait until complete.")

    # if there is no tmux session, we do filesize comparisons to verify correct upload
    mismatched_folders_detailed = compare_folder_tabei_bucket(folders, country, t, b)
    
    print("DETAILS:",mismatched_folders_detailed)

    mismatched_folders = [x['tabei_key'] for x in mismatched_folders_detailed]

    # Delete folders where there are discrpancies Something must have gone wrong
    # too_big = [x for x in mismatched_folders_detailed if int(x['tabei_size']) < int(x['bucket_size'])]
    if len(mismatched_folders_detailed) > 0:
        b.delete_from_bucket([x['bucket_key'] for x in mismatched_folders_detailed])

    if len(mismatched_folders) > 0:
        # prepare the tmux upload command
        env_interpreter = os.path.join(cfg['tabei']['conda_env'], "bin", "python")
        cmd_path_upload = os.path.join(cfg['tabei']['stitching-services'], "Tabei/upload_folders.py")
        cmd_path_update = os.path.join(cfg['tabei']['stitching-services'], "Local/update_status.py") 
        folder_string = " ".join(mismatched_folders)
        set_password = f"export SAVIO_DECRYPTION_PASSWORD={pwd}"  # need to send the password for SavioClient decryption.
        upload_command = f"{env_interpreter} {cmd_path_upload} --paths {folder_string} --country {country} --destination Bucket --workers {workers}"
        update_status_command = f"{env_interpreter} {cmd_path_update} --contract_alias {contract_alias} --machine google_vm --column image_upload --value Done"
        command = f"{set_password} && {upload_command} && {update_status_command}"

        # send the command to upload
        t.send_tmux_command(f"{contract_alias}_upload", command)

        return "Images are being uploaded. Please wait until complete."
            
    return "Complete"




def create_symlink_db(contract, contract_alias, country, t):
    local_dest = "Files/symlink_keys"
    fp = f"{local_dest}/{contract_alias}_symlinks_key.xlsx"

    if os.path.isfile(fp):
        return fp
    
    folder_paths = contract.df.path.to_list()
    folder_names = [os.path.basename(x.rstrip("/")) for x in folder_paths]
    folder_ids = {name: i for i, name in enumerate(folder_names)} 

    fps = t.get_filepaths_in_folders(folder_paths)

    # Assuming 'fps' is a list of file paths
    df = pd.DataFrame(fps, columns=['path'])

    # Create new columns 'foldername' and 'filename' based on dirname and basename
    df["country"] = country
    df["contract_alias"] = contract_alias
    df['foldername'] = df['path'].apply(lambda x: os.path.basename(os.path.dirname(x)))
    df['filename'] = df['path'].apply(os.path.basename)

    # Map foldername to folder_id using the dictionary
    df['folder_id'] = df['foldername'].map(folder_ids)


    # Calculate the average Levenshtein distance for each filename to all others in the group
    def calculate_average_distances(group):
        filenames = group['filename'].tolist()
        results = []
        for filename in filenames:
            distances = [levenshtein_distance(filename, other) for other in filenames if other != filename]
            avg_distance = sum(distances) / len(distances) if distances else 0
            results.append(avg_distance)
        return pd.Series(results, index=group.index)


    df['avg_lev_distance'] = df.groupby('foldername').apply(calculate_average_distances).reset_index(level=0, drop=True)

    # Filtering based on average Levenshtein distance
    mode_distance = df['avg_lev_distance'].mode()[0]
    print("Filtering out the following files from symlinks")
    print(df.loc[df['avg_lev_distance'] >= 3 * mode_distance, "path"].values)
    df = df[df['avg_lev_distance'] < 3 * mode_distance]

    # Sort by folder_id and filename
    df = df.sort_values(['folder_id', 'filename'])
    df['file_id'] = df.groupby('folder_id').cumcount()

    # Save or process data as needed
    os.makedirs(local_dest, exist_ok=True)
    df.to_excel(fp, index=False)
    print("Saved symlink key file")
    return fp






if __name__ == "__main__":
    b = GoogleBucket()

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

    filepaths = b.convert_to_bucket_paths(folders, "Nigeria")

