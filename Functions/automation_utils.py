import os

import Functions.utilities as u

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

    # if uploads are complete, we do filesize comparisons to verify correct upload
    mismatched_folders_detailed = compare_folder_tabei_savio(folders, country, t, s)
    mismatched_folders = [x['tabei_key'] for x in mismatched_folders_detailed]

    if len(mismatched_folders) > 0:
        # prepare the tmux upload command
        env_interpreter = os.path.join(cfg['tabei']['conda_env'], "bin", "python")
        cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/upload_folders.py") 
        folder_string = " ".join(mismatched_folders)
        set_password = f"export SAVIO_DECRYPTION_PASSWORD={pwd}"  # need to send the password for SavioClient decryption.
        command = f"{set_password} && {env_interpreter} {cmd_path} --paths {folder_string} --country {country} --machine Savio"

        # send the command to upload
        t.send_tmux_command(f"{contract_alias}_upload", command)

        return "Images are being uploaded. Please wait until complete."
            
    return "Complete"



