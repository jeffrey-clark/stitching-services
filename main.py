from Models.Contract import Country
from Models.GoogleDrive import ConfigSheet, StatusSheet, Status
from Functions.contract_utils import generate_default_config_data
import Functions.utilities as u
from Functions.shell_utils import generate_shell_script
from Models.Savio import SavioClient, pwd
from Models.Tabei import TabeiClient
import os
import sys
from tqdm import tqdm
import time


cfg = u.read_config()
config_db = ConfigSheet(cfg['google_drive']['config_files']['id'], "config")
status_db = StatusSheet(cfg['google_drive']['config_files']['id'], "status")


def compare_folder_tabei_savio(folders, t, s):
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


def upload_images_savio(contract_alias, folders, t, s):

    tmux_name = f"{contract_alias}_upload"  # set the tmux name
    
    # first check if there is a tmux session going
    tmux_sessions_response = t.list_tmux_sessions()
    if tmux_sessions_response is not None:
        if tmux_name in tmux_sessions_response:
            return "Upload is still ongoing. Please wait until complete."
            # print("Upload is still ongoing. Please wait until complete.")

    # if uploads are complete, we do filesize comparisons to verify correct upload
    mismatched_folders_detailed = compare_folder_tabei_savio(folders, t, s)
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


def main(machine, country, contract_name, contract_alias=None):

    if contract_alias is None:
        contract_alias = contract_name

    # load in the status object as contract_status
    exists = status_db.contract_exists(contract_name, machine, cfg['savio']['username'])
    if not exists:
        status_db.add_contract(contract_name, machine, cfg['savio']['username'])
    contract_status = Status(status_db, contract_name, machine, cfg['savio']['username'])

    # Load the contract from Data Overview file.
    country_contracts = Country(country, refresh=False)

    # get the specific contract of interest
    my_contract = country_contracts.get_contract(contract_name)

    # Now let us upload and or fetch configuration for the contract
    config_data = generate_default_config_data(my_contract.df)
    config_db.add_contract(contract_alias, config_data)
    
    # export the contract for savio
    config_fp = config_db.export_config(contract_alias, country, machine)
    

    # ----- upload the files from Tabei to Machine -----
    if contract_status.get_status()['image_upload'] != "Done":
        t = TabeiClient()
        s = SavioClient()

        folders = my_contract.df.path.to_list()

        # add a quick skip from google sheet if we know uploads went well. 
        t.connect('shell')
        upload_status = upload_images_savio(contract_alias, folders, t, s)
        t.close()
        if upload_status != "Complete":
            raise ValueError(upload_status)

        contract_status.update_status('image_upload', "Done")


    print("we made it here")








 
    
    # -------------------------------------------------

    # config_fp_remote = os.path.join(cfg[machine]['config_folder'], os.path.basename(config_fp))
    # shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))


    # s = SavioClient()
    # s.upload_files_sftp([config_fp, shell_fp], [config_fp_remote, shell_fp_remote])

    # s.upload_files_sftp()


    # send execution command






if __name__ == "__main__":
    machine = "savio"
    country = "Nigeria"
    contract_name = "NCAP_DOS_SHELL_BP"
    contract_name = "NCAP_DOS_USAAF_1"
    main(machine, country, contract_name)

    # s = SavioClient()
    # sq = s.get_job_list()
    # print(sq)