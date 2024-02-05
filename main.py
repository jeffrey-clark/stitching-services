from Models.Contract import Country
from Models.GoogleDrive import ConfigCollection
from Functions.contract_utils import generate_default_config_data
import Functions.utilities as u
from Functions.shell_utils import generate_shell_script
from Models.Savio import SavioClient, pwd
from Models.Tabei import TabeiClient
import os
import sys
from tqdm import tqdm


cfg = u.read_config()
config_db = ConfigCollection(cfg['google_drive']['config_files']['id'], "config")


def main(machine, country, contract_name, contract_alias=None):

    if contract_alias is None:
        contract_alias = contract_name

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


    t = TabeiClient()
    s = SavioClient()

    tmux_name = f"{contract_alias}_upload"  # set the tmux name

    folders = my_contract.df.path.to_list()

    # first check if there is a tmux session going
    tmux_sessions_response = t.list_tmux_sessions()
    if tmux_name in tmux_sessions_response:
        # return "Upload is still ongoing. Please wait until complete."
        print("Upload is still ongoing. Please wait until complete.")

    
    # if uploads are complete, we do filesize comparisons to verify correct upload
    print("  Fetching filesizes from Tabei:")
    tabei_sizes = t.get_folder_total_sizes(folders)
  
    print("  Fetching filesizes from Savio:")
    savio_directory_mappings = s.convert_to_savio_paths(folders, country)
    savio_paths = [savio_path for _, savio_path in savio_directory_mappings]
    savio_sizes = s.get_folder_total_sizes(savio_paths)


    with tqdm(total=len(savio_directory_mappings), desc="Checking folder sizes", file=sys.stdout) as pbar:
        for tabei_key, savio_key in savio_directory_mappings:
            tabei_size = tabei_sizes.get(tabei_key)
            savio_size = savio_sizes.get(savio_key)

            # Comparing sizes with a tolerance of 0.1%
            if tabei_size is not None and savio_size is not None:
                size_difference = abs(tabei_size - savio_size)
                allowed_difference = 0.001 * max(tabei_size, savio_size)  # 0.1% of the larger size

                if size_difference <= allowed_difference:
                    pbar.update(1)  # Update progress for each checked folder
                    continue
                else:
                    raise Exception(f"Size mismatch beyond tolerance for {tabei_key} ({tabei_size} bytes) and {savio_key} ({savio_size} bytes), difference: {size_difference} bytes")
            else:
                raise Exception(f"Size information missing for {tabei_key} or {savio_key}")

    print("All folder sizes match within tolerance.")






    # env_interpreter = os.path.join(cfg['tabei']['conda_env'], "bin", "python")
    # cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/upload_folders.py") 
    # folder_string = " ".join(folders)
    # set_password = f"export SAVIO_DECRYPTION_PASSWORD={pwd}"  # need to send the password for SavioClient decryption.
    # command = f"{set_password} && {env_interpreter} {cmd_path} --paths {folder_string} --country {country} --machine Savio"

    


    # # send the command to upload
    # t.send_tmux_command(f"{contract_alias}_upload", command)


 
    
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
    main(machine, country, contract_name)

    # s = SavioClient()
    # sq = s.get_job_list()
    # print(sq)