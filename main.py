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
import re


from Functions.automation_utils import upload_images_savio
from Local.download_thumbnails import tabei_create_thumbnails, download_thumbnails


cfg = u.read_config()
config_db = ConfigSheet(cfg['google_drive']['config_files']['id'], "config")
status_db = StatusSheet(cfg['google_drive']['config_files']['id'], "status")



def get_contract_status_object(contract_alias, machine):
    # load in the status object as contract_status
    exists = status_db.contract_exists(contract_alias, machine, cfg['savio']['username'])
    if not exists:
        status_db.add_contract(contract_alias, machine, cfg['savio']['username'])
    contract_status = Status(status_db, contract_alias, machine, cfg['savio']['username'])
    return contract_status


def get_contract(country, contract_name, refresh_data_overview=False):
    # Load the contract from Data Overview file.
    if refresh_data_overview:
        country_contracts = Country(country, refresh=True)
    else:
        country_contracts = Country(country, refresh=False)
    # get the specific contract of interest
    return country_contracts.get_contract(contract_name)


def upload_images(country, contract_alias, contract, contract_status):
    if contract_status.get_status()['image_upload'] != "Done":
        t = TabeiClient()
        s = SavioClient()

        folders = contract.df.path.to_list()

        # add a quick skip from google sheet if we know uploads went well. 
        t.connect('shell')
        upload_status = upload_images_savio(contract_alias, folders, country, pwd, t, s)
        t.close()
        if upload_status != "Complete":
            raise ValueError(upload_status)

        contract_status.update_status('image_upload', "Done")


def regex_test(contract_alias, contract, contract_status):

    if contract_status.get_status()['regex_test'] != "Done":
            
        # we need to get all filepaths
        t = TabeiClient()
        folders = contract.df.path.to_list()
        fps = t.get_filepaths_in_folders(folders)

        # check for a custom regex pattern in the config sheet
        contract_cfg = config_db.get_config(contract_alias)
        regex_pattern = '^(?P<prefix>.*)_(?P<idx0>.*)_(?P<idx1>.*).(jpg|tif)'
        if "collection_regex" in contract_cfg.keys():
            if contract_cfg['collection_regex'] != None:
                regex_pattern = contract_cfg['collection_regex']

        p = re.compile(regex_pattern)
        failed_fps = []
        for fp in fps:
            file = os.path.basename(fp)
            if file.lower().startswith('job sheet') or file.lower().startswith('job card'):
                continue
            if ((file.endswith('.jpg') or file.endswith('.tif'))
                        and not file.startswith('._')):
                m = p.search(file)
                try:
                    new_idx0 = int(m['idx0'])
                    new_idx1 = int(m['idx1'])
                except:
                    failed_fps.append(fp)
            else:
                failed_fps.append(fp)

        if len(failed_fps) > 0:
            contract_status.update_status('regex_test', f"Failed: {failed_fps[0:2]}")
            raise ValueError("Images need custom regex. Update config file.")
        
        print("Images paassed regex test.")
        contract_status.update_status('regex_test', f"Done")


def create_and_download_thumbnails(country, contract_name, contract_status):
   
    if contract_status.get_status()['thumbnails'] != "Done":
        tabei_create_thumbnails(country, contract_name)
        download_thumbnails(country, contract_name)
        contract_status.update_status('thumbnails', f"Done")


def check_if_crop_finished(contract_status):
    if contract_status.get_status()['crop_params'] != "Done":
        raise BrokenPipeError("You need to finish the manual cropping stage")


def main(contract_name, country, machine, contract_alias=None, refresh_data_overview=False):

    if contract_alias is None:
        contract_alias = contract_name

    contract = get_contract(country, contract_name, refresh_data_overview) # This is a contract object from the Data Overview
    contract_status = get_contract_status_object(contract_alias, machine)  # This is the Google Sheet Status Row

    # Step 1: Upload Images from Tabei to Machine
    upload_images(country, contract_alias, contract, contract_status)

    # Step 2: Regex Test
    regex_test(contract_alias, contract, contract_status)

    # Step 3: Create and Download Thumbnails
    create_and_download_thumbnails(country, contract_name, contract_status)

    # Step 4: Manual Crop check
    check_if_crop_finished(contract_status)

    


    #  # ---- upload and execute the initialize and cropping script
    
    # if contract_status.get_status()['cropping'] != "Done":

    #     # Now let us generate a default conifg and upload of not alreay exists
    #     config_data = generate_default_config_data(my_contract.df)
    #     config_db.add_contract(contract_alias, config_data)
        
    #     # export the contract for savio
    #     config_fp = config_db.export_config(contract_alias, country, machine)
    #     shell_fp = generate_shell_script(contract_alias, machine, 1)

    #     config_fp_remote = os.path.join(cfg[machine]['config_folder'], os.path.basename(config_fp))
    #     shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))

    #     s = SavioClient()
    #     s.upload_files_sftp([config_fp, shell_fp], [config_fp_remote, shell_fp_remote])

    #     # send execution command
    #     # s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])





if __name__ == "__main__":
    country = "Nigeria"
    contract_name = "NCAP_DOS_SHELL_BP"
    # contract_name = "NCAP_DOS_USAAF_1"

    main(contract_name, country, "savio")