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

from Models.Contract import Country
from Models.GoogleDrive import ConfigSheet, StatusSheet, Status
from Functions.contract_utils import generate_default_config_data, export_config_file
import Functions.utilities as u
from Functions.shell_utils import generate_shell_script
from Models.Savio import SavioClient, pwd
from Models.Tabei import TabeiClient
from Models.GoogleVM import VMClient
from Models.GoogleBucket import GoogleBucket
from Local.update_apptainer import update_docker
from tqdm import tqdm
import time
import re
import collections
import pandas as pd
import geopandas as gpd



from Functions.automation_utils import upload_images_savio, upload_images_bucket, create_symlink_db
from Local.download_thumbnails import tabei_create_thumbnails_contract, download_thumbnails, tabei_create_thumbnails_filepaths


cfg = u.read_config()
config_db = ConfigSheet(cfg['google_drive']['config_files']['id'], "config")
status_db = StatusSheet(cfg['google_drive']['config_files']['id'], "status")



def get_contract_status_object(contract_alias, machine, country, contract_code=None):
    # load in the status object as contract_status
    exists = status_db.contract_exists(contract_alias, machine, cfg[machine]['username'])
    if not exists:
        status_db.add_contract(contract_alias, machine, cfg[machine]['username'], contract_code=contract_code)
    contract_status = Status(status_db, contract_alias, machine, cfg[machine]['username'], country)
    return contract_status


def get_contract(country, contract_name, refresh_data_overview=False):
    # Load the contract from Data Overview file.
    if refresh_data_overview:
        country_contracts = Country(country, refresh=True)
    else:
        country_contracts = Country(country, refresh=False)
    # get the specific contract of interest
    return country_contracts.get_contract(contract_name)


def upload_conifg_sheet_entry(contract_alias, contract, machine):
    # Now let us generate a default conifg and upload of not alreay exists
    config_data = generate_default_config_data(contract.df, machine)
    config_db.add_contract(contract_alias, machine, config_data)


def upload_images(country, contract_alias, contract, contract_status):
    if contract_status.data['image_upload'] != "Done":

        if contract_status.machine == "savio":
            t = TabeiClient()
            s = SavioClient()

            folders = contract.df.path.to_list()

            t.connect('shell')
            upload_status = upload_images_savio(contract_alias, folders, country, pwd, t, s)
            t.close()

        elif contract_status.machine == "google_vm":
            t = TabeiClient()
            b = GoogleBucket()
  
            folders = contract.df.path.to_list()
            workers = cfg['bucket'].get("workers", 1)
            t.connect('shell')
            upload_status = upload_images_bucket(contract_alias, folders, country, pwd, t, b, workers)
            t.close()

        if upload_status != "Complete":
            contract_status.update_status('image_upload', "Uploading...")
            raise ValueError(upload_status)

        contract_status.update_status('image_upload', "Done")



def create_symlinks(contract_alias, country, contract, contract_status):
    
    status = contract_status.data
    machine = status['machine']
    contract_cfg = config_db.get_config(contract_alias, machine)

    symlink_setting = contract_cfg.get('symlink', None)
    symlink_active = symlink_setting not in ["", None, False, "false"]

    if (not symlink_active) or (symlink_active and status['symlinks'] == "Done"):
        return True
        
    # Create the symlinks db
    t = TabeiClient()
    create_symlink_db(contract, contract_alias, country, t)

    # Create the symlinks on Tabei
    t.makedirs(f"{cfg['tabei']['stitching-services']}/Files/symlink_keys")
    tabei_symlinks_fp = f"{cfg['tabei']['stitching-services']}/Files/symlink_keys/{contract_alias}_symlinks_key.xlsx"
    t.upload_files_sftp(
        [f"{root_dir}/Files/symlink_keys/{contract_alias}_symlinks_key.xlsx"],
        [tabei_symlinks_fp],
        )

    stitch_env = "/home/jeffrey.clark/miniconda3/envs/stitch-service/bin/python3.11"
    command = f"Local/create_symlinks.py --symlinks_fp {tabei_symlinks_fp} --machine tabei"
    print("Creating symlinks on Tabei")
    t.execute_command(f"{stitch_env} {command}", cfg["tabei"]['stitching-services'])

    # Create the symlinks on remote machine
    shell_fp = generate_shell_script(contract_status, 'create_symlinks')

    if machine == "savio":
        s = SavioClient()
        s.makedirs(f"{cfg['savio']['repos']}/stitching-services/Files/symlink_keys")

        # create a savio shell for the link creation
        shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))
 
        s.upload_files_sftp(
            [
                f"{root_dir}/Files/symlink_keys/{contract_alias}_symlinks_key.xlsx",
                shell_fp
            ],
            [
                f"{cfg['savio']['repos']}/stitching-services/Files/symlink_keys/{contract_alias}_symlinks_key.xlsx",
                shell_fp_remote
            ]
        )
       
        s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
        print(f"Command sent to {machine}: Create symlinks")

    else:
        raise NotImplementedError("Symlinks not implemented for Google VM")
    
    raise RuntimeError("Wait for the symlinks to be generated. This should take less than a minute")



def regex_test(contract_alias, country, contract, contract_status):

    status = contract_status.data
    machine = status['machine']
    contract_cfg = config_db.get_config(contract_alias, machine)
    symlink_setting = contract_cfg.get('symlink', None)
    symlink_active = symlink_setting not in ["", None, False, "false"]


    if contract_status.data['regex_test'] != "Done":
            
        # we need to get all filepaths
        t = TabeiClient()

        if not symlink_active:
            folders = contract.df.path.to_list()
        else:
            folders = contract_status.load_symlinks('tabei')
        
        fps = t.get_filepaths_in_folders(folders)

        # check for a custom regex pattern in the config sheet
        contract_cfg = config_db.get_config(contract_alias, machine)
        regex_pattern = '^(?P<prefix>.*)_(?P<idx0>.*)_(?P<idx1>.*).(jpg|tif)'
        if "collection_regex" in contract_cfg.keys():
            if contract_cfg['collection_regex'] != None:
                regex_pattern = contract_cfg['collection_regex']
                print("we have custom regex pattern", regex_pattern)
        p = re.compile(regex_pattern)
        successful_indices = []
        successful_fps = {}
        failed_fps = []
        for fp in fps:
            file = os.path.basename(fp)
            if file.lower().startswith('job s') or file.lower().startswith('job c'):
                continue
            if ((file.endswith('.jpg') or file.endswith('.tif'))
                        and not file.startswith('._')):
                m = p.search(fp)

                try:
                    new_idx0 = int(m['idx0'])
                    new_idx1 = int(m['idx1'])
                    successful_indices.append((new_idx0, new_idx1))
                    # add to list value of dictionary where key is the index, if non existent add with blank list, then append
                    if (new_idx0, new_idx1) in successful_fps.keys():
                        successful_fps[(new_idx0, new_idx1)].append(fp)
                    else:
                        successful_fps[(new_idx0, new_idx1)] = [fp]
                                        
                except:
                    failed_fps.append(fp)
            else:
                failed_fps.append(fp)

        if len(failed_fps) > 0:
            contract_status.update_status('regex_test', f"Failed: {len(failed_fps)} {failed_fps[0:10]}")
            raise ValueError("Images need custom regex. Update config file.")
        
        # also check so that we have no duplicate ids in successful_indices
        if len(successful_indices) != len(set(successful_indices)):
            duplicates = [item for item, count in collections.Counter(successful_indices).items() if count > 1]
            duplicates_fps = []
            for d in duplicates:
                duplicates_fps = duplicates_fps + successful_fps[d]

            # create the duplicate thumbnails
            remote_thumbnail_dir = os.path.join(cfg['tabei']['thumbnails_folder'], country, f"{contract_alias}_duplicates")
            remote_zip_file = os.path.join(remote_thumbnail_dir, "duplicates.zip")
            local_zip_file = os.path.join(cfg['local']['results'], country, contract_alias, "duplicates.zip")

            tabei_create_thumbnails_filepaths(duplicates_fps,  remote_thumbnail_dir)
            download_thumbnails(remote_zip_file, local_zip_file)

            contract_status.update_status('regex_test', f"Failed: Duplicate indices")
            raise ValueError("Images have duplicate indices. Update config file.")


        print("Images paassed regex test.")
        contract_status.update_status('regex_test', f"Done")



def create_and_download_thumbnails(contract_status, country, contract):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    contract_cfg = config_db.get_config(contract_alias, machine)    
    symlink_setting = contract_cfg.get('symlink', None)
    symlink_active = symlink_setting not in ["", None, False, "false"]

    if not symlink_active:
        tabei_folders = contract.df.path.to_list()
    else:
        tabei_folders = contract_status.load_symlinks('tabei')

    if status['thumbnails'] != "Done":
        tabei_create_thumbnails_contract(tabei_folders, country, contract_alias)
        thumb_remote_zip = os.path.join(cfg['tabei']['thumbnails_folder'], country, contract_alias, "thumbnails.zip")
        thumb_local_zip = os.path.join(cfg['local']['thumbnails_folder'], country, f"{contract_alias}.zip")

        download_thumbnails(thumb_remote_zip, thumb_local_zip)
        contract_status.update_status('thumbnails', f"Done")


def check_if_crop_finished(contract_status):
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    # contract_cfg = config_db.get_config(contract_alias, machine)
    # auto_crop = contract_cfg.get('auto_crop', None) is not None

    # if auto_crop:
    #     contract_status.update_status('crop_params', f"Auto")
    #     contract_status.data['crop_params'] = "Auto"

    # ---- I commented out above because even with the auto cropping need to look at thumbnails and inidicate which corners have numbers
    
    if contract_status.data['crop_params'] in ["Done", "Auto"] == False:
        raise BrokenPipeError("You need to finish the manual cropping stage")
    
def upload_services_config(machine):

    # export the contract for savio
    rel_config_fp = "Config/config.yml"
    rel_google_service_fp = "Config/google_service.json"
    rel_fps = [rel_config_fp, rel_google_service_fp]
    local_fps = [os.path.join(root_dir, x) for x in rel_fps]

    if machine == "savio":
        s = SavioClient()
        remote_root = os.path.join(cfg["savio"]["repos"], "stitching-services")
        # ensure that the config folder exists
        s.makedirs(os.path.join(remote_root, "Config"))
        # upload the important files
        remote_fps = [os.path.join(remote_root, x) for x in rel_fps]
        s.upload_files_sftp(local_fps, remote_fps)

    elif machine == "google_vm":
        vm = VMClient()

        remote_root = cfg["google_vm"]["vm_paths"]["services_repo"]
        # ensure that the config folder exists
        vm.makedirs(os.path.join(remote_root, "Config"))
        # upload the important files
        remote_fps = [os.path.join(remote_root, x) for x in rel_fps]
        vm.upload_files_sftp(local_fps, remote_fps)


    print(f"Uploaded the Stitching Services Config and Google Service Account Credentials")



def upload_contract_config(contract_status, country):

    machine = contract_status.machine
    
    # export the contract for savio
    config_fp = config_db.export_config(contract_status, country, machine)
     # (conract_status, country)
    if machine == "savio":
        s = SavioClient()
        
        config_fp_remote = os.path.join(cfg[machine]['config_folder'], os.path.basename(config_fp))
        
        s.upload_files_sftp([config_fp], [config_fp_remote])

    elif machine == "google_vm":
        vm = VMClient()

        config_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['stitching_repo'], "config", os.path.basename(config_fp))
        
        vm.upload_files_sftp([config_fp], [config_fp_remote])

        # execute the command in a tmux session

        print(f"Re-uploaded the config file")


def initialize_and_crop(contract_alias, country, contract_status):
    
    status = contract_status.data
    machine = status['machine']

    if status['crop_params'] not in ["Done", "Skip"]:
        raise RuntimeError("You need to finish the manual cropping stage")
    if status['init_and_crop'] != "Done":
        
        # export the contract for savio
        config_fp = config_db.export_config(contract_status, country, machine)
        shell_fp = generate_shell_script(contract_status, 'initialize_and_crop')
 
        if machine == "savio":
            s = SavioClient()
            
            config_fp_remote = os.path.join(cfg[machine]['config_folder'], os.path.basename(config_fp))
            shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))
            
            s.upload_files_sftp([config_fp, shell_fp], [config_fp_remote, shell_fp_remote])

            # send execution command
            s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
            print(f"Command sent to {machine}: Initialize, Crop and Inspect Cropping")

        elif machine == "google_vm":
            vm = VMClient()

            # check if containers are running or if tmux is running
            vm.check_tmux_session(f"{contract_alias}_stage_1")

            config_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['stitching_repo'], "config", os.path.basename(config_fp))
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))
            
            vm.upload_files_sftp([config_fp, shell_fp], [config_fp_remote, shell_fp_remote])

            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_stage_1", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: Initialize, Crop and Inspect Cropping")

        contract_status.update_status('init_and_crop', f"Submitted")


def download_cropping_sample(contract_alias, country, contract_status):

    status = contract_status.data
    machine = status['machine']

    if status['init_and_crop'] != "Done":
        raise RuntimeError("You need to finish the initialize and cropping stage")

    if status['download_cropping_sample'] != "Done":
     
        local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)

        # make sure that we have the local dir
        os.makedirs(local_results_folder, exist_ok=True)

        if machine == "savio":
            s = SavioClient()
            cropping_sample_folder = os.path.join(cfg['savio']['results_folder'], contract_alias, 'cropping_samples')
            s.download_folder_sftp(cropping_sample_folder, local_results_folder)

        elif machine == "google_vm":
            # first find all the files called cropping_sample
            b = GoogleBucket()
            cropping_sample_folder = os.path.join(cfg['bucket']['root'], "results", contract_alias, 'cropping_samples')
            b.download_folders_from_bucket([cropping_sample_folder], local_results_folder)


        contract_status.update_status('download_cropping_sample', f"Done")

def featurize(contract_status):
    
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status['init_and_crop'] != "Done":
        raise RuntimeError("You need to finish the initialize and cropping stage")
    if status['featurize'] != "Done":
        

        if machine == "savio":
            s = SavioClient()

            # export and upload the shell script
            shell_fp = generate_shell_script(contract_status, 'featurize')
            shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))

            s.upload_files_sftp([shell_fp], [shell_fp_remote])

            # send execution command
            s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
            print(f"Command sent to {machine}: Featurize")

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp = generate_shell_script(contract_status, 'featurize')
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])

            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_stage_2", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: Featurize")


        contract_status.update_status('featurize', f"Submitted")


def swath_breaks(contract_status):
    
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status['featurize'] != "Done":
        raise RuntimeError("You need to finish the featurize stage")
    if status['swath_breaks'] != "Done":
        
        # export the shell script
        shell_fp = generate_shell_script(contract_status, 'swath_breaks')

        if machine == "savio":
            s = SavioClient()

            # upload the shell script
            shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))
            s.upload_files_sftp([shell_fp], [shell_fp_remote])

            # send execution command
            s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
            print(f"Command sent to {machine}: Featurize")

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])
            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_stage_3", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: swath_breaks")

        contract_status.update_status('swath_breaks', f"Submitted")


def rasterize_swaths(contract_status):
    
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status['swath_breaks'] != "Done":
        raise RuntimeError("You need to finish the swath_breaks stage")
    if status['rasterize_swaths'] != "Done":
        
        # export the shell script
        shell_fp = generate_shell_script(contract_status, 'create_raster_swaths')

        if machine == "savio":
            s = SavioClient()

            # upload the shell script
            shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))
            s.upload_files_sftp([shell_fp], [shell_fp_remote])

            # send execution command
            s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
            print(f"Command sent to {machine}: rasterize_swaths")

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])
            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_rasterize_swaths", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: rasterize_swaths")

        contract_status.update_status('rasterize_swaths', f"Submitted")


def download_swaths(contract_status, country):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status['rasterize_swaths'] != "Done":
        raise RuntimeError(f"You need to finish the swath rasterization stage")
    if status['download_swaths'] != "Done":

        if machine == "savio":
            s = SavioClient()
                
            swaths_folder = os.path.join(cfg['savio']['results_folder'], contract_alias, 'swaths')
            local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)

            s.download_folders_sftp([swaths_folder], local_results_folder)
        
        elif machine == "google_vm":
            b = GoogleBucket()

            bucket_swaths_folder = os.path.join(cfg['bucket']['root'], "results", contract_alias, 'swaths')

            local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)

            b.download_folders_from_bucket([bucket_swaths_folder], local_results_folder)

        contract_status.update_status("download_swaths", f"Done")



def download_img_df(contract_status):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if machine == "savio":
            s = SavioClient()
            remote_fp = os.path.join(cfg['savio']['cache_folder'], contract_alias, "img_df.geojson")
            local_fp = os.path.join(cfg['local']['results'], contract_status.country, contract_alias, "img_df.geojson")
            s.download_files_sftp([remote_fp], [local_fp])


    elif machine == "google_vm":
        b = GoogleBucket()

        remote_fp = os.path.join(cfg['bucket']['root'], "cache", contract_alias, "img_df.geojson")
        local_results_folder = os.path.join(cfg['local']['results'], contract_status.country, contract_alias)
        b.download_files_from_bucket([remote_fp], local_results_folder)

    # create an excel version of the image df
    img_df = gpd.read_file(local_fp)
    img_df_fp = os.path.join(cfg['local']['results'], contract_status.country, contract_alias, "img_df.xlsx")
    img_df.to_excel(img_df_fp, index=False)


def stitch_across(contract_status):
    
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status['swath_breaks'] != "Done":
        raise RuntimeError("You need to finish the initialize and cropping stage")
    
    if status['stitch_across'] == "Submitted":
        raise RuntimeError("Stitch across is running already...")

    if status['stitch_across'] != "Done":
        

        if machine == "savio":
            s = SavioClient()

            # export and upload the shell script
            shell_fp = generate_shell_script(contract_status, 'stitch_across')
            shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))

            # upload the shell script
            s.upload_files_sftp([shell_fp], [shell_fp_remote])

            # send execution command
            s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
            print(f"Command sent to {machine}: stitch-across")

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp = generate_shell_script(contract_status, 'stitch_across')
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])

            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_stage_3", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: stitch-across")

        contract_status.update_status('stitch_across', f"Submitted")



def run_pipeline(contract_status, stage, required_stage=None):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if required_stage is not None:
        if status[required_stage] != "Done":
            raise RuntimeError(f"You need to finish the {required_stage} stage")
    if status[stage] != "Done":
        
        if machine == "savio":
                s = SavioClient()

                # export and upload the shell script
                shell_fp = generate_shell_script(contract_status, stage)
                shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))

                # upload the shell script
                s.upload_files_sftp([shell_fp], [shell_fp_remote])

                # send execution command
                s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
                print(f"Command sent to {machine}: {stage}")

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp = generate_shell_script(contract_status, stage)
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])

            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_{stage}", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: {stage}")

        contract_status.update_status(stage, f"Submitted")



def rasterize_clusters(contract_status, ids=None, destination_suffix=None):
    
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status['initialize_graph'] != "Done":
        raise RuntimeError("You need to finish the initialize_graph stage")
    if status['rasterize_clusters'] != "Done":
        
        # export the shell script
        shell_fp = generate_shell_script(contract_status, 'create_raster_clusters', ids=ids, destination_suffix=destination_suffix)

        if machine == "savio":
            s = SavioClient()

            # export and upload the shell script
            shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))

            s.upload_files_sftp([shell_fp], [shell_fp_remote])
            # execute the command in a tmux session
            s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
            print(f"Command sent to {machine}: rasterize_clusters")

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])
            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_rasterize_clusters", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: rasterize_clusters")

        contract_status.update_status('rasterize_clusters', f"Submitted")


def download_clusters(contract_status, country, stage, required_stage):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status[required_stage] != "Done":
        raise RuntimeError(f"You need to finish the {required_stage} stage")
    if status[stage] != "Done":

        if machine == "savio":
            s = SavioClient()

            clusters_folder = os.path.join(cfg['savio']['results_folder'], contract_alias, 'clusters')
            local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)

            s.download_folders_sftp([clusters_folder], local_results_folder)

        elif machine == "google_vm":
            b = GoogleBucket()

            cluster_folders = ["clusters", "clusters_annotated"]
            remote_folders = [os.path.join(cfg['bucket']['root'], "results", contract_alias, x) for x in cluster_folders]
            local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)
            b.download_folders_from_bucket(remote_folders, local_results_folder)

        contract_status.update_status(stage, f"Done")



def new_neighbors(contract_status, stage, required_stage):
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status[required_stage] != "Done":
        raise RuntimeError(f"You need to finish the {required_stage} stage")
    if status[stage] != "Done":
        # parse the keep_clusters from the config file
        contract_config = config_db.get_config(contract_alias, machine)
        try:
            cluster_ids = [int(x.strip()) for x in contract_config['keep_clusters'].split(",")]
        except:
            raise ValueError("Cluster ids in config sheet 'keep_clusters' is not correctly specified. Make sure it is a comma separated string of integers, indicating clusters to keep")
        
        if machine == "savio":
            s = SavioClient()

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp = generate_shell_script(contract_status, stage, ids=cluster_ids)
        #     shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

        #     vm.upload_files_sftp([shell_fp], [shell_fp_remote])

        #     # execute the command in a tmux session
        #     vm.send_tmux_command(f"{contract_alias}_{stage}", f"bash {shell_fp_remote}")
        #     print(f"Command sent to {machine}: {stage}")
            

        # contract_status.update_status(stage, f"Submitted")



def export_for_georeferencing(contract_status):
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']

    if status['export_georef'] != "Done":
        # parse the keep_clusters from the config file
        contract_config = config_db.get_config(contract_alias, machine)
        try:
            cluster_ids = [int(x.strip()) for x in contract_config['keep_clusters'].split(",")]
        except:
            raise ValueError("Cluster ids in config sheet 'keep_clusters' is not correctly specified. Make sure it is a comma separated string of integers, indicating clusters to keep")
        
        if machine == "savio":
            s = SavioClient()

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp = generate_shell_script(contract_status, 'export_georef', ids=cluster_ids)
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])

            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_export_georef", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: export_georef")
            

        # contract_status.update_status(stage, f"Submitted")


def download_cluster_selected_raws(contract_status):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract_name']


    local_raw_clusters = os.path.join(cfg['local']['results'], country, contract_alias, "clusters_w_raws")
        # make sure that we have the local dir
    os.makedirs(local_raw_clusters, exist_ok=True)


    if machine == "savio":
        s = SavioClient()

    elif machine == "google_vm":
        # first find all the files called cropping_sample
        b = GoogleBucket()
        all_fps = b.listdir_bucket(os.path.join(cfg['bucket']['root'], "results", contract_alias))
        bucket_fps = [x for x in all_fps if "w_raws.tif" in x]
        b.download_files_from_bucket(bucket_fps, local_raw_clusters)



def main(contract_name, country, machine, contract_alias=None, refresh_data_overview=False, export_swath_gcps=False, contract_code=None):
    
    machine = machine.lower()

    if contract_alias is None:
        contract_alias = contract_name

    contract = get_contract(country, contract_name, refresh_data_overview) # This is a contract object from the Data Overview
   
    contract_status = get_contract_status_object(contract_alias, machine, country, contract_code=contract_code)  # This is the Google Sheet Status Row
    upload_conifg_sheet_entry(contract_alias, contract, machine)  # Add a default config row in Google Sheet Config
    
    # quick fix clean country string
    country = re.sub(r"\(NCAP\)", "", country).strip()


    # upload the services config
    # upload_services_config(machine)


    # if you need to re-upload the config, uncomment this line
    # upload_contract_config(contract_status, country)


    # If Savio, make sure we have sinularity container
    if machine == "savio":
        update_docker()
    
    # download_img_df(contract_status)


    # Step 1: Upload Images from Tabei to Machine
    upload_images(country, contract_alias, contract, contract_status)

    # Step 2: Regex Test
    create_symlinks(contract_alias, country, contract, contract_status)
    regex_test(contract_alias, country, contract, contract_status)

    # Step 3: Create and Download Thumbnails
    create_and_download_thumbnails(contract_status, country, contract)

    # Step 4: Manual Crop check
    check_if_crop_finished(contract_status)

    # Step 5: Initialize, crop and inspect crop
    initialize_and_crop(contract_alias, country, contract_status)

    # Step 6: Download crop
    download_cropping_sample(contract_alias, country, contract_status)

    # Step 7: Featurize
    featurize(contract_status)

    # Step 8: Swath breaks
    swath_breaks(contract_status)

    # Step 9: Download Swaths
    rasterize_swaths(contract_status)
    download_swaths(contract_status, country)  # only for local machine


    # Alternative Path for exporting GCPs
    if export_swath_gcps:
        run_pipeline(contract_status, "prepare_swaths")

        export_georeferencing(contract_name, country, machine, contract_alias=contract_alias)
        
        upload_archive_swaths(contract_name, country, machine, contract_alias=contract_alias)

        raise ValueError("STOPPED HERE")


    # Step 9: Stitch Across
    stitch_across(contract_status)

    # Step 10: Initialize Graph, Refine Links, Opt Links, Global Opts
    run_pipeline(contract_status, "initialize_graph", "stitch_across")
    # download_img_df(contract_status)

    # Step 11: Create Raster
    # run_pipeline(contract_status, "create_raster_1", "initialize_graph")
    rasterize_clusters(contract_status, ids=[0, 1, 2, 3])

    # Step 12: Download Rasters
    download_clusters(contract_status, country, "download_clusters_1", "rasterize_clusters")

    # # Step 13: New Neighbors
    # #new_neighbors(contract_status, "new_neighbors", "create_raster_1")


    # Export for georeferencing
    #export_for_georeferencing(contract_status)

    # Download cluster selected raws
    # download_cluster_selected_raws(contract_status)





def prepare_swaths_georeferencing(contract_name, country, machine, contract_alias=None, refresh_data_overview=False):
    
    machine = machine.lower()

    if contract_alias is None:
        contract_alias = contract_name

    contract = get_contract(country, contract_name, refresh_data_overview) # This is a contract object from the Data Overview
    contract_status = get_contract_status_object(contract_alias, machine)  # This is the Google Sheet Status Row
    upload_conifg_sheet_entry(contract_alias, contract, machine)  # Add a default config row in Google Sheet Config

    # quick fix clean country string
    country = re.sub(r"\(NCAP\)", "", country).strip()

    # --- code starts here ----

    run_pipeline(contract_status, "prepare_swaths")


def export_georeferencing(contract_code, country, machine, contract_alias=None, refresh_data_overview=False):

    machine = machine.lower()

    if contract_alias is None:
        contract_alias = f"{country}_{contract_code}"

    contract = get_contract(country, contract_code, refresh_data_overview)
    contract_status = get_contract_status_object(contract_alias, machine, country)  # This is the Google Sheet Status Row
    status = contract_status.data
    machine = status['machine']
    contract_cfg = config_db.get_config(contract_alias, machine)
    symlink_setting = contract_cfg.get('symlink', None)
    symlink_active = symlink_setting not in ["", None, False, "false"]

    if status['prepare_swaths'] != "Done":
        raise RuntimeError(f"You need to finish the prepare_swaths stage")

    if status['export_georef_swaths'] == "Done":
        return True

    if machine == "savio":
        # download the csv file
        s = SavioClient()
        remote_csv_fp = os.path.join(cfg['savio']['results_folder'], contract_alias, "swaths_selected_raws.csv")
        local_folder = os.path.join(cfg['local']['results'], country, contract_alias)
        local_csv_fp = os.path.join(local_folder, os.path.basename(remote_csv_fp))
        s.download_files_sftp([remote_csv_fp],[local_csv_fp])

        # download the swath_rasters
        local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)
        s.download_folders_sftp([os.path.join(cfg['savio']['results_folder'], contract_alias, "swaths_w_raws")], local_results_folder)

    elif machine == "google_vm":
        # download the csv file
        b = GoogleBucket()
        remote_csv_fp = os.path.join(cfg['bucket']['root'], "results", contract_alias, "swaths_selected_raws.csv")
        local_folder = os.path.join(cfg['local']['results'], country, contract_alias)
        b.download_files_from_bucket([remote_csv_fp],local_folder)

        # download the swath_rasters
        local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)
        b.download_folders_from_bucket([os.path.join(cfg['bucket']['root'], "results", contract_alias, "swaths_w_raws")], local_results_folder)

    # upload the config file to Tabei (make sure that the paths are correctly specified)
    local_config_fp = config_db.export_config(contract_status, country, "tabei")
    remote_config_fp = os.path.join(cfg["tabei"]['stitching_repo'], "config", country, os.path.basename(local_config_fp))
    t = TabeiClient()
    t.upload_files_sftp([local_config_fp], [remote_config_fp])

    # upload the swaths_selected_raws.csv from local to Tabei results folder
    local_csv_fp = os.path.join(local_folder, "swaths_selected_raws.csv")
    tabei_results_folder = os.path.join(cfg["tabei"]['results_folder'], contract_alias)
    t.makedirs(tabei_results_folder)
    tabei_csv_fp = os.path.join(tabei_results_folder, "swaths_selected_raws.csv")
    t.upload_files_sftp([local_csv_fp], [tabei_csv_fp])

    # Run the generate archive command
    relative_config = os.path.relpath(remote_config_fp, cfg["tabei"]['stitching_repo'])

    # figure out the base to replace filepaths
    if symlink_active is False:
        folders = contract.df.path.to_list()
        if machine == "savio":
            replace_from = cfg['savio']['images_folder']
        elif machine == "google_vm":
            replace_from = cfg['google_vm']['docker_paths']['images_folder']
        else:
            raise RuntimeError("Machine not supported")
    else:
        folders = contract_status.load_symlinks('tabei')
        if machine == "savio":
            replace_from = cfg['savio']['symlinks_folder']
        elif machine == "google_vm":
            raise ValueError('not tested yet')
            # replace_from = cfg['google_vm']['docker_paths']['symlink_folder']
        else:
            raise RuntimeError("Machine not supported")


    options = [
        "/mnt/shackleton_shares/lab/aerial_history_project/Images",
        '/shares/lab/aerial_history_project/symlinks'     
        ]
    
    replace_to = None
    for o in options:
        if folders[0].startswith(o):
            replace_to = o

    if replace_to is None:
        raise ValueError('FIX THE SUBSITUTION')
    

    env = "/home/jeffrey.clark/miniconda3/envs/surf/bin/python3.6"
    stitch_env = "/home/jeffrey.clark/miniconda3/envs/stitch-service/bin/python3.11"

    command = f"scripts/collect_and_zip_raws.py --config {relative_config} --replace-from {replace_from} --replace-to {replace_to} --group-type swaths"
    
    update_status_command = f"{cfg['tabei']['stitching-services']}/Local/update_status.py --machine {machine} --username {cfg[machine]['username']} --country {country} --contract_alias {contract_alias} --column export_georef_swaths --value Done"

    full_command = f"{env} {command} && {stitch_env} {update_status_command}"
    print(full_command)
    t.send_tmux_command(f"{contract_alias}_collect_and_zip", full_command, cfg["tabei"]['stitching_repo'])
    print(f"Command sent to Tabei: collect_and_zip_raws")

    contract_status.update_status('export_georef_swaths', f"Submitted")



def upload_archive_swaths(contract_code, country, machine, contract_alias=None, refresh_data_overview=False):

    machine = machine.lower()

    if contract_alias is None:
        contract_alias = f"{country}_{contract_code}"

    contract_status = get_contract_status_object(contract_alias, machine, country)  # This is the Google Sheet Status Row
    status = contract_status.data

    if status['export_georef_swaths'] != "Done":
        raise RuntimeError(f"You need to finish the export_georef_swaths stage")

    if not status['upload_archive_swaths'] in [None, ""] and status['upload_archive_swaths'] != "Done":
        raise RuntimeError(f"It seems like the uploading is in progress. Check the status in the Google Sheet")

    t = TabeiClient()
    env = "/home/jeffrey.clark/miniconda3/envs/stitch-service/bin/python3.11"
    stitch_env = "/home/jeffrey.clark/miniconda3/envs/stitch-service/bin/python3.11"

    command = f"Tabei/upload_archive.py --contract {contract_alias} --country {country}"
    update_status_command = f"{cfg['tabei']['stitching-services']}/Local/update_status.py --machine {machine} --username {cfg[machine]['username']} --country {country} --contract_alias {contract_alias} --column upload_archive_swaths --value Done"

    full_command = f"{env} {command} && {stitch_env} {update_status_command}"
    print(full_command)

    t.send_tmux_command(f"{contract_alias}_upload_archive", full_command, cfg["tabei"]['stitching-services'])
    print(f"Command sent to Tabei: upload archive")

    contract_status.update_status('upload_archive_swaths', f"Uploading")

if __name__ == "__main__":

    # # ----- ON SAVIO -----

    # country = "Nigeria"
    # contract_name = "NCAP_DOS_SHELL_BP"
    # # contract_name = "NCAP_DOS_USAAF_1"

    # main(contract_name, country, "savio")



    # ----- ON GOOGLE VM -----

    country = "Nigeria"
    # contract_name = "NCAP_DOS_126_NG"
    # contract_name = "NCAP_DOS_CAS_FI"
    # contract_name = "NCAP_DOS_CAS"

    country = "Nigeria"
    contract_code = "26_07"
    contract_name = "NCAP_DOS_212_NG"

    # contract_alias = "test"
    main(contract_code, country, "savio", contract_alias=contract_name, export_swath_gcps=False)

    # prepare_swaths_georeferencing(contract_name, country, "google_vm") 

    # export_georeferencing(contract_name, country, "google_vm")
    # upload_archive(contract_name, country)