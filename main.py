from Models.Contract import Country
from Models.GoogleDrive import ConfigSheet, StatusSheet, Status
from Functions.contract_utils import generate_default_config_data
import Functions.utilities as u
from Functions.shell_utils import generate_shell_script
from Models.Savio import SavioClient, pwd
from Models.Tabei import TabeiClient
from Models.GoogleVM import VMClient
from Models.GoogleBucket import GoogleBucket
from Local.update_apptainer import update_docker
import os
import sys
from tqdm import tqdm
import time
import re


from Functions.automation_utils import upload_images_savio, upload_images_bucket
from Local.download_thumbnails import tabei_create_thumbnails, download_thumbnails


cfg = u.read_config()
config_db = ConfigSheet(cfg['google_drive']['config_files']['id'], "config")
status_db = StatusSheet(cfg['google_drive']['config_files']['id'], "status")



def get_contract_status_object(contract_alias, machine):
    # load in the status object as contract_status
    exists = status_db.contract_exists(contract_alias, machine, cfg[machine]['username'])
    if not exists:
        status_db.add_contract(contract_alias, machine, cfg[machine]['username'])
    contract_status = Status(status_db, contract_alias, machine, cfg[machine]['username'])
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
            if upload_status != "Complete":
                raise ValueError(upload_status)
            
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


def regex_test(contract_alias, contract, contract_status):

    status = contract_status.data
    machine = status['machine']

    if contract_status.data['regex_test'] != "Done":
            
        # we need to get all filepaths
        t = TabeiClient()
        folders = contract.df.path.to_list()
        fps = t.get_filepaths_in_folders(folders)

        # check for a custom regex pattern in the config sheet
        contract_cfg = config_db.get_config(contract_alias, machine)
        regex_pattern = '^(?P<prefix>.*)_(?P<idx0>.*)_(?P<idx1>.*).(jpg|tif)'
        if "collection_regex" in contract_cfg.keys():
            if contract_cfg['collection_regex'] != None:
                regex_pattern = contract_cfg['collection_regex']

        p = re.compile(regex_pattern)
        failed_fps = []
        for fp in fps:
            file = os.path.basename(fp)
            if file.lower().startswith('job s') or file.lower().startswith('job c'):
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
            contract_status.update_status('regex_test', f"Failed: {len(failed_fps)} {failed_fps[0:10]}")
            raise ValueError("Images need custom regex. Update config file.")
        
        print("Images paassed regex test.")
        contract_status.update_status('regex_test', f"Done")


def create_and_download_thumbnails(country, contract_name, contract_status):
   
    if contract_status.data['thumbnails'] != "Done":
        tabei_create_thumbnails(country, contract_name)
        download_thumbnails(country, contract_name)
        contract_status.update_status('thumbnails', f"Done")


def check_if_crop_finished(contract_status):
    if contract_status.data['crop_params'] != "Done":
        raise BrokenPipeError("You need to finish the manual cropping stage")
    
def upload_config(contract_status, country):

    machine = contract_status.machine
    

    # export the contract for savio
    config_fp = config_db.export_config(contract_status.contract_alias, country, machine)
    
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

    if status['crop_params'] != "Done":
        raise RuntimeError("You need to finish the manual cropping stage")
    if status['init_and_crop'] != "Done":
        
        # export the contract for savio
        config_fp = config_db.export_config(contract_alias, country, machine)
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
            results_folder = os.path.join(cfg['savio']['results_folder'], contract_alias)
            results_files = s.listdir(results_folder)
            savio_fps = [os.path.join(results_folder, x) for x in results_files if "cropping_sample" in x]
            local_fps = [os.path.join(local_results_folder, x) for x in results_files if "cropping_sample" in x]
            
            s.download_files_sftp(savio_fps, local_fps)

        elif machine == "google_vm":
            # first find all the files called cropping_sample
            b = GoogleBucket()
            all_fps = b.listdir_bucket(os.path.join(cfg['bucket']['root'], "results", contract_alias))
            bucket_fps = [x for x in all_fps if "cropping_sample" in x]
            b.download_files_from_bucket(bucket_fps, local_results_folder)


        contract_status.update_status('download_cropping_sample', f"Done")

def featurize(contract_status):
    
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract']

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
    contract_alias = status['contract']

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
    contract_alias = status['contract']

    if status['swath_breaks'] != "Done":
        raise RuntimeError("You need to finish the swath_breaks stage")
    if status['rasterize_swaths'] != "Done":
        
        # export the shell script
        shell_fp = generate_shell_script(contract_status, 'create_raster_swaths')

        if machine == "savio":
            s = SavioClient()

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])
            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_rasterize_swaths", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: rasterize_swaths")

        contract_status.update_status('rasterize_swaths', f"Submitted")


def download_swaths(contract_status):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract']

    if status['rasterize_swaths'] != "Done":
        raise RuntimeError(f"You need to finish the swath rasterization stage")
    if status['download_swaths'] != "Done":

        if machine == "savio":
                s = SavioClient()
                raise ValueError("NO CODE FOR SAVIO COMPLETE")
        
        elif machine == "google_vm":
            b = GoogleBucket()

            bucket_swaths_folder = os.path.join(cfg['bucket']['root'], "results", contract_alias, 'swaths')

            local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)

            b.download_folders_from_bucket([bucket_swaths_folder], local_results_folder)

        contract_status.update_status("download_swaths", f"Done")




def stitch_across(contract_status):
    
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract']

    if status['swath_breaks'] != "Done":
        raise RuntimeError("You need to finish the initialize and cropping stage")
    
    if status['stitch_across'] == "Submitted":
        raise RuntimeError("Stitch across is running already...")

    if status['stitch_across'] != "Done":
        

        if machine == "savio":
            s = SavioClient()

            # # export and upload the shell script
            # shell_fp = generate_shell_script(contract_status, 2)
            # shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))

            # s.upload_files_sftp([shell_fp], [shell_fp_remote])

            # # send execution command
            # s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
            # print(f"Command sent to {machine}: Featurize")

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



def run_pipeline(contract_status, stage, required_stage):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract']

    if status[required_stage] != "Done":
        raise RuntimeError(f"You need to finish the {required_stage} stage")
    if status[stage] != "Done":
        
        if machine == "savio":
                s = SavioClient()

                # # export and upload the shell script
                # shell_fp = generate_shell_script(contract_status, 2)
                # shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))

                # s.upload_files_sftp([shell_fp], [shell_fp_remote])

                # # send execution command
                # s.execute_command(f"sbatch {shell_fp_remote}", cfg[machine]['shells_folder'])
                # print(f"Command sent to {machine}: Featurize")

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



def rasterize_clusters(contract_status):
    
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract']

    if status['initialize_graph'] != "Done":
        raise RuntimeError("You need to finish the initialize_graph stage")
    if status['create_raster_1'] != "Done":
        
        # export the shell script
        shell_fp = generate_shell_script(contract_status, 'create_raster_clusters')

        if machine == "savio":
            s = SavioClient()

        elif machine == "google_vm":
            vm = VMClient()

            # export and upload the shell script
            shell_fp_remote = os.path.join(cfg['google_vm']['vm_paths']['shells_folder'], os.path.basename(shell_fp))

            vm.upload_files_sftp([shell_fp], [shell_fp_remote])
            # execute the command in a tmux session
            vm.send_tmux_command(f"{contract_alias}_rasterize_swaths", f"bash {shell_fp_remote}")
            print(f"Command sent to {machine}: rasterize_swaths")

        contract_status.update_status('rasterize_clusters', f"Submitted")


def download_clusters(contract_status, stage, required_stage):

    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract']

    if status[required_stage] != "Done":
        raise RuntimeError(f"You need to finish the {required_stage} stage")
    if status[stage] != "Done":

        if machine == "savio":
                s = SavioClient()

        elif machine == "google_vm":
            b = GoogleBucket()

            bucket_results_folder = os.path.join(cfg['bucket']['root'], "results", contract_alias)

            bucket_files_fps = [x for x in b.listdir_bucket(bucket_results_folder) if not x.endswith("/")]
            local_results_folder = os.path.join(cfg['local']['results'], country, contract_alias)
            os.makedirs(os.path.dirname(local_results_folder), exist_ok=True)


            print(bucket_files_fps)
            print(local_results_folder)
            b.download_files_from_bucket(bucket_files_fps, local_results_folder)

        contract_status.update_status(stage, f"Done")



def new_neighbors(contract_status, stage, required_stage):
    status = contract_status.data
    machine = status['machine']
    contract_alias = status['contract']

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
    contract_alias = status['contract']

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
    contract_alias = status['contract']


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



def main(contract_name, country, machine, contract_alias=None, refresh_data_overview=False):
    
    machine = machine.lower()

    if contract_alias is None:
        contract_alias = contract_name

    contract = get_contract(country, contract_name, refresh_data_overview) # This is a contract object from the Data Overview
    contract_status = get_contract_status_object(contract_alias, machine)  # This is the Google Sheet Status Row
    upload_conifg_sheet_entry(contract_alias, contract, machine)  # Add a default config row in Google Sheet Config


    # if you need to re-upload the config, uncomment this line
    # upload_config(contract_status, country)

    # If Savio, make sure we have sinularity container
    if machine == "savio":
        update_docker()

    # Step 1: Upload Images from Tabei to Machine
    upload_images(country, contract_alias, contract, contract_status)

    # Step 2: Regex Test
    regex_test(contract_alias, contract, contract_status)

    # Step 3: Create and Download Thumbnails
    create_and_download_thumbnails(country, contract_name, contract_status)

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
    download_swaths(contract_status)  # only for local machine

    # Step 9: Stitch Across
    stitch_across(contract_status)

    # Step 10: Initialize Graph, Refine Links, Opt Links, Global Opts
    run_pipeline(contract_status, "initialize_graph", "stitch_across")

    # Step 11: Create Raster
    # run_pipeline(contract_status, "create_raster_1", "initialize_graph")
    rasterize_clusters(contract_status)


    # # Step 12: Download Rasters
    # # download_clusters(contract_status, "download_clusters_1", "create_raster_1")

    # # Step 13: New Neighbors
    # #new_neighbors(contract_status, "new_neighbors", "create_raster_1")


    # Export for georeferencing
    # export_for_georeferencing(contract_status)

    # Download cluster selected raws
    # download_cluster_selected_raws(contract_status)

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
    contract_name = "NCAP_DOS_CAS"
    # contract_alias = "test"
    main(contract_name, country, "google_vm") # , contract_alias=contract_alias)