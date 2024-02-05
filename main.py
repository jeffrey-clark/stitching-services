from Models.Contract import Country
from Models.GoogleDrive import ConfigCollection
from Functions.contract_utils import generate_default_config_data
import Functions.utilities as u
from Functions.shell_utils import generate_shell_script
from Models.Savio import SavioClient, pwd
from Models.Tabei import TabeiClient
import os


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
    config_fp = config_db.export_config(contract_alias, machine)
    


    # upload the files from Tabei to Machine
    folders = my_contract.df.path.to_list()[0:1]
    folder_string = " ".join(folders)

    cmd_activate_conda = f"conda activate stitch-service"  # or use "conda activate"
    cmd_path = os.path.join(cfg['tabei']['stitching-services'], "Tabei/upload_folders.py") 
    cmd_upload = f"python {cmd_path} --paths {folder_string} --country {country} --machine Savio"
    command = f"{cmd_activate_conda} && {cmd_upload}"

    t = TabeiClient()
    # set the decryption password as an environmnet variable
    t.execute_command(f"export SAVIO_DECRYPTION_PASSWORD='{pwd}'")
    t.send_tmux_command(f"Image Upload: {contract_alias}", command)
    exit()


    # now create the shell script
    shell_fp = generate_shell_script(contract_alias, machine, 1)


    config_fp_remote = os.path.join(cfg[machine]['config_folder'], os.path.basename(config_fp))
    shell_fp_remote = os.path.join(cfg[machine]['shells_folder'], os.path.basename(shell_fp))


    s = SavioClient()
    s.upload_files_sftp([config_fp, shell_fp], [config_fp_remote, shell_fp_remote])

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