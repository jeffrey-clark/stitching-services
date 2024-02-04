from Models.Contract import Country
from Models.GoogleDrive import ConfigCollection
from Functions.contract_utils import generate_default_config_data
import Functions.utilities as u
from Functions.shell_utils import generate_shell_script
from Models.Savio import SavioClient


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
    config_db.export_config(contract_alias, machine)


    # upload the files from Tabei to Savio


    # now create the shell script
    generate_shell_script(contract_alias)


    # upload config and shell to machine
    s = SavioClient()
    # s.upload_files_sftp()


    # send execution command






if __name__ == "__main__":
    machine = "savio"
    country = "Nigeria"
    contract_name = "NCAP_DOS_SHELL_BP"
    main(machine, country, contract_name)