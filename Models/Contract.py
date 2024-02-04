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
import pandas as pd
from Models.GoogleDrive import GoogleDriveService, ConfigCollection  # Assuming you have a separate model for Google Drive
import Functions.utilities as u
from Functions.contract_utils import generate_default_config_data, export_config_file

cfg = u.read_config()
do = cfg['google_drive']['data_overview']




# Functions for the Country class


def download_data_overview():
    g = GoogleDriveService()
    g.download_file(do['id'], do['path'])

def exists_data_overview():
    return os.path.exists(do['path'])

def fetch_data_overview(refresh):
    if refresh or not exists_data_overview():
        download_data_overview()

def get_country_sheets():
    xls = pd.ExcelFile(do['path'])
    return xls.sheet_names

def read_country_sheet(sheet_name):
    try:
        df = pd.read_excel(do['path'], sheet_name=sheet_name)
        # remove the extra index column
        df = df.drop(columns=['Unnamed: 0'])
        return df
    except Exception as e:
        print(f"Error reading data overview: {e}")
    raise e



class Country:
    def __init__(self, country_name, refresh=False):
        # First, fetch the data overview Excel file (if necessary)
        fetch_data_overview(refresh)
        
        self.name = None
        # Find the appropriate sheet based on the country input
        sheets = get_country_sheets()
        country_matches = [x for x in sheets if country_name.lower() in x.lower()]
        
        if len(country_matches) == 1:
            self.name = country_matches[0]
        elif len(country_matches) == 0:
            raise ValueError(f"No match found for the provided country name: {country_name}")
        else:
            # Raise an error if there are too many matches with that country name
            raise ValueError(f"Multiple matches found for the provided country name: {country_name}. "
                             f"Matches: {country_matches}")
        
        self.df = read_country_sheet(self.name)

        self.contract_names = []
        self.group_contracts()
        
        

    def group_contracts(self):
        # Function to find the longest common prefix
        def longest_common_prefix(strs):
            if not strs:
                return ""
            
            # Find the shortest string as the maximum length of the common prefix
            min_length = min(len(s) for s in strs)
            
            common_prefix = ""
            for i in range(min_length):
                # Take the ith character from the first string
                char = strs[0][i]

                # Check if this character is present at the same position in all strings
                if all(s[i] == char for s in strs):
                    common_prefix += char
                else:
                    break

            # Split the common prefix at the last underscore and join back
            return '_'.join(common_prefix.split('_')[:-1])

        # Initialize contract name for all rows
        self.df['contract_name'] = pd.NA

        contract_start = 0

        # Iterate through the DataFrame
        for i in range(len(self.df)):
            # Check if we've reached the end or a new contract starts
            if i == len(self.df) - 1 or pd.notna(self.df.iloc[i + 1]['n images']):
                # Get folder names for this contract
                contract_folder_names = self.df.iloc[contract_start:i + 1]['folder'].tolist()
                contract_name = longest_common_prefix(contract_folder_names)

                # Update the contract name for each row in the group
                self.df.loc[contract_start:i, 'contract_name'] = contract_name

                contract_start = i + 1
        # Return the unique contract names
        self.contract_names = self.df.contract_name.unique()

        # Populate a dictionary of Contract instances
        self.contracts = {name: Contract(self, name) for name in self.contract_names}

    def get_contract(self, contract_name):
        # Check if the contract name is in the list of contract names
        if contract_name in self.contract_names:
            return Contract(self, contract_name)
        else:
            raise ValueError(f"No contract found with the name: {contract_name}")




class Contract:
    def __init__(self, country, contract_name):
        # If 'country' is a string, create a Country object, otherwise use it directly
        if isinstance(country, str):
            self.country = Country(country)
        elif isinstance(country, Country):
            self.country = country
        else:
            raise ValueError("Invalid country parameter. Must be a string or Country object.")

        self.name = contract_name
        self.df = self.get_contract_data()



    def get_contract_data(self):
        # Filter the country DataFrame for rows that belong to this contract
        return self.country.df[self.country.df['contract_name'] == self.name]


    # def generate_config_file

    

if __name__ == "__main__":

    # Load the contract from Data Overview file.
    nigeria_contracts = Country('Nigeria', refresh=False)
    my_contract = nigeria_contracts.get_contract(nigeria_contracts.contract_names[0])
    contract_name = my_contract.name
    contract_data = my_contract.df


    # now lets upload a config file to our config Google Sheet if it does not exist yet
    spreadsheet_id = cfg['google_drive']['config_files']['id']
    all_configs = ConfigCollection(spreadsheet_id, 'config')

    config_data = generate_default_config_data(contract_data)
    all_configs.add_contract(contract_name, config_data)
    
    # expor the contract for savio
    all_configs.export_config(contract_name, "savio")

