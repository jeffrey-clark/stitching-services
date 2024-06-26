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

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from oauth2client.service_account import ServiceAccountCredentials
import yaml

import io
import gspread
import json
import numpy as np
import pandas as pd

import Functions.utilities as u
from Functions.contract_utils import export_config_file, deserialize_from_google_sheet, serialize_for_google_sheet

cfg = u.read_config()


class GoogleDriveService:
    def __init__(self, auth_method):
        self.load_config()
        self._SCOPES = ['https://www.googleapis.com/auth/drive']
        self.auth_method = auth_method
    
    def load_config(self):
        with open(f"{root_dir}/Config/config.yml", 'r') as file:
            cfg = yaml.safe_load(file)
        self.service_account_cred_path = os.path.join(root_dir, cfg['google_drive'].get('service_account_cred_path'))
        self.personal_account_cred_path = os.path.join(root_dir, cfg['google_drive'].get('personal_account_cred_path'))
        self.personal_account_token_path = os.path.join(root_dir, cfg['google_drive'].get('personal_account_token_path'))

    def build(self):
        if self.auth_method == 'service_account':
            return self.build_service_account()
        elif self.auth_method == 'personal_account':
            return self.build_personal_account()

    def build_service_account(self):
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.service_account_cred_path, self._SCOPES)
        return build('drive', 'v3', credentials=creds)

    def build_personal_account(self):
        creds = None
        if os.path.exists(self.personal_account_token_path):
            with open(self.personal_account_token_path, 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.personal_account_cred_path, self._SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open(self.personal_account_token_path, 'wb') as token:
                pickle.dump(creds, token)

        return build('drive', 'v3', credentials=creds)

    def folder_exists(self, name, parent_id):
        service = self.build()
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        for file in response.get('files', []):
            # Assuming folder names are unique within the parent folder.
            return file.get('id')
        return None

    def list_items_owned_by_service_account(self, folder_id=None):
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.service_account_cred_path, self._SCOPES)
        service = build('drive', 'v3', credentials=creds)
        service_account_email = creds.service_account_email

        query = f"'{service_account_email}' in owners"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        results = service.files().list(q=query, fields="files(id, name, mimeType, owners)").execute()
        items = results.get('files', [])
        for item in items:
            owners = [owner['emailAddress'] for owner in item.get('owners', [])]
            print(f"{'Folder' if item['mimeType'] == 'application/vnd.google-apps.folder' else 'File'}: {item['name']} (ID: {item['id']}, Owners: {owners})")
        return items


    def delete_item(self, item_id):
        service = self.build()
        item = service.files().get(fileId=item_id, fields='mimeType').execute()
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            self._delete_contents_recursive(service, item_id)
        service.files().delete(fileId=item_id).execute()
        print(f"Deleted item with ID: {item_id}")

    def _delete_contents_recursive(self, service, folder_id):
        query = f"'{folder_id}' in parents"
        results = service.files().list(q=query, fields="files(id, mimeType)").execute()
        items = results.get('files', [])

        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                self._delete_contents_recursive(service, item['id'])
            service.files().delete(fileId=item['id']).execute()
            print(f"Deleted item: {item['id']}")

    def create_folder(self, name, parent_id):
        existing_folder_id = self.folder_exists(name, parent_id)
        if existing_folder_id:
            return existing_folder_id

        service = self.build()
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

    def file_exists(self, file_name, folder_id):
        service = self.build()
        query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id)').execute()
        for file in response.get('files', []):
            return file.get('id')
        return None

    def upload_file(self, file_name, file_path, mime_type, folder_id, overwrite=False):
        existing_file_id = self.file_exists(file_name, folder_id)
        service = self.build()

        chunksize = 1000 * 1024 * 1024  # 1 Gigabyte
        
        if existing_file_id:
            if overwrite:
                # Overwrite the existing file

                media = MediaFileUpload(file_path, mimetype=mime_type, chunksize=chunksize, resumable=True)
                request = service.files().update(fileId=existing_file_id, media_body=media)
                response = None
                while response is None:
                    status, response = request.next_chunk()
                    if status:
                        print(f"Upload progress: {int(status.progress() * 100)}%")
                print(f"File overwritten. File ID: {response.get('id')}")
                return response.get('id')
            else:
                print(f"File '{file_name}' already exists. Skipping upload.")
                return existing_file_id
        else:
            # File does not exist, proceed with upload
            file_metadata = {'name': file_name, 'parents': [folder_id]}
            media = MediaFileUpload(file_path, mimetype=mime_type, chunksize=chunksize, resumable=True)
            request = service.files().create(body=file_metadata, media_body=media, fields='id')
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    print(f"Upload progress: {int(status.progress() * 100)}%")
            print(f"File uploaded. File ID: {response.get('id')}")
            return response.get('id')


    def upload_folder(self, local_folder_path, drive_folder_id, skip=[], overwrite=False):
        for item in os.listdir(local_folder_path):
            item_path = os.path.join(local_folder_path, item)

            # Check if item should be skipped
            if item in skip:
                print(f"Skipping: {item}")
                continue

            if os.path.isfile(item_path):
                mime_type = 'application/pdf' if item_path.endswith('.pdf') else 'application/octet-stream'
                self.upload_file(item, item_path, mime_type, drive_folder_id, overwrite=overwrite)
            elif os.path.isdir(item_path):
                new_folder_id = self.create_folder(item, drive_folder_id)
                self.upload_folder(item_path, new_folder_id, skip, overwrite)


    def download_file(self, file_id, destination_path):
        """
        Downloads a file from Google Drive specified by the file_id to the given destination_path.
        """
        service = self.build()
        request = service.files().get_media(fileId=file_id)
        
        with open(destination_path, 'wb') as file_handle:
            downloader = MediaIoBaseDownload(file_handle, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}%.")



class GoogleSheet:
    def __init__(self, spreadsheet_id):
        self._SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self._credential_path = os.path.join(root_dir, 'Config', 'google_service.json')
        self.spreadsheet_id = spreadsheet_id
        self.client = self.authenticate()

    def authenticate(self):
        try:
            creds = Credentials.from_service_account_file(self._credential_path, scopes=self._SCOPES)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"Error in authentication: {e}")
            raise

    def get_worksheet(self, sheet_name):
        try:
            return self.client.open_by_key(self.spreadsheet_id).worksheet(sheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Spreadsheet with ID {self.spreadsheet_id} not found.")
            return None
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found in the spreadsheet.")
            return None
        except Exception as e:
            print(f"Unexpected error accessing worksheet: {e}")
            return None

    def read_cell(self, worksheet, row, col):
        try:
            return worksheet.cell(row, col).value
        except gspread.exceptions.CellNotFound:
            print(f"Cell at row {row}, column {col} not found.")
            raise

    def update_cell(self, worksheet, row, col, value):
        try:
            worksheet.update_cell(row, col, value)
        except Exception as e:
            print(f"Error updating cell at row {row}, column {col}: {e}")
            raise



config_columns = [
            'contract_name', 'machine', 'alg_kwargs', 'algorithm', 'pool_workers', 'surf_workers', 'n_workers' 
            'symlink', 'auto_crop', 'num_loc', 
            'cropping_parameters', 'cropping_std_threshold', 'cropping_filter_sigma',
            'cropping_origin', 'cropping_mode', 'crs', 'folders', 'hessian_threshold',
            'lowe_ratio', 'min_inliers','min_matches', 'min_swath_size', 'ransac_reproj_threshold',
            'swath_break_threshold', 'swath_reproj_threshold', 'threads_per_worker',
            'subsample_swath', 'early_stopping', 'across_swath_threshold', 'response_threshold',
            'cluster_inlier_threshold', 'cluster_link_method', 'individual_link_threshold',
            'artifact_angle_threshold', 'soft_break_threshold', 'soft_individual_threshold',
            'optim_inclusion_threshold', 'inlier_threshold', 'suspect_artifacts',	'strict_inlier_threshold',	
            'optim_inlier_threshold', 'n_within', 'n_across', 'n_swath_neighbors', 'retry_threshold',
            'n_iter', 'optim_lr_theta', 'optim_lr_scale', 'optim_lr_xy', 'raster_edge_size',
            'raster_edge_constraint_type', 'collection_regex'
        ]

class ConfigSheet(GoogleSheet):
    def __init__(self, spreadsheet_id, sheet_name):
        super().__init__(spreadsheet_id)
        self.sheet_name = sheet_name

        self.worksheet = self.get_worksheet(sheet_name)
        if not self.worksheet:
            print(f"Initialization failed: Worksheet '{sheet_name}' not found.")
            return

        self.initialize_columns()

    def find_contract_row(self, contract_name, machine):
        try:
            # Get all values in the worksheet (assuming the first row contains headers)
            all_values = self.worksheet.get_all_values()

            # Check if the first row contains 'contract_name' and 'machine' columns
            if 'contract_name' in all_values[0] and 'machine' in all_values[0]:
                contract_name_index = all_values[0].index('contract_name')
                machine_index = all_values[0].index('machine')

                # Iterate over each row (starting from the second row)
                for i, row in enumerate(all_values[1:], start=2):  # start=2 for 1-indexed row numbers
                    # Check if both 'contract_name' and 'machine' match
                    if len(row) > max(contract_name_index, machine_index) and \
                            row[contract_name_index] == contract_name and row[machine_index] == machine:
                        return i  # Row number where both match
            return None

        except Exception as e:
            print(f"Error finding contract row: {e}")
            raise

    def get_config_value(self, col):
        if self.row_id:
            try:
                value = self.read_cell(self.worksheet, self.row_id, col)
                return deserialize_from_google_sheet(value)
            except Exception as e:
                print(f"Error reading cell: {e}")
                raise
        else:
            print(f"Contract row not found.")
            return None

    def set_config_value(self, col, value):
        if self.row_id:
            try:
                self.update_cell(self.worksheet, self.row_id, col, value)
            except Exception as e:
                print(f"Error updating cell: {e}")
                raise
        else:
            print(f"Contract row not found.")
            return None

    def check_sheet_integrity(self):
        if not self.worksheet:
            print(f"Worksheet not initialized.")
            return False

        actual_columns = self.worksheet.row_values(1)
        if actual_columns != config_columns:
            print(f"Sheet structure does not match expected configuration. Found: {actual_columns}")
            return False
        return True

    def initialize_columns(self):
        # Check if the first row is empty (assuming if first cell is empty, the row is empty)
        if not self.worksheet.cell(1, 1).value:
            # Populate the first row with config_columns
            cell_list = self.worksheet.range(1, 1, 1, len(config_columns))
            for i, cell in enumerate(cell_list):
                cell.value = config_columns[i]
            self.worksheet.update_cells(cell_list)
            print("Column initialization complete.")
        else:
            # print("Sheet already has data. Column initialization skipped.")
            pass

    def contract_exists(self, contract_name):
        try:
            contract_col = self.worksheet.col_values(1)
            return contract_name in contract_col
        except Exception as e:
            print(f"Error checking for existing contract: {e}")
            raise

    def add_contract(self, contract_name, machine, config_data):
        if self.contract_exists(contract_name):
            print(f"Contract '{contract_name}' already exists. Aborting addition.")
            return

        # Get the column headers from the first row
        column_headers = self.worksheet.row_values(1)

        # Create a list to store the values for the new row
        new_row_values = [''] * len(column_headers)

        # Fill in the values based on the column headers
        for key, value in config_data.items():
            if key in column_headers:
                index = column_headers.index(key)
                
                # Use the serialize_for_google_sheet function
                serialized_value = serialize_for_google_sheet(value)

                new_row_values[index] = serialized_value

        # Insert the contract name and machine at the appropriate place
        extra_cols = {'contract_name': contract_name, 'machine': machine}

        for key, value in extra_cols.items():
            if key in column_headers:
                index = column_headers.index(key)
                new_row_values[index] = value

        # Add the new row to the worksheet
        self.worksheet.append_row(new_row_values)
        print(f"Contract '{contract_name}' added.")


    def get_config(self, contract_name, machine):
        # Retrieve configuration data for the specified contract
        row_id = self.find_contract_row(contract_name, machine)
        if row_id is None:
            print(f"Contract '{contract_name}' not found.")
            return

        # Assuming the first row contains column headers
        column_headers = self.worksheet.row_values(1)
        contract_data = self.worksheet.row_values(row_id)

        # Deserialize and convert row data to a dictionary
        config_data = {}
        for i, key in enumerate(column_headers):
            try:
                value = deserialize_from_google_sheet(contract_data[i])
            except:
                value = None
            # Explicitly convert 'True'/'False' strings to boolean values
            if value == 'True':
                value = True
            elif value == 'False':
                value = False
            config_data[key] = value

        return config_data
    

    # def export_config(self, contract_name, country, machine_name, symlinks, export_machine_name=None):
    #     config_data = self.get_config(contract_name, machine_name)
    #     if export_machine_name is None:
    #         export_machine_name = machine_name
    #     return export_config_file(contract_name, country, config_data, export_machine_name, symlinks)

    def export_config(self, contract_status, country, export_machine_name=None):
        status = contract_status.data
        machine = status['machine']
        contract_alias = status['contract_name']

        config_data = self.get_config(contract_alias, machine)
        if export_machine_name is None:
            export_machine_name = machine

        return export_config_file(contract_status, country, config_data, export_machine_name)


status_columns = ['contract_name', 'code', 'machine', 'user', 'image_upload', 'symlinks', 'regex_test', 'thumbnails', 'crop_params',
                  'init_and_crop', 'download_cropping_sample', 'featurize', 'swath_breaks', 'rasterize_swaths', 
                  'download_swaths', 'prepare_swaths', 'export_georef_swaths', 'upload_archive_swaths', 'stitch_across', 
                  'initialize_graph', 'rasterize_clusters', 'download_clusters_1', 'new_neighbors', 'export_georef']

class StatusSheet(GoogleSheet):
    def __init__(self, spreadsheet_id, sheet_name):
        super().__init__(spreadsheet_id)
        self.sheet_name = sheet_name
        self.worksheet = self.get_worksheet(sheet_name)
        if not self.worksheet:
            print(f"Initialization failed: Worksheet '{sheet_name}' not found.")
            return

        self.columns = {}  # This will store column names and their indexes
        self.initialize_columns()

    def initialize_columns(self):
        # Populate column headers into a dictionary for easy access
        headers = self.worksheet.row_values(1)
        if not headers[0]:  # Assume if the first cell is empty, the row is empty
            # Populate the first row with status_columns
            headers = status_columns  # Assuming status_columns is defined
            cell_list = self.worksheet.range(1, 1, 1, len(headers))
            for i, cell in enumerate(cell_list):
                cell.value = headers[i]
            self.worksheet.update_cells(cell_list)
            print("Column initialization complete.")
        else:
            # Map headers to their index positions
            self.columns = {name: idx for idx, name in enumerate(headers)}
            # print("Column headers retrieved:", self.columns)

    def find_contract_row(self, contract_name, machine, user):
        try:
            data = self.worksheet.get_all_values()
            contract_idx = self.columns['contract_name']
            machine_idx = self.columns['machine']
            user_idx = self.columns['user']
            for idx, row in enumerate(data):
                if row[contract_idx] == contract_name and row[machine_idx] == machine and row[user_idx] == user:
                    return idx + 1  # +1 because spreadsheet rows are 1-indexed
            return None
        except Exception as e:
            print(f"Error finding contract row: {e}")
            raise

    def contract_exists(self, contract_name, machine, user):
        return self.find_contract_row(contract_name, machine, user) is not None

    def add_contract(self, contract_name, machine, user, contract_code=None):
        if self.contract_exists(contract_name, machine, user):
            print(f"Contract '{contract_name}' already exists. Not adding.")
            return
            
        new_row_values = [contract_name, contract_code, machine, user] + [''] * (len(self.columns) - 4)
        self.worksheet.append_row(new_row_values)
        print(f"Contract '{contract_name}' added with machine '{machine}' and user '{user}'.")

    def update_status(self, contract_name, machine, user, column_name, status):
        row_index = self.find_contract_row(contract_name, machine, user)
        
        if row_index is None:
            self.add_contract(contract_name, machine, user)
            row_index = self.find_contract_row(contract_name, machine, user)

        try:
            col_index = self.columns[column_name] + 1  # +1 for 1-indexed columns
            self.update_cell(self.worksheet, row_index, col_index, status)
            print(f"Status '{status}' updated for '{contract_name}' in column '{column_name}'.")
        except KeyError:
            print(f"Column '{column_name}' not found.")
        except Exception as e:
            print(f"Error updating status: {e}")
            raise

    def update_status_multiple(self, contract_name, machine, user, status_updates):
        row_index = self.find_contract_row(contract_name, machine, user)
        
        if row_index is None:
            self.add_contract(contract_name, machine, user)
            row_index = self.find_contract_row(contract_name, machine, user)

        try:
            for column_name, status in status_updates.items():
                col_index = self.columns[column_name] + 1  # +1 for 1-indexed columns
                self.update_cell(self.worksheet, row_index, col_index, status)
            print(f"Statuses updated for '{contract_name}'.")
        except KeyError as e:
            print(f"Column not found: {e}")
        except Exception as e:
            print(f"Error updating statuses: {e}")
            raise


    def get_full_status(self, contract_name, machine, user):
        """
        Retrieves the entire status row for a given contract, machine, and user.

        :param contract_name: The contract name
        :param machine: The machine name
        :param user: The user name
        :return: A dictionary with column names as keys and the corresponding statuses as values
        """
        row_index = self.find_contract_row(contract_name, machine, user)
        if row_index is None:
            print(f"Contract '{contract_name}' not found.")
            return None

        try:
            row_values = self.worksheet.row_values(row_index)
            column_names = self.worksheet.row_values(1)
            status_dict = dict(zip(column_names, row_values))

            # Ensure all status_columns are included
            for col in status_columns:
                if col not in status_dict:
                    status_dict[col] = None

            return status_dict
        except Exception as e:
            print(f"Error retrieving status: {e}")
            return None
        

    def get_status(self, contract_name, machine, user, column_name):
        """
        Retrieves the status from a single column for a given contract, machine, and user.

        :param contract_name: The contract name
        :param machine: The machine name
        :param user: The user name
        :param column_name: The name of the column from which to retrieve the status
        :return: The status from the specified column, or None if not found
        """
        row_index = self.find_contract_row(contract_name, machine, user)
        if row_index is None:
            print(f"Contract '{contract_name}' not found.")
            return None

        try:
            if column_name not in self.columns:
                print(f"Column '{column_name}' not found.")
                return None

            col_index = self.columns[column_name] + 1  # +1 for 1-indexed columns
            return self.worksheet.cell(row_index, col_index).value
        except Exception as e:
            print(f"Error retrieving status from column '{column_name}': {e}")
            return None




class Status:
    def __init__(self, status_sheet, contract_alias, machine, user, country):
        self.status_sheet = status_sheet
        self.contract_alias = contract_alias
        self.machine = machine
        self.user = user
        self.country=country
        self.data = self.refresh_status()
        self.symlink_folders = self.load_symlinks()

    def refresh_status(self):
        """
        Refreshes the status data from the StatusSheet.
        """
        return self.status_sheet.get_full_status(self.contract_alias, self.machine, self.user)

    def get_status(self, column_name=None):
        """
        Gets the status for the specified column or the entire status if no column is specified.
        """
        if column_name:
            return self.status_sheet.get_status(self.contract_alias, self.machine, self.user, column_name)
        else:
            return self.refresh_status()

    def update_status(self, column_name, status):
        """
        Updates the status for a specified column.
        """
        self.status_sheet.update_status(self.contract_alias, self.machine, self.user, column_name, status)
        self.refresh_status()  # Refresh the status data after update

    def update_status_multiple(self, status_updates):
        """
        Updates multiple statuses.
        """
        self.status_sheet.update_status_multiple(self.contract_alias, self.machine, self.user, status_updates)
        self.refresh_status()  # Refresh the status data after updates

    def load_symlinks(self, machine='tabei'):
        symlinks_db_fp = f"{root_dir}/Files/symlink_keys/{self.contract_alias}_symlinks_key.xlsx"
        if os.path.isfile(symlinks_db_fp):
            symlink_db = pd.read_excel(symlinks_db_fp)
            # Convert and zero-pad 'folder_id' and 'file_id' to a width of 4, padding with zeros on the left
            symlink_db['folder_id'] = symlink_db['folder_id'].astype(str).str.pad(width=4, side='left', fillchar='0')
            symlink_db['file_id'] = symlink_db['file_id'].astype(str).str.pad(width=4, side='left', fillchar='0')
            
            unique_series = pd.Series(symlink_db['folder_id'].unique())
            folder_ids = unique_series.sort_values(ascending=True).values

            if machine == 'tabei':
                link_folders = [os.path.join(cfg['tabei']['symlinks_folder'], self.country, f"{self.contract_alias}_{x}") for x in folder_ids]
            elif machine == "savio":
                link_folders = [os.path.join(cfg['savio']['symlinks_folder'], self.country, f"{self.contract_alias}_{x}") for x in folder_ids]
            else:
                raise NotImplementedError(f"Symlinks for Machine '{machine}' not implemented.")
            
            return link_folders
        else:
            return None




if __name__ == "__main__":


    # # download the data overview file
    # file_id = cfg['google_drive']['data_overview']['id']
    # local_path = cfg['google_drive']['data_overview']['path']
    # g = GoogleDriveService()
    # g.download_file(file_id, local_path)

    # work with the config files google sheet
    spreadsheet_id = cfg['google_drive']['config_files']['id']
    
    sheet_name = 'config'  # Replace with your actual sheet name
    all_configs = ConfigSheet(spreadsheet_id, sheet_name)


    sheet_name = 'status'
    status_sheet = StatusSheet(spreadsheet_id, sheet_name)
    status = Status(status_sheet, 'my contract', 'savio', 'jeffreyclark')
    print(status.get_status())

    # # Example: Get value from column 2
    # value = config.get_config_value(2)
    # print(value)

    # # Example: Set value in column 2
    # config.set_config_value(2, 'New Value')