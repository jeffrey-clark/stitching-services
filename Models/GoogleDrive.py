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
import io
import gspread
import json
import numpy as np

import Functions.utilities as u
from Functions.contract_utils import export_config_file

cfg = u.read_config()


class GoogleDriveService:
    def __init__(self):
        self._SCOPES = ['https://www.googleapis.com/auth/drive']
        self._credential_path = os.path.join(root_dir, 'config', 'google_service.json')

    def build(self):
        creds = Credentials.from_service_account_file(self._credential_path, scopes=self._SCOPES)
        service = build('drive', 'v3', credentials=creds)
        return service
    
    def folder_exists(self, name, parent_id):
        service = self.build()
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}' and '{parent_id}' in parents and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        for file in response.get('files', []):
            # Assuming folder names are unique within the parent folder.
            return file.get('id')
        return None

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

    def file_exists_in_folder(self, file_name, folder_id):
        service = self.build()
        query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id)').execute()
        for file in response.get('files', []):
            return file.get('id')
        return None

    def upload_file(self, file_name, file_path, mime_type, folder_id, overwrite=False):
        existing_file_id = self.file_exists(file_name, folder_id)
        service = self.build()

        if existing_file_id:
            if overwrite:
                # Overwrite the existing file
                media = MediaFileUpload(file_path, mimetype=mime_type)
                updated_file = service.files().update(fileId=existing_file_id, media_body=media).execute()
                print(f"File overwritten. File ID: {updated_file.get('id')}")
                return updated_file.get('id')
            else:
                print(f"File '{file_name}' already exists. Skipping upload.")
                return existing_file_id
        else:
            # File does not exist, proceed with upload
            file_metadata = {'name': file_name, 'parents': [folder_id]}
            media = MediaFileUpload(file_path, mimetype=mime_type)
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"File uploaded. File ID: {file.get('id')}")
            return file.get('id')


    def upload_folder(self, local_folder_path, drive_folder_id, overwrite=False):
        for item in os.listdir(local_folder_path):
            item_path = os.path.join(local_folder_path, item)
            if os.path.isfile(item_path):
                mime_type = 'application/pdf' if item_path.endswith('.pdf') else 'application/octet-stream'
                self.upload_file(item, item_path, mime_type, drive_folder_id, overwrite=overwrite)
            elif os.path.isdir(item_path):
                new_folder_id = self.create_folder(item, drive_folder_id)
                self.upload_folder(item_path, new_folder_id)


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
        self._credential_path = os.path.join(root_dir, 'config', 'google_service.json')
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
            'contract_name', 'alg_kwargs', 'algorithm', 'pool_workers', 'surf_workers', 
            'cropping_parameters', 'cropping_std_threshold', 'cropping_filter_sigma',
            'cropping_origin', 'cropping_mode', 'crs', 'folders', 'hessian_threshold',
            'lowe_ratio', 'min_inliers','min_matches', 'min_swath_size', 'ransac_reproj_threshold',
            'swath_break_threshold', 'swath_reproj_threshold', 'threads_per_worker',
            'subsample_swath', 'early_stopping', 'across_swath_threshold', 'response_threshold',
            'cluster_inlier_threshold', 'cluster_link_method', 'individual_link_threshold',
            'artifact_angle_threshold', 'soft_break_threshold', 'soft_individual_threshold',
            'optim_inclusion_threshold', 'n_within', 'n_across', 'n_swath_neighbors', 'retry_threshold',
            'n_iter', 'optim_lr_theta', 'optim_lr_scale', 'optim_lr_xy', 'raster_edge_size',
            'raster_edge_constraint_type'
        ]

class ConfigCollection(GoogleSheet):
    def __init__(self, spreadsheet_id, sheet_name):
        super().__init__(spreadsheet_id)
        self.sheet_name = sheet_name

        self.worksheet = self.get_worksheet(sheet_name)
        if not self.worksheet:
            print(f"Initialization failed: Worksheet '{sheet_name}' not found.")
            return

        self.initialize_columns()

    def find_contract_row(self, contract_name):
        try:
            contract_col = self.worksheet.col_values(1)
            return contract_col.index(contract_name) + 1  # +1 because spreadsheet rows are 1-indexed
        except ValueError:
            return None
        except Exception as e:
            print(f"Error finding contract row: {e}")
            raise

    def get_config_value(self, col):
        if self.row_id:
            try:
                return self.read_cell(self.worksheet, self.row_id, col)
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

    def add_contract(self, contract_name, config_data):
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
                
                # Ensure value is a string for the sheet
                if not isinstance(value, str):
                    value = str(value)

                new_row_values[index] = value

        # Insert the contract name at the appropriate place
        if 'contract_name' in column_headers:
            contract_name_index = column_headers.index('contract_name')
            new_row_values[contract_name_index] = contract_name

        # Add the new row to the worksheet
        self.worksheet.append_row(new_row_values)
        print(f"Contract '{contract_name}' added.")


    def export_config(self, contract_name, machine_name):
        # Retrieve configuration data for the specified contract
        row_id = self.find_contract_row(contract_name)
        if row_id is None:
            print(f"Contract '{contract_name}' not found.")
            return

        # Assuming the first row contains column headers
        column_headers = self.worksheet.row_values(1)
        contract_data = self.worksheet.row_values(row_id)

        # Convert row data to a dictionary
        config_data = {column_headers[i]: contract_data[i] for i in range(len(column_headers))}

        # Export the config file
        return export_config_file(contract_name, config_data, machine_name)






if __name__ == "__main__":


    # # download the data overview file
    # file_id = cfg['google_drive']['data_overview']['id']
    # local_path = cfg['google_drive']['data_overview']['path']
    # g = GoogleDriveService()
    # g.download_file(file_id, local_path)

    # work with the config files google sheet
    spreadsheet_id = cfg['google_drive']['config_files']['id']
    sheet_name = 'config'  # Replace with your actual sheet name

    all_configs = ConfigCollection(spreadsheet_id, sheet_name)

    # # Example: Get value from column 2
    # value = config.get_config_value(2)
    # print(value)

    # # Example: Set value in column 2
    # config.set_config_value(2, 'New Value')