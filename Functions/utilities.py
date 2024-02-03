
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

from Functions.TOTP import get_totp_token
from Functions.encryption import decrypt_message
import yaml

def read_config():
    # Define the file path
    file_path = f"{root_dir}/Config/config.yml"

    # Initialize an empty dictionary
    cfg = {}

    # Read configuration
    with open(file_path, 'r') as file:
        # Load the YAML file
        loaded_cfg = yaml.safe_load(file)

        # Check if the loaded configuration is not None
        if loaded_cfg is not None:
            cfg = loaded_cfg

    return cfg




def get_savio_password(pwd):
    cfg = read_config()
    savio_pin_enc = cfg.get('SAVIO_ENCRYPTED').get("PIN")
    if savio_pin_enc is None:
        raise ValueError("Missing encrypted Savio PIN. Set it by running the encryption.py script")
    savio_hotp_enc = cfg.get('SAVIO_ENCRYPTED').get("HOTP")
    if savio_hotp_enc is None:
        raise ValueError("Missing encrypted Savio HOTP. Set it by running the encryption.py script")
    
    savio_pin = decrypt_message(savio_pin_enc, pwd)
    savio_hotp = decrypt_message(savio_hotp_enc, pwd)
    p = f"{savio_pin}{get_totp_token(savio_hotp)}"
    return p