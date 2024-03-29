
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
import socket


def load_yaml(filepath):
    # Initialize an empty dictionary
    cfg = {}

    # Read configuration
    with open(filepath, 'r') as file:
        # Load the YAML file
        loaded_cfg = yaml.safe_load(file)

        # Check if the loaded configuration is not None
        if loaded_cfg is not None:
            cfg = loaded_cfg

    return cfg


def read_config():
    return load_yaml(os.path.join(root_dir, "Config/config.yml"))

def read_savio_credentials():
    return load_yaml(os.path.join(root_dir, "Config/savio_credentials.yml"))


def get_savio_password(pwd):
    savcred = read_savio_credentials()
    savio_pin_enc = savcred.get('SAVIO_ENCRYPTED').get("PIN")
    if savio_pin_enc is None:
        raise ValueError("Missing encrypted Savio PIN. Set it by running the encryption.py script")
    savio_hotp_enc = savcred.get('SAVIO_ENCRYPTED').get("HOTP")
    if savio_hotp_enc is None:
        raise ValueError("Missing encrypted Savio HOTP. Set it by running the encryption.py script")
    
    savio_pin = decrypt_message(savio_pin_enc, pwd)
    savio_hotp = decrypt_message(savio_hotp_enc, pwd)
    p = f"{savio_pin}{get_totp_token(savio_hotp)}"
    return p


def translate_filepaths(fps):
    """ 
    Ensures that you access the correct Tabei Filepaths from Shackleton or Kupe
    """
    allowed_hostnames = ["tabei", "shackleton", "kupe"]
    hostname = socket.gethostname()
    instance_type = type(fps)
    
    if hostname not in allowed_hostnames:
        raise ValueError(f"Unsupported hostname. Supported hostnames are {', '.join(allowed_hostnames)}.")

    if isinstance(fps, str):
        fps = [fps]

    output_filepaths = []
    for fp in fps:
        if hostname == "tabei":
            output_filepaths.append(fp)
        else:
            # if the fp starts with /mnt/shackleton_shares/... change to /shares/... 
            if fp.startswith(f"/mnt/{hostname}_shares/"):
                fp = fp.replace(f"/mnt/{hostname}_shares/", "/shares/")
            # if the fp starts with /shares/... change to /mnt/tabei_shares/...
            elif fp.startswith("/shares/"):
                fp = fp.replace("/shares/", "/mnt/tabei_shares/")          
            output_filepaths.append(fp)

    return output_filepaths[0] if instance_type == str else output_filepaths