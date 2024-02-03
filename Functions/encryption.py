
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

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64
import yaml
import getpass



def generate_key_from_password(password, salt):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
    return key

def encrypt_message(msg, password):
    salt = os.urandom(16)  # Generate a new salt for each encryption
    key = generate_key_from_password(password, salt)
    fernet = Fernet(key)
    encMsg = fernet.encrypt(msg.encode())
    return salt + encMsg  # Prepend salt to encrypted message

def decrypt_message(encMsg, password=None):
    if password is None:
        password = getpass.getpass("  Enter password here: ")
    salt = encMsg[:16]  # Extract the salt from the beginning
    key = generate_key_from_password(password, salt)
    fernet = Fernet(key)
    msg = fernet.decrypt(encMsg[16:]).decode()  # Decrypt the message without the salt
    return msg

if __name__ == "__main__":
    
    import Functions.utilities as u

    cfg = u.read_config()

    # SET THE ENCRYPTED 
    print("First we will enter your true savio credentials:")
    pin = getpass.getpass("  Enter your savio pin: ")
    secret_key = getpass.getpass("  Enter your secret key: ")
    print("\nNow encrypt these details with a password")
    password = getpass.getpass("  Enter password here: ")

    pin_encrypted = encrypt_message(pin, password)
    secret_key_encrypted = encrypt_message(secret_key, password)

    # update the config file
    if cfg.get('SAVIO_ENCRYPTED') is None:
        cfg['SAVIO_ENCRYPTED'] = {}
    cfg['SAVIO_ENCRYPTED']['PIN'] = pin_encrypted
    cfg['SAVIO_ENCRYPTED']['HOTP'] = secret_key_encrypted

    # Write the updated data back to the file
    with open(os.path.join(root_dir, "Config", "config.yml"), 'w') as file:
        yaml.dump(cfg, file)
