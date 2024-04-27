
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

import argparse
import pandas as pd
import Functions.utilities as u
from tqdm import tqdm

cfg = u.read_config()


def create_symlinks(symlinks_fp, machine):

    symlink_db = pd.read_excel(symlinks_fp)
    # Convert and zero-pad 'folder_id' and 'file_id' to a width of 4, padding with zeros on the left
    symlink_db['folder_id'] = symlink_db['folder_id'].astype(str).str.pad(width=4, side='left', fillchar='0')
    symlink_db['file_id'] = symlink_db['file_id'].astype(str).str.pad(width=4, side='left', fillchar='0')
    
    if machine.lower() == "savio":
        
        for i, row in symlink_db.iterrows():
            img_fp = os.path.join(cfg['savio']['images_folder'], row['country'], row['foldername'], row['filename'])
            link_folder = os.path.join(cfg['savio']['symlinks_folder'], row['country'], f"{row['contract_alias']}_{row['folder_id']}")
            os.makedirs(link_folder, exist_ok=True)
            link_fp = os.path.join(link_folder, f"{row['contract_alias']}_{row['folder_id']}_{row['file_id']}.tif")

            # Check if the symlink already exists and delete if yes
            if os.path.islink(link_fp):
                os.unlink(link_fp)  # Unlink/remove the existing symlink

            os.symlink(img_fp, link_fp)

    elif machine.lower() == "tabei":

        for i, row in tqdm(symlink_db.iterrows(), total=symlink_db.shape[0]):
            img_fp = row['path']
            link_folder = os.path.join(cfg['tabei']['symlinks_folder'], row['country'], f"{row['contract_alias']}_{row['folder_id']}")
            os.makedirs(link_folder, exist_ok=True)
            link_fp = os.path.join(link_folder, f"{row['contract_alias']}_{row['folder_id']}_{row['file_id']}.tif")

            # Check if the symlink already exists and delete if yes
            if os.path.islink(link_fp):
                os.unlink(link_fp)  # Unlink/remove the existing symlink

            os.symlink(img_fp, link_fp)

    else:
        raise NotImplementedError(f"Machine {machine} not implemented")
    
    # for target, link in zip(targets, links):
    #     os.symlink(target, link)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create symlinks on a machine")
    parser.add_argument('--symlinks_fp', type=str, help='Filepath to symlinks db', required=True)
    parser.add_argument('--machine', type=str, help='Machine name', required=True)

    args = parser.parse_args()
    create_symlinks(args.symlinks_fp, args.machine)

