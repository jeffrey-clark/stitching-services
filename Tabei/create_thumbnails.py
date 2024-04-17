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

from Models.Contract import Country
from Functions.utilities import read_config, translate_filepaths
import socket
import os 
import random
from PIL import Image
import zipfile
# Overwrite pixel limit to allow image
Image.MAX_IMAGE_PIXELS = None
from tqdm import tqdm
import argparse
import pandas as pd



cfg = read_config()


def make_thumbnails(country, contract_code):

    c = Country(country, refresh=True)
    print("contract code is:", contract_code)
    contract = c.get_contract(contract_code)
    tabei_folders = contract.df.path
    # compute all of the fps
    fps = []
    for folder in tabei_folders:
        folder = translate_filepaths(folder)
        for file in os.listdir(folder):
            if file.lower().startswith('job sheet'):
                continue
            if ((file.endswith('.jpg') or file.endswith('.tif')) and not file.startswith('._')):
                fps.append(os.path.join(folder, file))

    # random sample
    random.seed(45367) 
    if len(fps) > 100:
        sample_fps = random.sample(fps, 100)
    else:
        sample_fps = fps
    
    # make sure that the output dir exists
    contract_name = f"{country}_{contract_code}"
    output_dir = translate_filepaths(os.path.join(cfg['tabei']['thumbnails_folder'], country, contract_name))
    for d in [os.path.dirname(output_dir), output_dir]:
        if not os.path.exists(d):
            os.mkdir(d)

    thumbnail_files = []
    image_shapes = []
    with tqdm(total=len(sample_fps), desc="Creating Thumbnails") as pbar:
        for fp in sample_fps:
            with Image.open(fp) as img:
                original_shape = img.size
                # Resize while maintaining aspect ratio
                img.thumbnail((500, 500))
                thumbnail_shape = img.size

                output_fp = os.path.join(output_dir, os.path.basename(fp))
                base_name, _ = os.path.splitext(output_fp)
                output_fp_jpg = base_name + ".jpg"

                img.save(output_fp_jpg, quality=50, compression="tiff_deflate")

                thumbnail_files.append(output_fp_jpg)
                image_shapes.append({'original_shape': original_shape, 'thumbnail_shape': thumbnail_shape, 'file_path': fp})

                pbar.update(1)

    # Create DataFrame from image_shapes
    df = pd.DataFrame(image_shapes)

     # Zipping the thumbnails and DataFrame
    zip_filename = os.path.join(output_dir, "thumbnails.zip")
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for file in thumbnail_files:
            zipf.write(file, os.path.basename(file))
        # Add DataFrame as a CSV to the zip
        df_csv_path = os.path.join(output_dir, "image_shapes.csv")
        df.to_csv(df_csv_path, index=False)
        zipf.write(df_csv_path, os.path.basename(df_csv_path))

    print(f"Thumbnails zipped in {zip_filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create thumbnails from a contract")
    parser.add_argument('--country', type=str, help='Country name', required=True)
    parser.add_argument('--contract_name', type=str, help='Contract name', required=True)
    args = parser.parse_args()
    make_thumbnails(args.country, args.contract_name)

    # # If debugging use this instead
    # nigeria = Country("Nigeria", refresh=False)
    # make_thumbnails("Nigeria", nigeria.contract_names[15])
