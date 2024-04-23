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
from PIL import Image
import random
from tqdm import tqdm
import argparse
import pandas as pd
import zipfile

# Set a limit to the maximum number of image pixels to prevent errors on very large images.
Image.MAX_IMAGE_PIXELS = None

def make_thumbnails(file_paths, output_dir):
    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    thumbnail_files = []
    image_shapes = []

    with tqdm(total=len(file_paths), desc="Creating Thumbnails") as pbar:
        for fp in file_paths:
            with Image.open(fp) as img:
                original_shape = img.size
                img.thumbnail((500, 500))  # Resize while maintaining aspect ratio
                thumbnail_shape = img.size

                output_filename = "_".join(fp.split("/")[-2:])
                output_fp = os.path.join(output_dir, f"{output_filename}.jpg")
                img.save(output_fp, 'JPEG', quality=50)

                thumbnail_files.append(output_fp)
                image_shapes.append({'original_shape': original_shape, 'thumbnail_shape': thumbnail_shape, 'file_path': fp})
                pbar.update(1)

    # Create DataFrame from image_shapes
    df = pd.DataFrame(image_shapes)

    # Zipping the thumbnails and DataFrame
    zip_filename = os.path.join(output_dir, "duplicates.zip")
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for file in thumbnail_files:
            zipf.write(file, os.path.basename(file))
        df_csv_path = os.path.join(output_dir, "image_shapes.csv")
        df.to_csv(df_csv_path, index=False)
        zipf.write(df_csv_path, os.path.basename(df_csv_path))

    print(f"Thumbnails zipped in {zip_filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create thumbnails from a list of file paths")
    parser.add_argument('--filepaths', nargs='+', help='List of file paths')
    parser.add_argument('--outputdir', type=str, help='Output directory', required=True)
    args = parser.parse_args()

    make_thumbnails(args.filepaths, args.outputdir)