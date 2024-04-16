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
import shutil
import Functions.utilities as u

cfg = u.read_config()



def move_swaths(contract_alias, machine):
    if machine.lower() == "savio":
        # skip savio for now
        pass
    elif machine.lower() == "google_vm":
        bucket_home = cfg['docker_paths']['bucket']

        # Define the paths
        results = os.path.join(bucket_home, "results", contract_alias)
        swaths = os.path.join(results, "swaths")
        swaths_geojson = os.path.join(swaths, "geojson")

        # ensure that the folders exist
        os.makedirs(swaths_geojson, exist_ok=True)

        # Function to get detailed file info
        def get_file_details(path):
            return {
                'path': path,
                'size': os.path.getsize(path),
                'creation_time': os.path.getctime(path)
            }

        # List files in results directory
        results_files = [get_file_details(os.path.join(results, f)) for f in os.listdir(results) if "swath" in f and (f.endswith(".tif") or f.endswith('.geojson'))]

        # List files in swaths directory
        try:
            swath_files = {f: get_file_details(os.path.join(swaths, f)) for f in os.listdir(swaths)}
        except FileNotFoundError:
            swath_files = {}

        # Move or replace files
        for result_file in results_files:
            result_filename = os.path.basename(result_file['path'])
            dest = swaths_geojson if "geojson" in result_filename else swaths
            dest_file_path = os.path.join(dest, result_filename)

            file_moved = False
            if result_filename in swath_files:
                if result_file['size'] != swath_files[result_filename]['size']:
                    print(f"Replacing {result_filename} in swaths...")
                    shutil.move(result_file['path'], dest_file_path)
                    file_moved = True
            else:
                print(f"Moving {result_filename} to swaths...")
                shutil.move(result_file['path'], dest_file_path)
                file_moved = True

            # Check if the file was not moved and delete it if it still exists
            if not file_moved and os.path.exists(result_file['path']):
                print(f"Deleting {result_filename} as it was not moved...")
                os.remove(result_file['path'])


def move_files_by_pattern(source_dir, dest_dir, pattern):
    """
    Move files from source_dir to dest_dir based on a regex pattern.
    
    Parameters:
    - source_dir: The directory to search for files.
    - dest_dir: The destination directory where matching files will be moved.
    - pattern: The regex pattern to match filenames.
    """
    
    # Ensure the destination directory exists
    os.makedirs(dest_dir, exist_ok=True)
    
    # Compile the regex pattern for filename matching
    compiled_pattern = re.compile(pattern, re.CASEIGNORE)
    
    # List all files in the source directory
    for filename in os.listdir(source_dir):
        if compiled_pattern.match(filename):
            # Construct full paths
            source_path = os.path.join(source_dir, filename)
            dest_path = os.path.join(dest_dir, filename)
            
            # Move file
            print(f"Moving {filename} to {dest_dir}...")
            shutil.move(source_path, dest_path)


def main(contract_alias, machine, type):

    if machine.lower() == "google_vm"
        bucket_home = cfg['docker_paths']['bucket']
        results = os.path.join(bucket_home, "results", contract_alias)

    # Define the paths
    
    swaths = os.path.join(results, "swaths")
    swaths_geojson = os.path.join(swaths, "geojson")


    if type.lower() in ["swath", "swaths"]:
        move_swaths(contract_alias, machine)
    elif type.lower() in ["cluster", "clusters"]:
        # first we move the tif files
        destination = os.path.join(results, "clusters")
        regex_pattern = r"cluster.*\.tif$"  # Adjust the pattern as needed
        move_files_by_pattern(results, destination, regex_pattern)
        # then move the geojson files
        destination = os.path.join(results, "clusters/geojson")
        regex_pattern = r"cluster.*\.geojson$"  # Adjust the pattern as needed
        move_files_by_pattern(results, destination, regex_pattern)
    else:
        raise ValueError(f"Invalid type: {type}. Expected 'swath', 'swaths', or 'clusters'.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move images to folder in results folder.")
    parser.add_argument('--contract_alias', type=str, help='Contract alias', required=True)
    parser.add_argument('--machine', type=str, help='Machine', required=True)
    parser.add_argument('--type', type=str, help='Type of operation (swath, swaths, clusters)', required=True)
    
    args = parser.parse_args()
    
    print("args are:", args)

    main(args.contract_alias, args.machine, args.type)



    ## What to run in the command line?
    #
    #   bash VM/run.sh <filepath> <container_name> <contract_alias> <machine> <type>
    #
    #  Example:
    #   bash VM/run.sh VM/move_files.py move_files NCAP_DOS_CAS_FI google_vm swaths
    #
