import os
import Functions.utilities as u
import inspect


cfg = u.read_config()


def _savio_base(contract_status, slurm_suffix, time_limit):
    contract_alias = contract_status.contract_alias
    username = contract_status.user  # Assuming 'user' attribute exists within contract_status
    savio_cfg = cfg['savio']  # Assuming cfg is a global variable that includes configuration for Savio

    config_path = os.path.join(savio_cfg['config_folder'], f"{contract_alias}.yml")
    resources_folder = savio_cfg['resources_folder']
    stitching_services_sif = os.path.join(resources_folder, "stitching-services.sif")
    
    surf_sif = "/global/home/groups/co_laika/ahp/surf.sif"
    crop_sif = os.path.join(resources_folder, "crop.sif")
    
    ahp_repo = os.path.join(savio_cfg['repos'], "aerial-history-stitching")
    services_repo = os.path.join(savio_cfg['repos'], "stitching-services")


    shell_script_base = f"""#!/bin/bash
# Job name:
#SBATCH --job-name={contract_alias}_{slurm_suffix}
#
# Account:
#SBATCH --account=co_laika
#
# Partition:
#SBATCH --partition=savio3
#
# Wall clock limit:
#SBATCH --time={time_limit}
#
#
run_singularity() {{
    local stage="$1"
    shift  # Shift the first argument so that "$@" can be used for additional arguments
    
    singularity run {surf_sif} \\
        python3 {os.path.join(ahp_repo, "main.py")} \\
        --config {config_path} \\
        --stage "$stage" \\
        "$@"

    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Error: Singularity container for stage '$stage' failed with exit code $exit_code."
        exit $exit_code
    fi

}}

# Function to update the status in the status Google Sheet using Singularity
update_status() {{
    local column="$1"
    local value="$2"

    singularity run {stitching_services_sif} \\
        python3 {os.path.join(services_repo, "Local/update_status.py")} \\
        --contract_alias {contract_alias} \\
        --country {contract_status.country} \\
        --machine savio \\
        --username {username} \\
        --column "${{column}}" \\
        --value "${{value}}"

    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Error: Container for updating status failed with exit code $exit_code."
        exit $exit_code
    fi
}}

# Function to run a stitching services script
run_stitching_services() {{
    local script_path="$1"
    shift
    singularity run {stitching_services_sif} \\
        python3 {os.path.join(services_repo, "$script_path")} \\
        "$@"

    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Error: Singularity container for '$script_path' failed with exit code $exit_code."
        exit $exit_code
    fi
}}

# Function to run a stitching services script
run_crop_container() {{
    local script_path="$1"
    shift
    singularity run {crop_sif} \\
        python3 {os.path.join(ahp_repo, "$script_path")} \\
        "$@"

    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Error: Singularity container for '$script_path' failed with exit code $exit_code."
        exit $exit_code
    fi
}}


# Placeholder for any additional common setup or functions
"""
    return shell_script_base


def _savio_create_symlinks(contract_status):
    
    contract_alias = contract_status.contract_alias
    services_repo = os.path.join(cfg['savio']['repos'], "stitching-services")


    shell_script_content = _savio_base(contract_status, "symlinks", "00:05:00") + f"""
# Run each stage
echo "Creating symlinks"

# call the create_symlinks script
run_stitching_services Local/create_symlinks.py --symlinks_fp {services_repo}/Files/symlink_keys/{contract_alias}_symlinks_key.xlsx --machine savio

update_status "symlinks" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _savio_init_and_crop(contract_status):
    
    contract_alias = contract_status.contract_alias
    savio_cfg = cfg['savio']  # Assuming cfg is a global variable that includes configuration for Savio
    config_path = os.path.join(savio_cfg['config_folder'], f"{contract_alias}.yml")

    source_folder = os.path.join(cfg['savio']['results_folder'], contract_alias)
    destination_folder = os.path.join(source_folder, "cropping_samples")

    shell_script_content = _savio_base(contract_status, "stage_1", "06:00:00") + f"""
# Run each stage
echo "starting initialize"
run_singularity "initialize"
# run_crop_container "scripts/alex_crop.py" --config {config_path} --stage "initialize"

echo "starting crop"
run_singularity "crop"
# run_crop_container "scripts/alex_crop.py" --config {config_path} --stage "crop"

echo "starting inspect-cropping"
run_singularity "inspect-cropping"
# run_crop_container "scripts/alex_crop.py" --config {config_path} --stage "inspect-cropping"

# organize the cropping samples in folder
run_stitching_services VM/move_files.py --source {source_folder} --destination {destination_folder} --pattern "cropping_sample.*\.jpg$"

update_status "init_and_crop" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _savio_featurize(contract_status):
    shell_script_content = _savio_base(contract_status, "featurize", "06:00:00") + f"""
# Run each stage
echo "starting feautrization"
run_singularity "featurize" --only-missing

update_status "featurize" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _savio_swath_breaks(contract_status):
    shell_script_content = _savio_base(contract_status, "swath_breaks", "24:00:00") + f"""
# Run each stage
echo "starting swath-breaks"
run_singularity "swath-breaks"

update_status "swath_breaks" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _savio_create_raster_swaths(contract_status):

    contract_alias = contract_status.contract_alias

    source_folder = os.path.join(cfg['savio']['results_folder'], contract_alias)
    destination_folder = os.path.join(source_folder, "swaths")
    destination_folder_geojons = os.path.join(source_folder, "swaths/geojson")

    shell_script_content = _savio_base(contract_status, "rasterize_swaths", "03:00:00") + f"""
# Create the swath rasters
echo "starting create-raster"
run_singularity "create-raster" --raster-type "swaths" 

# organize the swaths in folder
run_stitching_services VM/move_files.py --source {source_folder} --destination {destination_folder} --pattern "swath.*\.tif$"
run_stitching_services VM/move_files.py --source {source_folder} --destination {destination_folder_geojons} --pattern "swath.*\.geojson$"

# Set update to Done
update_status "rasterize_swaths" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content



def _savio_stitch_across(contract_status):
    shell_script_content = _savio_base(contract_status, "stitch_across", "48:00:00") + f"""
## Run each stage
echo "starting stitch-across"
run_singularity "stitch-across"

update_status "stitch_across" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _savio_refine_and_init_graph(contract_status):

    shell_script_content = _savio_base(contract_status, "initialize_graph", "12:00:00") + f"""
# Run each stage
echo "starting refine-links"
run_singularity "refine-links"

echo "starting initialize-graph"
run_singularity "initialize-graph"

echo "starting opt-links"
run_singularity "opt-links"

echo "starting global-opt"
run_singularity "global-opt"

update_status "initialize_graph" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content




def _savio_create_raster_clusters(contract_status, ids=None, destination_suffix=None):

    contract_alias = contract_status.contract_alias

    source_folder = os.path.join(cfg['savio']['results_folder'], contract_alias)

    if destination_suffix is None:
        foldername = "clusters"
    else:
        foldername = f"clusters_{destination_suffix}"

    destination_folder = os.path.join(source_folder, foldername, "rasters")
    destination_folder_annotated = os.path.join(source_folder,  foldername, "annotated")
    destination_folder_geojons = os.path.join(source_folder, foldername, "geojson")

    id_restriction = ""
    if ids is not None:
        if isinstance(ids, list):
            ids = [str(x) for x in ids]
            id_restriction = f"--ids {' '.join(ids)}"

    shell_script_content = _savio_base(contract_status, "rasterize_clusters", "03:00:00") + f"""
# First create the cluster rasters without annotation
echo "starting create-raster for clusters"
run_singularity "create-raster" --raster-type "clusters" {id_restriction}

# organize the clusters in folder
run_stitching_services VM/move_files.py --source {source_folder} --destination {destination_folder} --pattern "cluster.*\.tif$"
run_stitching_services VM/move_files.py --source {source_folder} --destination {destination_folder_geojons} --pattern "cluster.*\.geojson$"

# Second create the cluster rasters with graph annotation
echo "starting create-raster for clusters"
run_singularity "create-raster" --raster-type "clusters" --annotate "graph" {id_restriction}

# organize the clusters in folder
run_stitching_services VM/move_files.py --source {source_folder} --destination {destination_folder_annotated} --pattern "cluster.*\.tif$"
run_stitching_services VM/delete_files.py --source {source_folder} --pattern "cluster.*\.geojson$"

# Set update to Done
update_status "rasterize_clusters" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content




def _savio_prepare_swaths(contract_status):

    contract_alias = contract_status.contract_alias

    source_folder = os.path.join(cfg['savio']['results_folder'], contract_alias)
    destination_folder = os.path.join(source_folder, "swaths_w_raws")
    destination_folder_geojons = os.path.join(source_folder, "swaths_w_raws/geojson")

    shell_script_content = _savio_base(contract_status, "prepare_swaths", "12:00:00") + f"""
# Run each stage
echo "starting prepare-swaths"
run_singularity "create-raster-w-raws" --raster-type swaths

# organize the swaths with raws in folder
run_stitching_services VM/move_files.py --source {source_folder} --destination {destination_folder}  --pattern "swath.+_w_raws\.tif$"
run_stitching_services VM/move_files.py --source {source_folder} --destination {destination_folder_geojons} --pattern "swath.+_w_raws\.tif.geojson$"

update_status "prepare_swaths" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content



def _google_vm_base(contract_status):

    contract_alias = contract_status.contract_alias
    username = contract_status.user

    vm_paths = cfg['google_vm']['vm_paths']
    docker_paths = cfg['google_vm']['docker_paths']

    config_path = os.path.join(docker_paths['stitching_repo'], "config", f"{contract_alias}.yml")

    shell_script_base = f"""#!/usr/bin/env bash

# Function to run docker and check for errors
run_docker() {{
    local stage_name="$1"
    local container_name="{contract_alias}_${{stage_name}}"

    # Check if the container already exists
    if sudo docker ps -a --format '{{{{.Names}}}}' | grep -q "^${{container_name}}$"; then
        # Check if the container is running
        if sudo docker ps --format '{{{{.Names}}}}' | grep -q "^${{container_name}}$"; then
            # echo "Error: Container ${{container_name}} is still running."
            # exit 1

            # Stop the running container
            sudo docker stop "${{container_name}}"
            # Remove the stopped container
            sudo docker rm "${{container_name}}"
        
        else
            # Remove the stopped container
            sudo docker rm "${{container_name}}"
        fi
    fi

    # Shift arguments to exclude the first one
    shift

    sudo docker run --name "${{container_name}}" \\
        --mount type=bind,source={vm_paths['bucket']},target={docker_paths['bucket']} \\
        --mount type=bind,source={vm_paths['stitching_repo']},target={docker_paths['stitching_repo']} \\
        --mount type=bind,source={vm_paths['logs_folder']},target={docker_paths['logs_folder']} \\
        enoda/opencv_surf \\
        python3 {docker_paths['stitching_repo']}/main.py \\
        --config {config_path} \\
        --stage "${{stage_name}}" \\
        "$@"

    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Error: Container for stage ${{stage_name}} failed with exit code $exit_code."
        exit $exit_code
    fi
}}

# Function to update the status in the status Google Sheet
update_status() {{
    local column="$1"
    local value="$2"
    local container_name="{contract_alias}_update_status"

    # Check if the container already exists
    if sudo docker ps -a --format '{{{{.Names}}}}' | grep -q "^${{container_name}}$"; then
        # Check if the container is running
        if sudo docker ps --format '{{{{.Names}}}}' | grep -q "^${{container_name}}$"; then
            echo "Error: Container ${{container_name}} is still running."
            exit 1
        else
            # Remove the stopped container
            sudo docker rm "${{container_name}}"
        fi
    fi

    # Run the Docker container to execute the update status script
    sudo docker run --name "${{container_name}}" \\
        --mount type=bind,source={vm_paths['services_repo']},target={docker_paths['services_repo']} \\
        stitching-services \\
        python3 {docker_paths['services_repo']}/Local/update_status.py \\
        --contract_alias {contract_alias} \\
        --country {contract_status.country} \\
        --machine google_vm \\
        --username {username} \\
        --column "${{column}}" \\
        --value "${{value}}"

    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Error: Container for updating status failed with exit code $exit_code."
        exit $exit_code
    fi
}}
    """
    return shell_script_base



def _google_vm_init_and_crop(contract_status):
    shell_script_content = _google_vm_base(contract_status) + f"""
# Run each stage
echo "starting initialize"
run_docker "initialize"
echo "starting crop"
run_docker "crop"
echo "starting inspect-cropping"
run_docker "inspect-cropping"

update_status "init_and_crop" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_featurize(contract_status):
    shell_script_content = _google_vm_base(contract_status) + f"""
# Run each stage
echo "starting feautrization"
run_docker "featurize"

update_status "featurize" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_swath_breaks(contract_status):
    shell_script_content = _google_vm_base(contract_status) + f"""
# Run each stage
echo "starting swath-breaks"
run_docker "swath-breaks"

update_status "swath_breaks" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content



def _google_vm_stitch_across(contract_status):
    shell_script_content = _google_vm_base(contract_status) + f"""
## Run each stage
echo "starting stitch-across"
run_docker "stitch-across"

update_status "stitch_across" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_refine_and_init_graph(contract_status):

    shell_script_content = _google_vm_base(contract_status) + f"""
# Run each stage
echo "starting refine-links"
run_docker "refine-links"

echo "starting initialize-graph"
run_docker "initialize-graph"

echo "starting opt-links"
run_docker "opt-links"

echo "starting global-opt"
run_docker "global-opt"

update_status "initialize_graph" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_create_raster_swaths(contract_status):

    vm_run_shell = os.path.join(cfg['google_vm']['vm_paths']['services_repo'], "VM/run.sh")
    contract_alias = contract_status.contract_alias

    source_folder = os.path.join(cfg['google_vm']['docker_paths']['results_folder'], contract_alias)
    destination_folder = os.path.join(source_folder, "swaths")
    destination_folder_geojons = os.path.join(source_folder, "swaths/geojson")

    shell_script_content = _google_vm_base(contract_status) + f"""
# Create the swath rasters
echo "starting create-raster"
run_docker "create-raster" --raster-type "swaths" 

# organize the swaths in folder
bash {vm_run_shell} VM/move_files.py move_files --source {source_folder} --destination {destination_folder} --pattern "swath.*\.tif$"
bash {vm_run_shell} VM/move_files.py move_files --source {source_folder} --destination {destination_folder_geojons} --pattern "swath.*\.geojson$"

# Set update to Done
update_status "rasterize_swaths" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_create_raster_clusters(contract_status, ids=None, destination_suffix=None):

    vm_run_shell = os.path.join(cfg['google_vm']['vm_paths']['services_repo'], "VM/run.sh")
    contract_alias = contract_status.contract_alias

    if destination_suffix is None:
        foldername = "clusters"
    else:
        foldername = f"clusters_{destination_suffix}"

    source_folder = os.path.join(cfg['google_vm']['docker_paths']['results_folder'], contract_alias)
    destination_folder = os.path.join(source_folder, foldername, "rasters")
    destination_folder_annotated = os.path.join(source_folder,  foldername, "annotated")
    destination_folder_geojons = os.path.join(source_folder, foldername, "geojson")

    id_restriction = ""
    if ids is not None:
        if isinstance(ids, list):
            ids = [str(x) for x in ids]
            id_restriction = f"--ids {' '.join(ids)}"

    shell_script_content = _google_vm_base(contract_status) + f"""

# First create the cluster rasters without annotation
echo "starting create-raster for clusters"
run_docker "create-raster" --raster-type "clusters" {id_restriction}

# organize the clusters in folder
bash {vm_run_shell}  VM/move_files.py --source {source_folder} --destination {destination_folder} --pattern "cluster.*\.tif$"
bash {vm_run_shell}  VM/move_files.py --source {source_folder} --destination {destination_folder_geojons} --pattern "cluster.*\.geojson$"

# Second create the cluster rasters with graph annotation
echo "starting create-raster for clusters"
run_docker "create-raster" --raster-type "clusters" --annotate "graph" {id_restriction}

# organize the clusters in folder
bash {vm_run_shell}  VM/move_files.py --source {source_folder} --destination {destination_folder_annotated} --pattern "cluster.*\.tif$"
bash {vm_run_shell}  VM/delete_files.py --source {source_folder} --pattern "cluster.*\.geojson$"

# Set update to Done
update_status "rasterize_clusters" "Done"
echo "All stages completed successfully."

"""
    return shell_script_content


# def _google_vm_create_raster(status_col, contract_alias, type, annotate=None):

#     if annotate is None:
#         annotate_str = ""
#     elif annotate == "graph":
#         annotate_str = '--annotate "graph"'
#     else:
#         raise SyntaxError("Invalid annotate argument")

#     shell_script_content = _google_vm_base(contract_alias) + f"""
# # Run each stage
# echo "starting create-raster"
# run_docker "create-raster" --raster-type "{type}" {annotate_str}

# update_status "{status_col}" "Done"
# echo "All stages completed successfully."
#     """
#     return shell_script_content


def _google_vm_new_neighbors(contract_status, ids):

    contract_alias = contract_status.contract_alias
    vm_paths = cfg['google_vm']['vm_paths']
    docker_paths = cfg['google_vm']['docker_paths']
    config_path = os.path.join(docker_paths['stitching_repo'], "config", f"{contract_alias}.yml")
        
    shell_script_content = _google_vm_base(contract_status) + f"""
# Run each stage
echo "starting new neighbors for "
run_docker "new-neighbors" --ids {ids} 

echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_export_georef(contract_status, ids):

    contract_alias = contract_status.contract_alias

    docker_paths = cfg['google_vm']['docker_paths']
    config_path = os.path.join(docker_paths['stitching_repo'], "config", f"{contract_alias}.yml")

    shell_script_content = _google_vm_base(contract_status) + f"""
run_collect_and_zip() {{
    local container_name="{contract_alias}_collect_and_zip"

     # Check if the container already exists
    if sudo docker ps -a --format '{{{{.Names}}}}' | grep -q "^${{container_name}}$"; then
        # Check if the container is running
        if sudo docker ps --format '{{{{.Names}}}}' | grep -q "^${{container_name}}$"; then
            echo "Error: Container ${{container_name}} is still running."
            exit 1
        else
            # Remove the stopped container
            sudo docker rm "${{container_name}}"
        fi
    fi

    # Run the Docker container
    sudo docker run --name "${{container_name}}" \
        --mount type=bind,source=/mnt/jeffrey_stitching_bucket_2,target=/app/bucket \
        --mount type=bind,source=/home/jeffrey/repos/aerial-history-stitching,target=/home/app/aerial-history-stitching \
        --mount type=bind,source=/home/jeffrey/logs,target=/home/app/logs \
        enoda/opencv_surf \
        python3 /home/app/aerial-history-stitching/scripts/collect_and_zip_raws.py --config "{config_path}"

    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Error: Container for collect_and_zip failed with exit code $exit_code."
        exit $exit_code
    fi
}}

# Run each stage
echo "Starting reate-raster-w-raws for cluster ids {ids}"
run_docker "create-raster-w-raws" --ids {" ".join([str(x) for x in ids])}

echo "Running collect_and_zip"
run_collect_and_zip

echo "All stages completed successfully."

    """
    return shell_script_content





def _google_vm_prepare_swaths(contract_status):

    vm_run_shell = os.path.join(cfg['google_vm']['vm_paths']['services_repo'], "VM/run.sh")
    contract_alias = contract_status.contract_alias

    source_folder = os.path.join(cfg['google_vm']['docker_paths']['results_folder'], contract_alias)
    destination_folder = os.path.join(source_folder, "swaths_w_raws")
    destination_folder_geojons = os.path.join(source_folder, "swaths_w_raws/geojson")

    shell_script_content = _google_vm_base(contract_status) + f"""
# Run each stage
echo "starting feautrization"
run_docker "create-raster-w-raws" --raster-type swaths

# organize the clusters in folder
bash {vm_run_shell} VM/move_files.py move_files --source {source_folder} --destination {destination_folder} --pattern "swath.+_w_raws\.tif$"
bash {vm_run_shell} VM/move_files.py move_files --source {source_folder} --destination {destination_folder_geojons} --pattern "swath.+_w_raws\.tif.geojson$"

update_status "prepare_swaths" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content








def generate_shell_script(contract_status, shell_template_id, **kwargs):

    contract_alias = contract_status.contract_alias
    machine = contract_status.machine

    if machine.lower() == "savio":
        func_map = {
            'create_symlinks': _savio_create_symlinks,
            'initialize_and_crop': _savio_init_and_crop, 
            'featurize': _savio_featurize, 
            'swath_breaks': _savio_swath_breaks,
            'create_raster_swaths': _savio_create_raster_swaths,
            'stitch_across': _savio_stitch_across,
            'initialize_graph': _savio_refine_and_init_graph,
            'create_raster_clusters': _savio_create_raster_clusters,
            'prepare_swaths': _savio_prepare_swaths
            }
        
    elif machine.lower() == "google_vm":
        func_map = {
            'initialize_and_crop': _google_vm_init_and_crop, 
            'featurize': _google_vm_featurize,
            'swath_breaks': _google_vm_swath_breaks,
            'create_raster_swaths': _google_vm_create_raster_swaths,
            'stitch_across': _google_vm_stitch_across,
            'initialize_graph': _google_vm_refine_and_init_graph,
            'create_raster_clusters': _google_vm_create_raster_clusters,
            'new_neighbors': _google_vm_new_neighbors,
            'export_georef': _google_vm_export_georef,
            'prepare_swaths': _google_vm_prepare_swaths
            }
    else:
        raise ValueError('Specified machine is not supported')
    
    func = func_map[shell_template_id]
    func_sig = inspect.signature(func)

    # Ensure all kwargs are valid for the function
    valid_params = set(func_sig.parameters.keys())
    for kwarg in kwargs:
        if kwarg not in valid_params:
            raise ValueError(f"Invalid argument '{kwarg}' for function '{func.__name__}'")

    # Prepare arguments to be passed to the function
    func_args = {name: kwargs[name] for name in valid_params if name in kwargs}

    fp = os.path.join("Files/job_shells", machine.lower(), f"{contract_alias}_{shell_template_id}.sh")
    os.makedirs(os.path.dirname(fp), exist_ok=True)

    with open(fp, "w") as file:
        file.write(func(contract_status, **func_args))

    return fp



if __name__ == "__main__":
    pass