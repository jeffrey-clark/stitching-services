import os
import Functions.utilities as u
import inspect


cfg = u.read_config()


def _savio_init_and_crop(contract_alias):

    config_path = os.path.join(cfg['savio']['config_folder'], f"{contract_alias}.yml")

    shell_script_content = f"""#!/bin/bash
# Job name:
#SBATCH --job-name={contract_alias}_stage_1     
#
# Account:
#SBATCH --account=co_laika
#
# Partition:
#SBATCH --partition=savio3
#
# Wall clock limit:
#SBATCH --time=02:00:00
#
## Command(s) to run:
singularity run /global/home/groups/co_laika/ahp/surf.sif \\
    python3 /global/home/users/jeffreyclark/repos/aerial-history-stitching/main.py \\
    --config {config_path} \\
    --stage initialize 

singularity run /global/home/groups/co_laika/ahp/surf.sif \\
    python3 /global/home/users/jeffreyclark/repos/aerial-history-stitching/main.py \\
    --config {config_path} \\
    --stage crop 

singularity run /global/home/groups/co_laika/ahp/surf.sif \\
    python3 /global/home/users/jeffreyclark/repos/aerial-history-stitching/main.py \\
    --config {config_path} \\
    --stage inspect-cropping 
"""
    return shell_script_content



def _savio_featurize(contract_alias):

    config_path = os.path.join(cfg['savio']['config_folder'], f"{contract_alias}.yml")

    shell_script_content = f"""#!/bin/bash
# Job name:
#SBATCH --job-name={contract_alias}_stage_2 
#
# Account:
#SBATCH --account=co_laika
#
# Partition:
#SBATCH --partition=savio3
#
# Wall clock limit:
#SBATCH --time=24:00:00
#
## Command(s) to run:
singularity run /global/home/groups/co_laika/ahp/surf.sif \\
    python3 /global/home/users/jeffreyclark/repos/aerial-history-stitching/main.py \\
    --config {config_path} \\
    --stage featurize 
"""
    return shell_script_content






def _google_vm_base(contract_alias):

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
            echo "Error: Container ${{container_name}} is still running."
            exit 1
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
        --machine google_vm \\
        --username jeffreyclark \\
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



def _google_vm_init_and_crop(contract_alias):
    shell_script_content = _google_vm_base(contract_alias) + f"""
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


def _google_vm_featurize(contract_alias):
    shell_script_content = _google_vm_base(contract_alias) + f"""
# Run each stage
echo "starting feautrization"
run_docker "featurize"

update_status "featurize" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_swath_breaks(contract_alias):
    shell_script_content = _google_vm_base(contract_alias) + f"""
# Run each stage
echo "starting swath-breaks"
run_docker "swath-breaks"

update_status "swath_breaks" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content



def _google_vm_stitch_across(contract_alias):
    shell_script_content = _google_vm_base(contract_alias) + f"""
## Run each stage
echo "starting stitch-across"
run_docker "stitch-across"

update_status "stitch_across" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_refine_and_init_graph(contract_alias):

    shell_script_content = _google_vm_base(contract_alias) + f"""
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


def _google_vm_create_raster(contract_alias):

    shell_script_content = _google_vm_base(contract_alias) + f"""
# Run each stage
echo "starting create-raster"
run_docker "create-raster" --raster-type "clusters" --annotate "graph"

update_status "create_raster_1" "Done"
echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_new_neighbors(contract_alias, ids):

    vm_paths = cfg['google_vm']['vm_paths']
    docker_paths = cfg['google_vm']['docker_paths']
    config_path = os.path.join(docker_paths['stitching_repo'], "config", f"{contract_alias}.yml")
        
    shell_script_content = _google_vm_base(contract_alias) + f"""
# Run each stage
echo "starting new neighbors for "
run_docker "new-neighbors" --ids {ids} 

echo "All stages completed successfully."
    """
    return shell_script_content


def _google_vm_export_georef(contract_alias, ids):

    docker_paths = cfg['google_vm']['docker_paths']
    config_path = os.path.join(docker_paths['stitching_repo'], "config", f"{contract_alias}.yml")

    shell_script_content = _google_vm_base(contract_alias) + f"""
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
        --mount type=bind,source=/mnt/jeffrey_stitching_bucket,target=/app/bucket \
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



def generate_shell_script(contract_alias, machine, shell_template_id, **kwargs):

    if machine.lower() == "savio":
        func_map = {'initialize_and_crop': _savio_init_and_crop, 
                    'featurize': _savio_featurize
                    }
        
    elif machine.lower() == "google_vm":
        func_map = {'initialize_and_crop': _google_vm_init_and_crop, 
                    'featurize': _google_vm_featurize,
                    'swath_breaks': _google_vm_swath_breaks,
                    'stitch_across': _google_vm_stitch_across,
                    'initialize_graph': _google_vm_refine_and_init_graph,
                    'create_raster_1': _google_vm_create_raster,
                    'new_neighbors': _google_vm_new_neighbors,
                    'export_georef': _google_vm_export_georef
                    }
    else:
        raise ValueError('ONLY SAVIO SO FAR')
    
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
        file.write(func(contract_alias, **func_args))

    return fp



if __name__ == "__main__":
    pass