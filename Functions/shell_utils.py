import os
import Functions.utilities as u


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



def _google_vm_init_and_crop(contract_alias):

    vm_paths = cfg['google_vm']['vm_paths']
    docker_paths = cfg['google_vm']['docker_paths']

    config_path = os.path.join(docker_paths['stitching_repo'], "config", f"{contract_alias}.yml")

    shell_script_content = f"""#!/usr/bin/env bash

# Function to run docker and check for errors
run_docker() {{
    sudo docker run --name "{contract_alias}_$1" \\
        --mount type=bind,source={vm_paths['bucket']},target={docker_paths['bucket']} \\
        --mount type=bind,source={vm_paths['stitching_repo']},target={docker_paths['stitching_repo']} \\
        --mount type=bind,source={vm_paths['logs_folder']},target={docker_paths['logs_folder']} \\
        enoda/opencv_surf \\
        python3 {docker_paths['stitching_repo']}/main.py \\
        --config {config_path} \\
        --stage "$1"

    # Capture the exit code
    local exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo "Error: Container for stage $1 failed with exit code $exit_code."
        exit $exit_code
    fi
}}

# Run each stage
echo "starting initialize"
run_docker "initialize"
echo "starting crop"
run_docker "crop"
echo "starting inspect-cropping"
run_docker "inspect-cropping"

echo "All stages completed successfully."
    """
    return shell_script_content




def _google_vm_featurize(contract_alias):

    vm_paths = cfg['google_vm']['vm_paths']
    docker_paths = cfg['google_vm']['docker_paths']

    config_path = os.path.join(docker_paths['stitching_repo'], "config", f"{contract_alias}.yml")

    shell_script_content = f"""#!/usr/bin/env bash

# Function to run docker and check for errors
run_docker() {{
    sudo docker run --name "{contract_alias}_$1" \\
        --mount type=bind,source={vm_paths['bucket']},target={docker_paths['bucket']} \\
        --mount type=bind,source={vm_paths['stitching_repo']},target={docker_paths['stitching_repo']} \\
        --mount type=bind,source={vm_paths['logs_folder']},target={docker_paths['logs_folder']} \\
        enoda/opencv_surf \\
        python3 {docker_paths['stitching_repo']}/main.py \\
        --config {config_path} \\
        --stage "$1"

    # Capture the exit code
    local exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo "Error: Container for stage $1 failed with exit code $exit_code."
        exit $exit_code
    fi
}}

# Run each stage
echo "starting feautrization"
run_docker "featurize"

echo "All stages completed successfully."
    """
    return shell_script_content




def generate_shell_script(contract_alias, machine, shell_template_id):

    if machine.lower() == "savio":
        func_map = {1: _savio_init_and_crop, 
                    2: _savio_featurize}
        
    elif machine.lower() == "google_vm":
        func_map = {1: _google_vm_init_and_crop, 
                    2: _google_vm_featurize
                    }
    else:
        raise ValueError('ONLY SAVIO SO FAR')
    
    # make sure that we have the 

    fp = os.path.join("Files/job_shells", machine.lower(), f"{contract_alias}_{shell_template_id}.sh")
    os.makedirs(os.path.dirname(fp), exist_ok=True)


    with open(fp, "w") as file:
        file.write(func_map[shell_template_id](contract_alias))

    return fp


if __name__ == "__main__":
    generate_shell_script('test', 'yoyoyo', 'savio', 1)