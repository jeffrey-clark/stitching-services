import os
import Functions.utilities as u


cfg = u.read_config()


def _savio_init_and_crop(contract_alias):

    config_path = os.path.join(cfg['savio']['config_folder'], f"{contract_alias}.yml")

    shell_script_content = f"""#!/bin/bash
# Job name:
#SBATCH --job-name={job_name}_stage_1     
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




def generate_shell_script(contract_alias, config_path, computer, shell_template_id):

    if computer.lower() == "savio":
        func_map = {1: _savio_init_and_crop}
    else:
        raise ValueError('ONLY SAVIO SO FAR')
    
    # make sure that we have the 

    fp = os.path.join("Files/job_shells", computer.lower(), f"{contract_alias}_{shell_template_id}.sh")
    os.makedirs(os.path.dirname(fp), exist_ok=True)

    with open(fp, "w") as file:
        file.write(func_map[shell_template_id](contract_alias, config_path))


if __name__ == "__main__":
    generate_shell_script('test', 'yoyoyo', 'savio', 1)