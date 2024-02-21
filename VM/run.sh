#!/bin/bash

# Function to execute a Python script in a Docker container with named arguments
run_docker_script() {
    local script_path="$1"
    local container_name="$2"
    shift 2  # Shift arguments to pass the remaining ones to the Python script

    # Check if the container already exists
    if sudo docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
        # Check if the container is running
        if sudo docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
            echo "Error: Container ${container_name} is still running."
            exit 1
        else
            # Remove the stopped container
            sudo docker rm "${container_name}"
        fi
    fi

    # Run the Docker container to execute the specified Python script with named arguments
    sudo docker run --name "${container_name}" \
        --mount type=bind,source=/home/jeffrey/repos/stitching-services,target=/home/app/stitching-services \
        --mount type=bind,source=/mnt/jeffrey_stitching_bucket,target=/app/bucket \
        stitching-services \
        python3 /home/app/stitching-services/${script_path} "$@"

    # Capture the exit code of the Docker run command
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Error: Docker container '${container_name}' exited with code $exit_code."
        exit $exit_code
    fi
}

# Main function
main() {
    if [ "$#" -lt 5 ]; then
        echo "Usage: $0 <python_script_path> <container_name> --contract_alias <alias> --machine <machine> --type <type>"
        exit 1
    fi

    run_docker_script "$@"
}

# Execute the main function with provided arguments
main "$@"