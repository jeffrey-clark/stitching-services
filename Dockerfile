# Start with a base image that includes Conda. Here, we're using a minimal Anaconda image
FROM continuumio/miniconda3

# Optionally set a working directory inside the container
WORKDIR /app

# Copy the Conda environment file into the container
COPY environment.yml /app/environment.yml

# Create the Conda environment using the environment file
RUN conda env create -f /app/environment.yml

# Make RUN commands use the new environment and activate it on shell
SHELL ["conda", "run", "-n", "stitch-service", "/bin/bash", "-c"]

# Copy the rest of your application's code into the container
# COPY . /app

# Set the environment name (as defined in your environment.yml)
ENV CONDA_DEFAULT_ENV stitch-service

# Specify the command to run on container start
# CMD ["conda", "run", "--no-capture-output", "-n", "stitch-service", "python", "your-script.py"]