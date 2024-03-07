# Stitching Services

This repository contains code to simplify and scale the stitiching process for the Aerial History Project.



## 1. Set Up Encrypted Savio Credentials

Integration with Savio is enabled using the Paramiko package + TOTP calculations which allows us to fetch the updated password when required, removing the need for Google Authenticator.  However, to avoid revealing senstive data we encrypt credentials. 

To set up your encrypted credentials run the `Functions/encryption.py` file and follow the instructions. The encrypted credentials are stored in `Config/savio_credentials.yml`.



## 2. Config File

Before running code you also need to set up the file paths to savio and Google bucket folders. This is done by filling in the YAML file found in `Config/config.yml`.



## Installing environemnt from file

First export the environment

`conda env export --from-history > environment.yml`

Then copy over the file to the other machine. Remember to change the conda prefix in the envionrment file, to match that of the new computer.

 Then install the environment on the other machine

`conda env create -f environment.yml `





# Docker

The stitching services relies on Docker and Apptainer (Singularity) containers to run the `aerial-history-stitching` pipeline as well as custom scripts. For the stitching pipeline we use the `enoda/surf` image, while for custom script we create an image from the repository Dockerfile. 



### How to build from Docker file

In the command line run the following command to build a Docker Image from the Dockerfile.

```bash
docker build -t stitching-services .
```

This is done on the VM



#### How to upload an Docker container to Savio and convert it to an Apptainer Container

We need to export the Docker image, upload it to savio, and covert it there to an Apptainer. We do the export into the Files directory which is not tracked by Git.

1. Export your docker container

```bash
docker save stitching-services > Files/stitching-services.tar
```

**Note:** if building on a MacOS device with the M1, M2, chips i.e. ARM, need to build on VM or something instead with AMD CPU. Otherwise it will confict with Savio

2. upload the `.tar` file to savio in your home folder 

3. Go into the resources dir and convert the `.tar` to a `.sif` file

```
apptainer build stitching-services.sif docker-archive://stitching-services.tar
```







# Cloning Repos to all remote locations









# Other comments

capture output is used when text is returned in the console on command execution. 





