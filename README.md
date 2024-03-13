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





# Things that need to be done when setting up new VM

- Make sure that the VM has the following settings

  ```
  API and identity management
  
  	Cloud API access scopes: Allow full access to all Cloud APIs
  ```

  

- Linux APT installs

  ```shell
  sudo apt update
  sudo apt upgrade
  sudo apt install git
  sudo apt install htop
  sudo apt install tmux
```
  
With the Google VMs it is rather easy to install `GCSFuse`
  
  ```shell
  export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
  
  sudo mkdir -p /etc/apt/keyrings/
  	wget -O- https://packages.cloud.google.com/apt/doc/apt-key.gpg |
  	    gpg --dearmor |
  	    sudo tee /etc/apt/keyrings/gcsfuse.gpg > /dev/null
  	
  	echo "deb [signed-by=/etc/apt/keyrings/gcsfuse.gpg] https://packages.cloud.google.com/apt $GCSFUSE_REPO main" |
  	    sudo tee /etc/apt/sources.list.d/gcsfuse.list
  
  sudo apt-get update
  sudo apt-get install gcsfuse
```
  
You also need to install **Docker**

- Clone the repos `stitching-services` and `aerial-history-stitching`

- Build the Docker image from the Dockerfile

  ```
  sudo docker build -t stitching-services .
  ```

- Create the mount point `/mnt/<name of google bucket>` and set the chmod

  ```
  sudo mkdir /mnt/<name of google bucket>
  sudo chmod 777 /mnt/<name of google bucket>
  
  sudo mkdir /mnt/jeffrey_stitching_bucket_2
  sudo chmod 777 /mnt/jeffrey_stitching_bucket_2
  ```

  

- Modify `/etc/fuse.conf`

  ```
  sudo nano /etc/fuse.conf
  
  --> Uncomment the option user_allow_other
  ```

- Modify `/etc/fstab`

  - First run the command `id` and get the user_id and group_id, these need to be insterted correctly into the mount command below

    ````
    sudo nano /etc/fstab
    
    # Append the following mount entry
    
    jeffrey_stitching_bucket_2 /mnt/jeffrey_stitching_bucket_2 gcsfuse rw,gid=1005,uid=1004,file_mode=777,dir_mode=777,user,allow_other,_netdev,stat_cache_ttl=0,ttl=0,type_cache_ttl=0,implicit_dirs
    ````



- IN the 



# Other comments

capture output is used when text is returned in the console on command execution. 





