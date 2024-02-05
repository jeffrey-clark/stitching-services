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







