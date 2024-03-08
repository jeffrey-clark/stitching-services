from main import *


# s = SavioClient()
# s.upload_files_sftp(["Config/google_service.json"], ["/global/home/users/jeffreyclark/repos/stitching-services/Config/google_service.json"])

country = "Nigeria"
contract_name = "NCAP_DOS_SHELL_BP"
# contract_name = "NCAP_DOS_USAAF_1"

main(contract_name, country, "savio")
