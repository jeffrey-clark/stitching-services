import pandas
import os

contracts = ["Nigeria_26_37", "Nigeria_26_35", "NCAP_DOS_SHELL_BP", "NCAP_DOS_212_NG"]
contracts = ["Nigeria_26_10", "Nigeria_26_47", "Nigeria_26_34"]
contracts = ["Nigeria_CPE_0300", "Nigeria_AAS", "Nigeria_84"]
contracts = ["Nigeria_21"]
contracts = ["Nigeria_A"]

results_folder = "/Users/jeffrey/Aerial History Project/Stitching/results/Nigeria"

total_images = 0
print("\nWe have new contract(s) ready for georeferencing:")
print("\nContract:             Number of Images")
print("--------------------------------------------------")
for c in contracts:
    fp = os.path.join(results_folder, c, "swaths_selected_raws.csv")
    df = pandas.read_csv(fp)
    # count the number of rows
    # print(f"{c}: {len(df)}")
    print(f"{c:25} {len(df):>5}")
    total_images+= len(df)
print("--------------------------------------------------")
print(f"{'Total images':24}  {total_images:>5}\n")
print("Please advise when we can delete the contract(s) from the folder \"ready_to_georeference\".\n")

