# ======================================
# Script to Create Initial Group CSVs from Excel Source Files
# ======================================

import pandas as pd
import os

# Set the folder containing all the XLS/XLSX files
data_folder = "eia_epa_all_xls"

# --- Block 1: Build Group A1 CSV (National sector totals) ---

files_a1 = [
    "epa_01_01.xlsx", "epa_01_02.xlsx", "epa_01_03.xlsx", 
    "epa_02_02.xlsx", "epa_02_11.xlsx"
]

dfs_a1 = []
for file in files_a1:
    filepath = os.path.join(data_folder, file)
    df = pd.read_excel(filepath, skiprows=0)
    dfs_a1.append(df)

# Combine and clean Group A1
group_a1_combined = pd.concat(dfs_a1, ignore_index=True)
group_a1_combined = group_a1_combined.dropna(subset=["Year"])

# Save
group_a1_combined.to_csv("group_a1_cleaned.csv", index=False)

# --- Block 2: Build B-ALT CSV (State-level fuel mix totals) ---

files_balt = [
    "epa_03_01_a.xlsx", "epa_03_01_b.xlsx", "epa_03_02_a.xlsx", 
    "epa_03_02_b.xlsx", "epa_03_03_a.xlsx", "epa_03_04_a.xlsx",
    "epa_03_04_b.xlsx", "epa_03_05_a.xlsx", "epa_03_05_b.xlsx"
]

dfs_balt = []
for file in files_balt:
    filepath = os.path.join(data_folder, file)
    df = pd.read_excel(filepath, skiprows=0)
    dfs_balt.append(df)

# Combine and clean Group B-ALT
group_balt_combined = pd.concat(dfs_balt, ignore_index=True)
group_balt_combined = group_balt_combined.dropna(subset=["Year"])

# Save
group_balt_combined.to_csv("full_group_b_alt_cleaned_merged.csv", index=False)

# --- Block 3: Build Group C Mega CSV (State-sector generation tables) ---

files_c = [
    "epa_03_17.xlsx", "epa_03_18.xlsx", "epa_03_19.xlsx", "epa_03_20.xlsx", "epa_03_21.xlsx", "epa_03_22.xlsx",
    "epa_03_23.xlsx", "epa_03_24.xlsx", "epa_03_25.xlsx", "epa_03_26.xlsx", "epa_03_27.xlsx",
    "epa_04_01.xlsx", "epa_04_02_a.xlsx", "epa_04_02_b.xlsx", "epa_04_03.xlsx", "epa_04_04.xlsx"
]

dfs_c = []
for file in files_c:
    filepath = os.path.join(data_folder, file)
    df = pd.read_excel(filepath, skiprows=0)
    dfs_c.append(df)

# Combine Group C and remove non-generation columns
group_c_combined = pd.concat(dfs_c, ignore_index=True)
drop_cols = [col for col in group_c_combined.columns if "Division" in col or "Unnamed" in col]
group_c_cleaned = group_c_combined.drop(columns=drop_cols)

# Save
group_c_cleaned.to_csv("mega_group_c_cleaned_final_generation_only_no_division.csv", index=False)

# --- Done ---

print("\n✅ All three group CSVs created and saved:")
print("- group_a1_cleaned.csv")
print("- full_group_b_alt_cleaned_merged.csv")
print("- mega_group_c_cleaned_final_generation_only_no_division.csv")