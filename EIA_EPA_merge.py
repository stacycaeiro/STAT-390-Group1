# ======================================
# Final Merging Script: Merge All Datasets into One Unified DataFrame
# ======================================

import pandas as pd

# --- Block 1: Load Datasets ---

# Load primary plant-level dataset
plant_data = pd.read_parquet("merged_main_camp.parquet")

# Load A1 national sector totals
a1_data = pd.read_csv("group_a1_cleaned.csv")

# Load B-ALT state-fuel totals
balt_data = pd.read_csv("full_group_b_alt_cleaned_merged.csv")

# Clean B-ALT column names
balt_data.columns = balt_data.columns.str.strip().str.replace("\\n", " ").str.replace("\r", " ").str.replace("\t", " ").str.replace(" +", " ", regex=True)

# Smart dynamic renaming for balt_data
column_mapping = {}
for col in balt_data.columns:
    col_lower = col.lower()
    if "state" in col_lower:
        column_mapping[col] = "State"
    if "fuel" in col_lower:
        column_mapping[col] = "Fuel"
    if "year" in col_lower:
        column_mapping[col] = "Year"

balt_data = balt_data.rename(columns=column_mapping)

# Load Group C final state-sector totals
groupc_data = pd.read_csv("mega_group_c_cleaned_final_generation_only_no_division.csv")

# --- Block 2: Summarize Plant-Level Data ---

# Summarize generation by State, Year, Fuel
gen_summary_state_fuel = plant_data.groupby([
    "EPA_EIA_Crosswalk_epa_eia_crosswalk_CAMD_STATE", "year", "EPA_EIA_Crosswalk_epa_eia_crosswalk_CAMD_FUEL_TYPE"
]).agg({
    "Net Generation (Megawatthours)": "sum"
}).reset_index().rename(columns={
    "EPA_EIA_Crosswalk_epa_eia_crosswalk_CAMD_STATE": "State",
    "EPA_EIA_Crosswalk_epa_eia_crosswalk_CAMD_FUEL_TYPE": "Fuel",
    "year": "Year"
})

# Summarize generation by Year only
gen_summary_year = plant_data.groupby(["year"]).agg({
    "Net Generation (Megawatthours)": "sum"
}).reset_index().rename(columns={"year": "Year"})

# Summarize generation by State and Year (for Group C merge)
gen_summary_state_year = plant_data.groupby([
    "EPA_EIA_Crosswalk_epa_eia_crosswalk_CAMD_STATE", "year"
]).agg({
    "Net Generation (Megawatthours)": "sum"
}).reset_index().rename(columns={
    "EPA_EIA_Crosswalk_epa_eia_crosswalk_CAMD_STATE": "State",
    "year": "Year"
})

# --- Fix Year types across datasets ---

a1_data["Year"] = pd.to_numeric(a1_data["Year"], errors="coerce")
a1_data = a1_data.dropna(subset=["Year"])
a1_data["Year"] = a1_data["Year"].astype(int)

balt_data["Year"] = pd.to_numeric(balt_data["Year"], errors="coerce")
balt_data = balt_data.dropna(subset=["Year"])
balt_data["Year"] = balt_data["Year"].astype(int)

gen_summary_year["Year"] = gen_summary_year["Year"].astype(int)
gen_summary_state_fuel["Year"] = gen_summary_state_fuel["Year"].astype(int)
gen_summary_state_year["Year"] = gen_summary_state_year["Year"].astype(int)

# --- Block 3: Prepare Group C Correctly ---

# Dynamically pick only columns that end with _2022 or _2023
sector_columns = [col for col in groupc_data.columns if col.endswith('_2022') or col.endswith('_2023')]

# Melt Group C into long format for easier Year/Sector alignment
groupc_long = pd.melt(
    groupc_data,
    id_vars=["Census Division\nand State", "Source_File"],
    value_vars=sector_columns,
    var_name="Sector_Year",
    value_name="Generation"
)

# Correctly split Sector_Year into Sector and Year (safe extraction)
sector_year_split = groupc_long["Sector_Year"].str.extract(r"(.*)_(\d{4})")
groupc_long["Sector"] = sector_year_split[0]
groupc_long["Year"] = sector_year_split[1].astype(int)

# Rename for easier merge
groupc_long = groupc_long.rename(columns={"Census Division\nand State": "State"})

# --- Block 4: Merge Everything into One ---

# First, merge plant generation summary by Year with national totals
gen_summary_with_a1 = gen_summary_year.merge(a1_data, how="left", on="Year")

# Then, dynamically figure out merge keys for balt
available_merge_keys = [key for key in ["State", "Fuel", "Year"] if key in balt_data.columns]
if not available_merge_keys:
    raise ValueError("B-ALT dataset has no suitable columns to merge on.")

gen_summary_with_balt = gen_summary_state_fuel.merge(balt_data, how="left", on=available_merge_keys)

# Then, merge plant generation summary by State and Year with Group C (sector totals)
gen_summary_with_groupc = gen_summary_state_year.merge(groupc_long, how="left", on=["State", "Year"])

# Merge all together using outer joins for maximum coverage
final_merged = pd.merge(gen_summary_with_balt, gen_summary_with_groupc, how="outer", on=["State", "Year"])
final_merged = pd.merge(final_merged, gen_summary_with_a1, how="outer", on=["Year"])

# --- Block 5: Save Final Unified Output ---

final_merged.to_parquet("merged_full_unified_dataset.parquet", index=False)

print("\n✅ Final unified merge complete! File saved:")
print("- merged_full_unified_dataset.parquet")
