import os
import requests
import zipfile
from io import BytesIO
import pandas as pd

# Base directory to save files
base_dir = "power_data_bulk/923"
os.makedirs(base_dir, exist_ok=True)

# Years to download (2001 to 2025)
years = range(2001, 2026)

for year in years:
    print(f"\n📁 Downloading and extracting year: {year}")
    try:
        # Adjust URL based on year
        if year == 2025:
            url = f"https://www.eia.gov/electricity/data/eia923/xls/f923_{year}.zip"
        elif year <= 2007:
            url = f"https://www.eia.gov/electricity/data/eia923/archive/xls/f906920_{year}.zip"            
        else:
            url = f"https://www.eia.gov/electricity/data/eia923/archive/xls/f923_{year}.zip"

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Extract ZIP file into year folder
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            year_folder = os.path.join(base_dir, str(year))
            os.makedirs(year_folder, exist_ok=True)
            z.extractall(year_folder)

        print(f"✅ Success: Extracted {year} into {year_folder}")

    except Exception as e:
        print(f"❌ Failed for {year}: {e}")

# Hardcoded filenames
year_file_map = {
    2001: "f906920y2001.xls",
    2002: "f906920y2002.xls",
    2003: "f906920_2003.xls",
    2004: "f906920_2004.xls",
    2005: "f906920_2005.xls",
    2006: "f906920_2006.xls",
    2007: "f906920_2007.xls",
    2008: "eia923December2008.xls",
    2009: "EIA923 SCHEDULES 2_3_4_5 M Final 2009 REVISED 05252011.XLS",
    2010: "EIA923 SCHEDULES 2_3_4_5 Final 2010.xls",
    2011: "EIA923_Schedules_2_3_4_5_2011_Final_Revision.xlsx",
    2012: "EIA923_Schedules_2_3_4_5_M_12_2012_Final_Revision.xlsx",
    2013: "EIA923_Schedules_2_3_4_5_2013_Final_Revision.xlsx",
    2014: "EIA923_Schedules_2_3_4_5_M_12_2014_Final_Revision.xlsx",
    2015: "EIA923_Schedules_2_3_4_5_M_12_2015_Final_Revision.xlsx",
    2016: "EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx",
    2017: "EIA923_Schedules_2_3_4_5_M_12_2017_Final_Revision.xlsx",
    2018: "EIA923_Schedules_2_3_4_5_M_12_2018_Final_Revision.xlsx",
    2019: "EIA923_Schedules_2_3_4_5_M_12_2019_Final_Revision.xlsx",
    2020: "EIA923_Schedules_2_3_4_5_M_12_2020_Final_Revision.xlsx",
    2021: "EIA923_Schedules_2_3_4_5_M_12_2021_Final_Revision.xlsx",
    2022: "EIA923_Schedules_2_3_4_5_M_12_2022_Final_Revision.xlsx",
    2023: "EIA923_Schedules_2_3_4_5_M_12_2023_Final.xlsx",
    2024: "EIA923_Schedules_2_3_4_5_M_12_2024_21FEB2025.xlsx",
    2025: "EIA923_Schedules_2_3_4_5_M_02_2025_22APR2025.xlsx"
}

# Columns to fix
basic_column_fixes = {
    "combined_heat_and_power_plant": "combined_heat_&_power_plant",
    "nuclear_unit_i.d.": "nuclear_unit_id",
    "elec_fuel_consumption_mmbtu": "elec_fuel_consumption_mmbtus",
    "total_fuel_consumption_mmbtu": "total_fuel_consumption_mmbtus",
    "plant_state": "state",
    "mer_fuel_type_code": "aer_fuel_type_code",
    "elec_mmbtudec": "elec_mmbtus_dec",
}

# Common typos across many years
month_mapping = {
    "january": "jan", "february": "feb", "march": "mar", "april": "apr",
    "may": "may", "june": "jun", "july": "jul", "august": "aug",
    "september": "sep", "october": "oct", "november": "nov", "december": "dec"
}

merged_data = []
standard_columns = None

for year, filename in year_file_map.items():
    print(f"\n📁 Year: {year}")
    year_path = os.path.join(base_dir, str(year))
    file_path = os.path.join(year_path, filename)

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        continue

    try:
        excel_file = pd.ExcelFile(file_path)
        sheets = excel_file.sheet_names
    except Exception as e:
        print(f"❌ Failed to open {filename}: {e}")
        continue

    generation_sheet = next((sheet for sheet in sheets if "generation and fuel" in sheet.lower()), None)
    if not generation_sheet:
        print(f"⚠️ No 'Generation and Fuel' sheet found in {filename}")
        continue

    try:
        df = pd.read_excel(file_path, sheet_name=generation_sheet, header=None)

        header_row_idx = None
        for idx in range(min(10, len(df))):
            row = df.iloc[idx]
            joined_row = " ".join([str(x).strip().lower() for x in row])
            if (("plant id" in joined_row) or ("plant code" in joined_row)) and ("plant name" in joined_row):
                header_row_idx = idx
                break

        if header_row_idx is None:
            print(f"⚠️ No good header in {filename}")
            continue

        df.columns = df.iloc[header_row_idx]
        df = df.drop(index=df.index[:header_row_idx + 1])

        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace('\n', '_', regex=True)
            .str.replace(' ', '_', regex=True)
            .str.replace('"', '', regex=True)
        )
        
        # Apply manual renames
        df.rename(columns=basic_column_fixes, inplace=True)

        # Fix month typos like 'mmbtuper_unit_january' -> 'mmbtu_per_unit_jan'
        fixed_columns = {}
        for col in df.columns:
            new_col = col
            for full, abbr in month_mapping.items():
                new_col = new_col.replace(f"mmbtuper_unit_{full}", f"mmbtu_per_unit_{abbr}")
                new_col = new_col.replace(f"elec_quantity_{full}", f"elec_quantity_{abbr}")
                new_col = new_col.replace(f"elec_mmbtu_{full}", f"elec_mmbtus_{abbr}")
                new_col = new_col.replace(f"tot_mmbtu_{full}", f"tot_mmbtu_{abbr}")
                new_col = new_col.replace(f"netgen_{full}", f"netgen_{abbr}")
            fixed_columns[col] = new_col
        df.rename(columns=fixed_columns, inplace=True)

        df["year_reported"] = year
        cols = df.columns.tolist()
        cols = ['year_reported'] + [col for col in cols if col != 'year_reported']
        df = df[cols]

        # After first file, set standard
        if standard_columns is None:
            standard_columns = df.columns.tolist()

        else:
            # Make missing columns NaN
            for missing_col in set(standard_columns) - set(df.columns):
                df[missing_col] = pd.NA

            # Drop extra columns
            df = df[[col for col in df.columns if col in standard_columns]]

            # Reorder
            df = df[standard_columns]

        df = df.loc[:, ~df.columns.duplicated()]
        merged_data.append(df)
        print(f"✅ Loaded {filename} ({generation_sheet})")

    except Exception as e:
        print(f"❌ Failed processing {filename}: {e}")

# 🔍 Final
if merged_data:
    print(f"\n✅ All files now aligned to {len(standard_columns)} standard columns.")
else:
    print("\n❌ No data was loaded.")


merged_df = pd.concat(merged_data, ignore_index=True)
merged_df.to_csv("aggregate_eia923.csv", index=False)

print("\n✅ Merged file saved: eia923_generation_fuel_2001_2025.csv")
print(f"✅ Shape: {merged_df.shape[0]} rows × {merged_df.shape[1]} columns")
