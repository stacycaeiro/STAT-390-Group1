# --- SETUP ---
import os
import pandas as pd
from pathlib import Path

# --- CONSTANTS ---
DATA_ROOT = "eia_aeo2025_all_xlsx"
OUTPUT_FOLDER = "melted_aeo_data_fixed"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

all_melted_dfs = []

# --- PROCESS FILES ---
for root, _, files in os.walk(DATA_ROOT):
    for file in files:
        if file.endswith(".xlsx") and not file.startswith('~$'):
            file_path = os.path.join(root, file)
            try:
                # Load main data, skipping first 10 rows
                df = pd.read_excel(file_path, skiprows=10, engine="openpyxl", header=None)

                # Rename first column manually
                df = df.rename(columns={0: "category"})

                # Load the row with years (row 9)
                year_row = pd.read_excel(file_path, skiprows=9, nrows=1, header=None)
                for i, val in enumerate(year_row.values[0][1:], start=1):
                    df = df.rename(columns={i: str(val).strip()})

                # Drop empty columns
                df.dropna(axis=1, how="all", inplace=True)

                # --- Extract region ---
                # Extract region (only for suptab files)
                if "suptab" in file.lower():
                    region_row = pd.read_excel(file_path, skiprows=8, nrows=1, header=None)
                    region = str(region_row.iloc[0, 1]).strip()  # <-- Column 1 not Column 0!
                
                    if not region or region.lower() == "nan":
                        region = "Unknown"
                else:
                    region = "United States"


                # --- MELT ---
                df_melted = df.melt(id_vars=["category"], var_name="year", value_name="value")

                # Drop empty categories
                df_melted = df_melted[df_melted["category"].notna()]

                # Drop rows where 'year' is weird
                df_melted = df_melted[df_melted["year"].str.isnumeric()]

                # Add region now
                df_melted["object_id"] = region

                # Add filename
                df_melted["source_file"] = Path(file).stem

                # Save individual melted parquet
                output_file = os.path.join(OUTPUT_FOLDER, f"melted_{Path(file).stem}.parquet")
                df_melted.to_parquet(output_file)

                # Append to big list
                all_melted_dfs.append(df_melted)

                print(f"✅ Melted and saved: {file}")

            except Exception as e:
                print(f"❌ Could not process {file}: {e}")

# --- FINAL COMBINE ---
if all_melted_dfs:
    final_df = pd.concat(all_melted_dfs, ignore_index=True)

    # Convert all columns to string to be safe
    final_df = final_df.astype(str)

    final_path = os.path.join(OUTPUT_FOLDER, "all_aeo_data_combined.parquet")
    final_df.to_parquet(final_path)

    print(f"\n🎉 All files melted and combined! Final shape: {final_df.shape}")
else:
    print("\n⚠️ No usable files processed.")
