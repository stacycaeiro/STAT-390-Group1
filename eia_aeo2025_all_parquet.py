import os
import pandas as pd
from pathlib import Path

DATA_ROOT = "eia_aeo2025_all_xlsx"
OUTPUT_FOLDER = "melted_aeo_data_fixed"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

all_melted_dfs = []

for root, _, files in os.walk(DATA_ROOT):
    for file in files:
        if file.endswith(".xlsx") and not file.startswith('~$'):
            file_path = os.path.join(root, file)
            try:
                # Load skipping first 10 rows
                df = pd.read_excel(file_path, skiprows=10, engine="openpyxl", header=None)

                # Rename first column manually
                df = df.rename(columns={0: "category"})

                # Rest of the columns are years (columns 1 onward)
                # Extract years manually from the original file
                year_row = pd.read_excel(file_path, skiprows=9, nrows=1, header=None)
                for i, val in enumerate(year_row.values[0][1:], start=1):
                    df = df.rename(columns={i: str(val).strip()})

                # Drop empty columns
                df.dropna(axis=1, how="all", inplace=True)

                # Melt the dataframe
                df_melted = df.melt(id_vars=["category"], var_name="year", value_name="value")

                # Drop empty categories
                df_melted = df_melted[df_melted["category"].notna()]

                # Drop rows where 'year' is weird
                df_melted = df_melted[df_melted["year"].str.isnumeric()]

                # Add filename
                df_melted["source_file"] = Path(file).stem

                # Save individual melted parquet
                output_file = os.path.join(OUTPUT_FOLDER, f"melted_{Path(file).stem}.parquet")
                df_melted.to_parquet(output_file)

                all_melted_dfs.append(df_melted)

                print(f"✅ Melted and saved: {file}")

            except Exception as e:
                print(f"❌ Could not process {file}: {e}")

# FINAL COMBINE
if all_melted_dfs:
    final_df = pd.concat(all_melted_dfs, ignore_index=True)

    final_df = final_df.astype(str)

    final_path = os.path.join(OUTPUT_FOLDER, "all_aeo_data_combined.parquet")
    final_df.to_parquet(final_path)

    print(f"\n🎉 All files melted and combined! Final shape: {final_df.shape}")
else:
    print("\n⚠️ No usable files processed.")
