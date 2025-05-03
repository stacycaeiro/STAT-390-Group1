import os
import pandas as pd
from pathlib import Path
from functools import reduce

# Paths
EIA_923_ROOT = "EIA_Form_923"
MAIN_PARQUET = "merged_main_with_aeo_and_ghg.parquet"
OUTPUT_PARQUET = "merged_main_with_aeo_and_ghg_with_923.parquet"

def smart_read_eia923(filepath):
    # Read the first 10 rows to detect header
    preview = pd.read_excel(filepath, sheet_name=0, nrows=10, header=None)

    header_row = None
    for i, row in preview.iterrows():
        joined_row = ' '.join(row.fillna('').astype(str)).lower()
        if 'plant' in joined_row and ('id' in joined_row or 'code' in joined_row):
            header_row = i
            break

    if header_row is None:
        print(f"⚠️ Couldn't find a good header in {filepath}. Defaulting to row 0.")
        header_row = 0

    # Now read the full file using detected header
    df = pd.read_excel(filepath, sheet_name=0, header=header_row)
    df.dropna(axis=1, how='all', inplace=True)  # Drop empty columns
    df.columns = df.columns.map(str)  # Make sure columns are all strings
    return df

# Helper function to load each Excel file
def load_excel_file(filepath):
    try:
        df = smart_read_eia923(filepath)
        if df is not None:
            df['year'] = Path(filepath).parent.name  # Assuming parent folder name = year
            df['source_file'] = Path(filepath).name
        return df
    except Exception as e:
        print(f"❌ Failed to smart load {filepath}: {e}")
        return None

# Step 1-3: Crawl and load all EIA 923 files
def load_all_eia_923():
    all_dfs = []
    for root, dirs, files in os.walk(EIA_923_ROOT):
        for file in files:
            if file.endswith('.xlsx') and not file.startswith('~$'):
                filepath = os.path.join(root, file)
                df = load_excel_file(filepath)
                if df is not None:
                    year_guess = Path(root).name  # folder name should be the year
                    df['year'] = year_guess  # Add year column
                    df['source_file'] = file  # Keep track of original file
                    all_dfs.append(df)

    if not all_dfs:
        print("⚠️ No Excel files loaded!")
        return None

    print(f"✅ Loaded {len(all_dfs)} files")
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"✅ Combined cleaned EIA 923 shape: {combined_df.shape}")
    return combined_df

# Step 4-5: Merge with main parquet
def merge_with_main(eia923_df):
    print("📂 Loading main parquet...")
    main_df = pd.read_parquet(MAIN_PARQUET)
    print(f"✅ Main parquet shape: {main_df.shape}")

    print("\nEIA 923 Columns:\n", eia923_df.columns.tolist())
    print("\nMain Parquet Columns:\n", main_df.columns.tolist())

    if 'Plant Id' in eia923_df.columns and 'EPA_EIA_Crosswalk_epa_eia_crosswalk_EIA_PLANT_ID' in main_df.columns:
        print("🔎 Found good plant ID columns in both datasets.")

        # Fix types
        eia923_df['Plant Id'] = eia923_df['Plant Id'].astype(str).str.strip()
        main_df['EPA_EIA_Crosswalk_epa_eia_crosswalk_EIA_PLANT_ID'] = main_df['EPA_EIA_Crosswalk_epa_eia_crosswalk_EIA_PLANT_ID'].astype(str).str.strip()

        print("📊 Grouping EIA 923 by [year, Plant Id] and averaging numeric columns...")
        eia923_grouped = eia923_df.groupby(['year', 'Plant Id']).mean(numeric_only=True).reset_index()

        print("🪄 Splitting main_df into chunks...")
        chunk_size = 50000  # you can tweak this if you want (e.g., 30,000 or 70,000 depending on your memory)
        chunks = [main_df[i:i+chunk_size] for i in range(0, main_df.shape[0], chunk_size)]
        print(f"✅ Split into {len(chunks)} chunks of about {chunk_size} rows each")

        merged_chunks = []

        for idx, chunk in enumerate(chunks):
            print(f"🔗 Merging chunk {idx+1}/{len(chunks)}...")
            merged_chunk = pd.merge(
                chunk,
                eia923_grouped,
                left_on=['year', 'EPA_EIA_Crosswalk_epa_eia_crosswalk_EIA_PLANT_ID'],
                right_on=['year', 'Plant Id'],
                how='left'
            )
            merged_chunks.append(merged_chunk)
        
        print("🧹 Concatenating all merged chunks...")
        merged_df = pd.concat(merged_chunks, ignore_index=True)
        print(f"✅ Final merged dataframe shape: {merged_df.shape}")

        print(f"💾 Saving merged dataframe to {OUTPUT_PARQUET}...")
        merged_df.to_parquet(OUTPUT_PARQUET)
        print("✅ Done.")

    else:
        print("⚠️ Could not find matching plant ID columns in both datasets.")

if __name__ == "__main__":
    print("🚀 Starting merge process...")
    eia923_df = load_all_eia_923()
    if eia923_df is not None:
        merge_with_main(eia923_df)
