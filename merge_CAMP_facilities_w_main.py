import os
import pandas as pd
from pathlib import Path

# Paths
FACILITY_DATA_ROOT = "facility_data_simple"
MAIN_PARQUET = "merged_main_data_with_usurdb.parquet"
OUTPUT_PARQUET = "merged_main_camp.parquet"

def load_csv_file(filepath):
    try:
        df = pd.read_csv(filepath)
        if df is not None:
            df['Year'] = extract_year_from_filename(filepath)
            df['source_file'] = Path(filepath).name
        return df
    except Exception as e:
        print(f"❌ Failed to load {filepath}: {e}")
        return None

def extract_year_from_filename(filepath):
    filename = Path(filepath).stem
    for part in filename.split('_'):
        if part.isdigit() and len(part) == 4:
            return part
    return None

def load_all_facility_data():
    all_dfs = []
    for root, dirs, files in os.walk(FACILITY_DATA_ROOT):
        for file in files:
            if file.endswith('.csv'):
                filepath = os.path.join(root, file)
                df = load_csv_file(filepath)
                if df is not None:
                    all_dfs.append(df)
    if not all_dfs:
        print("⚠️ No CSV files loaded!")
        return None
    print(f"✅ Loaded {len(all_dfs)} files")
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"✅ Combined facility data shape: {combined_df.shape}")
    return combined_df

def merge_with_main(facility_df):
    print("📂 Loading main parquet...")
    main_df = pd.read_parquet(MAIN_PARQUET)
    print(f"✅ Main parquet shape: {main_df.shape}")

    print("\nFacility Columns:\n", facility_df.columns.tolist())
    print("\nMain Parquet Columns:\n", main_df.columns.tolist())

    # Auto-detect facility ID column
    facility_id_col = None
    for candidate in ['Facility ID', 'Facility Id']:
        if candidate in facility_df.columns:
            facility_id_col = candidate
            break

    if facility_id_col and 'EPA_EIA_Crosswalk_epa_eia_crosswalk_EIA_PLANT_ID' in main_df.columns:
        print(f"🔎 Using facility ID column: {facility_id_col}")

        facility_df[facility_id_col] = facility_df[facility_id_col].astype(str).str.strip()
        main_df['EPA_EIA_Crosswalk_epa_eia_crosswalk_EIA_PLANT_ID'] = main_df['EPA_EIA_Crosswalk_epa_eia_crosswalk_EIA_PLANT_ID'].astype(str).str.strip()

        print(f"📊 Grouping facility data by [Year, {facility_id_col}] and averaging numeric columns...")
        facility_grouped = facility_df.groupby(['Year', facility_id_col]).mean(numeric_only=True).reset_index()

        print("🪄 Splitting main_df into chunks...")
        chunk_size = 50000
        chunks = [main_df[i:i+chunk_size] for i in range(0, main_df.shape[0], chunk_size)]
        print(f"✅ Split into {len(chunks)} chunks of about {chunk_size} rows each")

        merged_chunks = []

        for idx, chunk in enumerate(chunks):
            print(f"🔗 Merging chunk {idx+1}/{len(chunks)}...")
            merged_chunk = pd.merge(
                chunk,
                facility_grouped,
                left_on=['Year', 'EPA_EIA_Crosswalk_epa_eia_crosswalk_EIA_PLANT_ID'],
                right_on=['Year', facility_id_col],
                how='left'
            )
            merged_chunks.append(merged_chunk)

        print("🧹 Concatenating all merged chunks...")
        merged_df = pd.concat(merged_chunks, ignore_index=True)
        print(f"✅ Final merged dataframe shape: {merged_df.shape}")

        # Calculate match statistics
        matches = merged_df[facility_grouped.columns.difference(['Year', facility_id_col])].notnull().any(axis=1).sum()
        total = merged_df.shape[0]
        print(f"🔎 Matches found: {matches}/{total} ({matches/total:.2%})")

        print(f"💾 Saving merged dataframe to {OUTPUT_PARQUET}...")
        merged_df.to_parquet(OUTPUT_PARQUET)
        print("✅ Done.")
    else:
        print("⚠️ Could not find matching facility ID columns in both datasets.")

if __name__ == "__main__":
    print("🚀 Starting merge process...")
    facility_df = load_all_facility_data()
    if facility_df is not None:
        merge_with_main(facility_df)
