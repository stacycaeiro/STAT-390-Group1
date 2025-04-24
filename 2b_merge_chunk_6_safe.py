import pandas as pd
from build_wide_dataframe_full import normalize_file
from pathlib import Path
import os

DATA_ROOT = "power_data_bulk"

def load_chunk_6():
    dfs = []
    i = 0
    for root, _, files in os.walk(DATA_ROOT):
        for file in sorted(files):  # ensure order
            if file.endswith(('.csv', '.xlsx', '.xlsb')) and not file.startswith('~$'):
                filepath = os.path.join(root, file)
                filename_prefix = Path(file).stem.replace(" ", "_").replace(".", "_")
                dataset_prefix = f"{Path(root).name}_{filename_prefix}"
                df = normalize_file(filepath, dataset_prefix)
                if df is not None:
                    df = df.loc[:, ~df.columns.duplicated()]
                    dfs.append(df)
                i += 1
    return dfs[15:18]  # Assuming chunk size of 3 and chunk 6 = index 5

def safe_merge_chunk_6():
    print("🔎 Loading chunk 6...")
    df1, df2, df3 = load_chunk_6()

    print("🔗 Merging DF2 and DF3 (US-level only)...")
    us_merge = pd.merge(df2, df3, on=['year', 'object_id'], how='outer')
    print("✅ Merged DF2 + DF3:", us_merge.shape)

    print("💾 Saving US-level data to chunk_6_us_only.parquet")
    us_merge.to_parquet("merged_chunks/chunk_6_us_only.parquet")

    print("💾 Saving state-level data (DF1) to chunk_6_states_only.parquet")
    df1.to_parquet("merged_chunks/chunk_6_states_only.parquet")

    print("🎉 You can keep them separate, or concatenate manually with:")
    print("   pd.concat([df1, us_merge], ignore_index=True)")

if __name__ == "__main__":
    safe_merge_chunk_6()
