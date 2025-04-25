import os
import pandas as pd
from functools import reduce
from pathlib import Path

DATA_ROOT = "power_data_bulk"

def extract_year_from_filename(filepath):
    parts = Path(filepath).stem.split("_")
    for part in parts:
        if part.isdigit() and len(part) == 4:
            return int(part)
    return None

def get_best_key_column(df):
    key_candidates = [
        'plant_id', 'plant code', 'plantcode',
        'facility_id', 'facility name',
        'utility_id', 'utility name', 'state'
    ]
    for col in df.columns:
        for key in key_candidates:
            if key in col.lower():
                return col
    return None

def normalize_file(file_path, dataset_prefix):
    ext = file_path.lower().split(".")[-1]
    try:
        if ext == "csv":
            df = pd.read_csv(file_path, low_memory=False)
        elif ext == "xlsx":
            df = pd.read_excel(file_path, sheet_name=0)
        elif ext == "xlsb":
            import pyxlsb
            xls = pd.ExcelFile(file_path, engine="pyxlsb")
            dfs = []
            for sheet_name in xls.sheet_names:
                try:
                    df_sheet = xls.parse(sheet_name)
                    df_sheet.dropna(axis=1, how='all', inplace=True)
                    df_sheet.columns = df_sheet.columns.map(str)
                    df_sheet = df_sheet.astype(str)
                    df_sheet['year'] = sheet_name if sheet_name.isdigit() else None
                    dfs.append(df_sheet)
                except Exception as e:
                    print(f"⚠️ Could not load sheet {sheet_name} in {file_path}: {e}")
            df = pd.concat(dfs, ignore_index=True) if dfs else None
        else:
            return None
    except Exception as e:
        print(f"❌ Could not read {file_path}: {e}")
        return None

    if df is None or df.empty:
        return None

    df = df.copy()
    df.dropna(axis=1, how='all', inplace=True)
    df.columns = df.columns.map(str)
    df = df.astype(str)

    # Year handling
    if 'year' in df.columns:
        df['year'] = df['year']
    else:
        year_col = next((col for col in df.columns if 'year' in col.lower()), None)
        if year_col:
            df['year'] = df[year_col]
        else:
            extracted_year = extract_year_from_filename(file_path)
            df['year'] = str(extracted_year) if extracted_year else None

    # object_id
    key_col = get_best_key_column(df)
    df['object_id'] = df[key_col].astype(str) if key_col and key_col in df.columns else f"unknown_{Path(file_path).stem}"

    key_cols = ['year', 'object_id']
    df = df.rename(columns={
        col: f"{dataset_prefix}_{col}" for col in df.columns if col not in key_cols
    })

    value_cols = [col for col in df.columns if col not in key_cols]
    return df[key_cols + value_cols]

def build_wide_dataframe():
    dfs = []
    chunk_log = []

    for root, _, files in os.walk(DATA_ROOT):
        for file in files:
            if file.endswith(('.csv', '.xlsx', '.xlsb')) and not file.startswith('~$'):
                filepath = os.path.join(root, file)
                filename_prefix = Path(file).stem.replace(" ", "_").replace(".", "_")
                dataset_prefix = f"{Path(root).name}_{filename_prefix}"
                df = normalize_file(filepath, dataset_prefix)
                if df is not None:
                    df = df.loc[:, ~df.columns.duplicated()]
                    dfs.append(df)

    print(f"✅ Loaded {len(dfs)} dataframes")

    if not dfs:
        print("⚠️ No usable dataframes found. Exiting.")
        return

    chunks = [dfs[i:i + 3] for i in range(0, len(dfs), 3)]
    merged_chunks = []
    os.makedirs("merged_chunks", exist_ok=True)

    for i, chunk in enumerate(chunks):
        if i == 5:  # Skip chunk 6
            print(f"⏭️ Skipping chunk {i + 1} (chunk 6) due to memory blow-up")
            chunk_log.append({
                "chunk": i + 1,
                "rows": None,
                "columns": None,
                "file": None,
                "status": "skipped"
            })
            continue

        print(f"\n🔍 Chunk {i + 1}/{len(chunks)}")
        for j, d in enumerate(chunk):
            print(f"  📄 DF {j + 1}: shape={d.shape}, columns={len(d.columns)}")

        try:
            merged = reduce(lambda left, right: pd.merge(
                left, right, on=['year', 'object_id'], how='outer'), chunk)

            merged = merged.loc[:, ~merged.columns.duplicated()]
            print(f"✅ Merged chunk {i + 1}: shape={merged.shape}")

            chunk_path = f"merged_chunks/chunk_{i + 1}.parquet"
            merged.to_parquet(chunk_path)
            merged_chunks.append(merged)

            chunk_log.append({
                "chunk": i + 1,
                "rows": merged.shape[0],
                "columns": merged.shape[1],
                "file": chunk_path,
                "status": "success"
            })

        except Exception as e:
            print(f"❌ Failed to merge chunk {i + 1}: {e}")
            chunk_log.append({
                "chunk": i + 1,
                "rows": None,
                "columns": None,
                "file": None,
                "status": f"failed: {e}"
            })

    pd.DataFrame(chunk_log).to_csv("merge_log.csv", index=False)

    if not merged_chunks:
        print("⚠️ No chunks were successfully merged. Skipping final merge.")
        return

    print("\n🧩 Final merge of all merged chunks...")
    try:
        final_df = reduce(lambda left, right: pd.merge(
            left, right, on=['year', 'object_id'], how='outer'), merged_chunks)
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]
        print(f"✅ Final merged dataframe shape: {final_df.shape}")

        print("💾 Saving to all_data_raw_merged.parquet ...")
        final_df.to_parquet("all_data_raw_merged.parquet")
        print("✅ Done.")
    except Exception as e:
        print(f"❌ Final merge failed: {e}")

if __name__ == "__main__":
    print("🚀 Starting wide dataframe builder...")
    build_wide_dataframe()
