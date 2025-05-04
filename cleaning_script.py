import pandas as pd
import numpy as np
from collections import defaultdict
import json
import warnings

def load_data(file_path):
    print("✨ Loading data...")
    return pd.read_parquet(file_path)

def remove_duplicates(df):
    print(f"🔄 Removing duplicates: {df.duplicated().sum()} duplicates found.")
    return df.drop_duplicates()

def detect_mixed_dtypes(df, sample_frac=0.3, verbose=True):
    """
    Report object columns with >1 underlying Python type.
    Great for catching hidden numbers stored as strings, etc.
    """
    mixed_cols = {}
    for col in df.select_dtypes(include='object').columns:
        # Sample to avoid scanning huge columns
        sample = df[col].dropna().sample(frac=sample_frac, random_state=42) if len(df) > 10_000 else df[col].dropna()
        types = sample.map(type).unique()
        if len(types) > 1:
            mixed_cols[col] = [t.__name__ for t in types[:5]]  # keep it readable
            if verbose:
                print(f"⚠️ Mixed types in '{col}': {mixed_cols[col]}")
    if not mixed_cols and verbose:
        print("✅ No mixed‑dtype object columns detected.")
    return mixed_cols

def fix_mixed_dtypes(df, mixed_cols_report, numeric_cutoff=0.90, datetime_cutoff=0.90):
    """
    For every column flagged by detect_mixed_dtypes:
      1. Try numeric coercion.  Accept if ≥ numeric_cutoff non‑null.
      2. Else try datetime coercion.  Accept if ≥ datetime_cutoff non‑null.
      3. Else force string dtype (or drop if everything became NaN).
    Returns the cleaned DataFrame.
    """
    for col in mixed_cols_report:
        col_series = df[col]

        # 1️⃣ numeric attempt
        num = pd.to_numeric(col_series.astype(str).str.replace(",", "").str.strip(),
                            errors="coerce")
        if num.notna().mean() >= numeric_cutoff:
            df[col] = num
            print(f"🔢  Fixed '{col}' ➜ numeric ({num.notna().sum()} valid rows)")
            continue

        # 2️⃣ datetime attempt
        dt = pd.to_datetime(col_series, errors="coerce")
        if dt.notna().mean() >= datetime_cutoff:
            df[col] = dt
            print(f"📅  Fixed '{col}' ➜ datetime ({dt.notna().sum()} valid rows)")
            continue

        # 3️⃣ fallback: string or drop
        if col_series.notna().any():
            df[col] = col_series.astype(str)
            print(f"🔤  Normalised '{col}' ➜ all strings")
        else:
            df.drop(columns=[col], inplace=True)
            print(f"🗑️  Dropped '{col}' (all values became NaN)")

    return df

def downcast_numeric(df, verbose=True):
    """Losslessly down‑cast numeric columns to the smallest dtype."""
    for col in df.select_dtypes(include=['int', 'float']).columns:
        old_dtype = df[col].dtype
        df[col] = pd.to_numeric(df[col], downcast='integer' if 'int' in str(old_dtype) else 'float')
        if verbose and df[col].dtype != old_dtype:
            print(f"🔻 Down‑cast {col}: {old_dtype} ➜ {df[col].dtype}")
    return df

def categorify_low_cardinality(df, threshold=100, verbose=True):
    """
    Convert object columns with ≤ `threshold` unique values to pandas 'category' dtype.
    This saves memory and speeds up many operations.
    """
    for col in df.select_dtypes(include='object').columns:
        nunique = df[col].nunique(dropna=False)
        if nunique <= threshold:
            df[col] = df[col].astype('category')
            if verbose:
                print(f"🏷️  Categorified '{col}' ({nunique} unique values)")
    return df

def drop_unnamed_columns(df):
    print("✂️ Dropping 'Unnamed:' columns...")
    return df.loc[:, ~df.columns.str.contains('^Unnamed')]

def clean_column_names(df):
    print("✨ Cleaning column names...")
    df.columns = (
        df.columns
        .str.strip()
        .str.replace('%20', '_', regex=False)
        .str.replace(r'\s+', '_', regex=True)
        .str.replace('[^0-9a-zA-Z_]', '', regex=True)
    )
    return df

def drop_high_null_columns(df, threshold=0.99):
    print(f"🔢 Dropping columns with >{int(threshold*100)}% null values...")
    null_ratios = df.isnull().mean()
    to_drop = null_ratios[null_ratios > threshold].index
    print(f"❌ Columns to drop: {len(to_drop)}")
    return df.drop(columns=to_drop)

def replace_empty_strings(df):
    print("🫗 Replacing empty strings and 'nan' strings with NaN...")
    return df.replace(['', 'nan'], pd.NA)

def convert_year_to_numeric(df):
    print("⏳ Converting 'year'-related columns to numeric...")
    year_cols = [col for col in df.columns if 'year' in col.lower()]
    for col in year_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        print(f"🔢 Forced numeric conversion: {col}")
    return df

def convert_object_columns(df, verbose=True):
    print("🔧 Attempting to convert object columns to appropriate types...")
    object_cols = df.select_dtypes(include='object').columns

    for col in object_cols:
        if verbose:
            print(f"🧪 Column '{col}': sample values (up to 5 unique) = {df[col].dropna().astype(str).unique()[:5]}")

        try:
            # Suppress datetime conversion warnings
            with warnings.catch_warnings():
                warnings.simplefilter(action='ignore', category=UserWarning)

                # Attempt numeric conversion
                converted_numeric = pd.to_numeric(df[col].str.replace(",", "").str.strip(), errors='coerce')
                numeric_valid = converted_numeric.notna().sum() / len(df)
                if numeric_valid > 0.5:
                    df[col] = converted_numeric
                    print(f"✅ Converted to numeric ({numeric_valid:.0%} valid): {col}")
                    continue

                # Attempt datetime check but do NOT convert
                converted_date = pd.to_datetime(df[col], errors='coerce')
                datetime_valid = converted_date.notna().sum() / len(df)
                if datetime_valid > 0.5:
                    print(f"📅 Detected datetime-like column ({datetime_valid:.0%} valid): {col}")
                    continue

        except Exception as e:
            print(f"⚠️ Skipped column {col} due to error: {e}")

    print("🔍 Object column conversion complete.")
    return df

def drop_metadata_columns(df, keywords=None):
    if keywords is None:
        keywords = [
            'note', 'description', 'about', 'info', 'metadata', 'comment', 'remarks', 'source',
            'unit', 'type', 'flag', 'category', 'code', 'status', 'reference', 'definition'
        ]

    name_based_cols = [
        col for col in df.columns
        if any(kw in col.lower() for kw in keywords)
    ]

    if name_based_cols:
        print(f"❌ Dropping metadata columns: {name_based_cols}")
        return df.drop(columns=name_based_cols)
    else:
        print("✅ No metadata columns detected.")
        return df

def generate_nested_column_groups(df):
    nested_groups = defaultdict(lambda: defaultdict(list))

    for col in df.columns:
        parts = col.split('_')
        if len(parts) >= 3 and parts[0] in {"EPA", "EIA"}:
            top = f"{parts[0]}_{parts[1]}"
            sub = '_'.join(parts[2:4])  # sub-sub
        elif len(parts) >= 2:
            top = parts[0]
            sub = parts[1]
        else:
            top = parts[0]
            sub = "misc"
        nested_groups[top][sub].append(col)

    with open("nested_column_groups.json", "w") as f:
        json.dump(nested_groups, f, indent=2)

    print("🗂️ Group structure saved to nested_column_groups.json")
    return nested_groups

def create_summary(df):
    print("📊 Creating summary DataFrame...")
    summary = pd.DataFrame({
        'Column': df.columns,
        'Missing %': df.isna().mean().round(3),
        'Unique Values': df.nunique(),
        'Dtype': df.dtypes.astype(str),
        'Low Variance': [df[col].nunique(dropna=False) <= 1 for col in df.columns]
    })

    summary.to_csv("column_summary.csv", index=False)
    print("📁 Summary saved to column_summary.csv")
    return summary

def clean_dataset(file_path, save_cleaned=True):
    df_original = load_data(file_path)
    original_shape = df_original.shape

    df = remove_duplicates(df_original)
    df = drop_unnamed_columns(df)
    df = clean_column_names(df)
    df = drop_high_null_columns(df)
    df = replace_empty_strings(df)
    df = convert_year_to_numeric(df)
    df = convert_object_columns(df, verbose=True)
    
    mixed_cols_report = detect_mixed_dtypes(df, verbose=True)
    df = fix_mixed_dtypes(df, mixed_cols_report)
    
    for col in mixed_cols_report:               # auto‑coerce flagged cols
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "").str.strip(),
            errors="coerce"
        )
        
    df = drop_metadata_columns(df)

    # Drop all-NaN columns
    all_nan_cols = df.columns[df.isna().all()]
    if len(all_nan_cols) > 0:
        print(f"❌ Dropping all-NaN columns: {list(all_nan_cols)}")
        df.drop(columns=all_nan_cols, inplace=True)
    else:
        print("✅ No all-NaN columns to drop.")

    # Drop low-variance columns
    low_var_cols = [col for col in df.columns if df[col].nunique(dropna=False) <= 1]
    if low_var_cols:
        print(f"⚠️ Dropping low-variance columns (nunique ≤ 1): {low_var_cols}")
        df.drop(columns=low_var_cols, inplace=True)
    else:
        print("✅ No low-variance columns to drop.")

    # Optimize memory usage
    df = downcast_numeric(df)
    df = categorify_low_cardinality(df, threshold=100)

    # Generate column summary of cleaned DataFrame
    summary_df = create_summary(df)

    # Generate and save nested groups
    nested_groups = generate_nested_column_groups(df)

    if save_cleaned:
        print(f"📂 Saving cleaned data to {file_path}")
        df.to_parquet(file_path, index=False)

    print("✅ Cleaning complete!")
    return df, original_shape, nested_groups, summary_df

if __name__ == "__main__":
    input_path = "cleaned_data.parquet"

    df, original_shape, nested_groups, summary_df = clean_dataset(
        input_path,
        save_cleaned=True
    )
