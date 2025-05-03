import pandas as pd
from collections import defaultdict
import json
import warnings

def load_data(file_path):
    print("✨ Loading data...")
    return pd.read_parquet(file_path)

def remove_duplicates(df):
    print(f"🔄 Removing duplicates: {df.duplicated().sum()} duplicates found.")
    return df.drop_duplicates()

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
