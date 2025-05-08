import pandas as pd
import numpy as np
from collections import defaultdict
import json
import warnings


# ---------------------------
# NEW: Numeric conversion prefixes
# ---------------------------
NUMERIC_PREFIXES = [
    "EIA_SEDS_pr_US_19",
    "EIA_SEDS_pr_US_20",
    "EIA_SEDS_use_US_19",
    "EIA_SEDS_use_US_20"
]

NUMERIC_EXACT_COLUMNS = [
    'EPA_FLIGHT_GHGRP_ghgp_data_parent_company_PARENT_CO_PERCENT_OWNERSHIP',
    'EPA_NEEDS_needsrev06062024_Capacity_MW',
    'EPA_NEEDS_needsrev06062024_Heat_Rate_BtukWh',
    'EPA_NEEDS_needsrev06062024_SO2_Permit_Rate_lbsmmBtu',
    'EPA_NEEDS_needsrev06062024_Mode_1_NOx_Rate_lbsmmBtu',
    'EPA_NEEDS_needsrev06062024_Mode_2_NOx_Rate_lbsmmBtu',
    'EPA_NEEDS_needsrev06062024_Mode_3_NOx_Rate_lbsmmBtu',
    'EPA_NEEDS_needsrev06062024_Mode_4_NOx_Rate_lbsmmBtu',
    'EPA_NEEDS_needsrev06062024_Owner_Percent',
    'EPA_NEEDS_needsrev06062024_Holding_Company_Percent'
]

def force_numeric_conversion(df, prefixes=None, exact_columns=None):
    print("🔧 Forcing numeric conversion...")
    matched_cols = set()

    if prefixes:
        prefix_matches = [col for col in df.columns if any(col.startswith(p) for p in prefixes)]
        print(f"📌 Matched {len(prefix_matches)} columns by prefix")
        matched_cols.update(prefix_matches)

    if exact_columns:
        exact_matches = [col for col in exact_columns if col in df.columns]
        print(f"📌 Matched {len(exact_matches)} columns by exact name")
        matched_cols.update(exact_matches)

    for col in matched_cols:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "").str.strip(),
            errors='coerce'
        )
        print(f"🔢 Converted '{col}' to numeric")

    return df


# ---------------------------
# Cleaning helper functions
# ---------------------------

def load_data(file_path):
    print("✨ Loading data...")
    return pd.read_parquet(file_path)

def remove_duplicates(df):
    print(f"🔄 Removing duplicates: {df.duplicated().sum()} duplicates found.")
    return df.drop_duplicates()

def detect_mixed_dtypes(df, sample_frac=0.3, verbose=True):
    mixed_cols = {}
    for col in df.select_dtypes(include='object').columns:
        sample = (
            df[col].dropna()
                 .sample(frac=sample_frac, random_state=42)
            if len(df) > 10_000 else
            df[col].dropna()
        )
        types = sample.map(type).unique()
        if len(types) > 1:
            mixed_cols[col] = [t.__name__ for t in types[:5]]
            if verbose:
                print(f"⚠️ Mixed types in '{col}': {mixed_cols[col]}")
    if not mixed_cols and verbose:
        print("✅ No mixed-dtype object columns detected.")
    return mixed_cols

def fix_mixed_dtypes(df, mixed_cols_report, numeric_cutoff=0.90, datetime_cutoff=0.90):
    for col in mixed_cols_report:
        s = df[col]
        num = pd.to_numeric(
            s.astype(str).str.replace(",", "").str.strip(),
            errors="coerce"
        )
        if num.notna().mean() >= numeric_cutoff:
            df[col] = num
            print(f"🔢 Fixed '{col}' ➜ numeric")
            continue
        dt = pd.to_datetime(s, errors="coerce")
        if dt.notna().mean() >= datetime_cutoff:
            df[col] = dt
            print(f"📅 Fixed '{col}' ➜ datetime")
            continue
        if s.notna().any():
            df[col] = s.astype(str)
            print(f"🔤 Normalised '{col}' ➜ strings")
        else:
            df.drop(columns=[col], inplace=True)
            print(f"🗑️ Dropped '{col}' (all NaN)")
    return df

def downcast_numeric(df, verbose=True):
    for col in df.select_dtypes(include=['int', 'float']).columns:
        old = df[col].dtype
        df[col] = pd.to_numeric(
            df[col],
            downcast='integer' if 'int' in str(old) else 'float'
        )
        if verbose and df[col].dtype != old:
            print(f"🔻 Down-cast {col}: {old} → {df[col].dtype}")
    return df

def categorify_low_cardinality(df, threshold=100, verbose=True):
    for col in df.select_dtypes(include='object').columns:
        nu = df[col].nunique(dropna=False)
        if nu <= threshold:
            df[col] = df[col].astype('category')
            if verbose:
                print(f"🏷️ Categorified '{col}' ({nu} values)")
    return df

def drop_unnamed_columns(df):
    print("✂️ Dropping unnamed columns...")
    return df.loc[:, ~df.columns.str.contains(r'^Unnamed')]

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
    print("🔢 Dropping mostly-null columns...")
    null_ratios = df.isnull().mean()
    to_drop = null_ratios[null_ratios > threshold].index
    print(f"❌ Columns dropped: {len(to_drop)}")
    return df.drop(columns=to_drop)

def replace_empty_strings(df):
    print("🫗 Converting blank and 'None' to NA...")
    return df.replace(r'^(?:\s*|nan|none)$', pd.NA, regex=True)

def convert_year_to_numeric(df):
    print("⏳ Converting year columns to numeric...")
    for col in df.columns:
        if 'year' in col.lower():
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def convert_object_columns(df, verbose=True):
    print("🔧 Converting remaining object columns where possible...")
    for col in df.select_dtypes(include='object').columns:
        if verbose:
            sample = df[col].dropna().astype(str).unique()[:5]
            print(f"🧪 '{col}': {sample}")
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', UserWarning)
                num = pd.to_numeric(df[col].str.replace(",", "").str.strip(), errors='coerce')
                if num.notna().mean() > 0.5:
                    df[col] = num
                    print(f"✅ Converted '{col}' to numeric")
                    continue
                dt = pd.to_datetime(df[col], errors='coerce')
                if dt.notna().mean() > 0.5:
                    print(f"📅 Detected datetime-like '{col}'")
        except Exception:
            pass
    return df

def drop_metadata_columns(df, keywords=None):
    if keywords is None:
        keywords = [
            'note','description','info','metadata','comment',
            'source','unit','type','flag','category','status'
        ]
    to_drop = [c for c in df.columns if any(kw in c.lower() for kw in keywords)]
    if to_drop:
        print(f"❌ Dropping metadata columns: {len(to_drop)}")
        return df.drop(columns=to_drop)
    return df

def generate_nested_column_groups(df):
    nested = defaultdict(lambda: defaultdict(list))
    for col in df.columns:
        parts = col.split('_')
        if len(parts) >= 3 and parts[0] in {"EPA","EIA"}:
            top = f"{parts[0]}_{parts[1]}"
            sub = '_'.join(parts[2:4])
        elif len(parts) >= 2:
            top, sub = parts[0], parts[1]
        else:
            top, sub = parts[0], 'misc'
        nested[top][sub].append(col)
    with open("nested_column_groups.json","w") as f:
        json.dump(nested, f, indent=2)
    return nested

def create_summary(df):
    print("📊 Saving column summary...")
    summary = pd.DataFrame({
        'Column':    df.columns,
        'Missing %': df.isna().mean().round(3),
        'Unique':    df.nunique(),
        'Dtype':     df.dtypes.astype(str),
        'LowVar':    [df[c].nunique(dropna=False)<=1 for c in df.columns]
    })
    summary.to_csv("column_summary.csv", index=False)
    return summary

# ---------------------------
# Main cleaning function
# ---------------------------

def clean_dataset(raw_path, save_cleaned=True):
    df = load_data(raw_path)
    orig_shape = df.shape

    df = remove_duplicates(df)
    df = drop_unnamed_columns(df)
    df = clean_column_names(df)
    df = force_numeric_conversion(df, prefixes = NUMERIC_PREFIXES, exact_columns = NUMERIC_EXACT_COLUMNS)
    df = drop_high_null_columns(df)
    df = replace_empty_strings(df)
    df = convert_year_to_numeric(df)
    df = convert_object_columns(df)

    mixed = detect_mixed_dtypes(df)
    df = fix_mixed_dtypes(df, mixed)

    df = drop_metadata_columns(df)

    # remove fully empty or single-value columns
    all_na  = [c for c in df.columns if df[c].isna().all()]
    low_var = [c for c in df.columns if df[c].nunique(dropna=False) <= 1]
    for c in set(all_na + low_var):
        df.drop(columns=[c], inplace=True)

    df = downcast_numeric(df)
    df = categorify_low_cardinality(df)

    summary = create_summary(df)
    nested  = generate_nested_column_groups(df)

    if save_cleaned:
        df.to_parquet(raw_path, index=False)

    print(f"✅ Cleaning complete: {orig_shape} → {df.shape}")
    return df

# ---------------------------
# Validation function
# ---------------------------

def validate_and_finalize(df, raw_path):
    # replace any leftover placeholders
    df = df.replace(r'(?i)^(?:none|\s*)$', pd.NA, regex=True)

    # drop columns now entirely NA
    placeholder_only = [c for c in df.columns if df[c].isna().all()]
    if placeholder_only:
        df.drop(columns=placeholder_only, inplace=True)

    orig      = pd.read_parquet(raw_path)
    orig_n    = orig.shape[0] - orig.duplicated().sum()
    clean_n   = df.shape[0]
    before_o  = orig.select_dtypes("object").shape[1]
    after_o   = df.select_dtypes("object").shape[1]

    assert clean_n == orig_n, "Row count mismatch"
    assert after_o < before_o, "Object columns not reduced"
    bad_cols = [
        c for c in df.select_dtypes("object")
        if df[c].dropna().astype(str).str.strip().str.lower().isin({"none",""}).any()
    ]
    assert not bad_cols, f"Placeholders remain: {bad_cols}"
    assert not any(df[c].isna().all() for c in df.columns), "Empty columns remain"

    print("🎉 Validation succeeded!")
    return df

# ---------------------------
# Script entrypoint
# ---------------------------

if __name__ == "__main__":
    RAW = "merged_main_camp.parquet"
    cleaned = clean_dataset(RAW, save_cleaned=False)
    cleaned = validate_and_finalize(cleaned, RAW)
    cleaned.to_parquet("cleaned_dataset.parquet", index=False)
