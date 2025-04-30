import pandas as pd


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
    print("🫗 Replacing empty strings with NaN...")
    return df.replace('', pd.NA)


def convert_year_to_numeric(df):
    print("⏳ Converting 'year'-related columns to numeric...")
    year_cols = [col for col in df.columns if 'year' in col.lower()]
    for col in year_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        print(f"🔢 Forced numeric conversion: {col}")
    return df

def convert_object_columns(df):
    print("🔧 Attempting to convert object columns to appropriate types...")
    object_cols = df.select_dtypes(include='object').columns

    for col in object_cols:
        sample_values = df[col].dropna().astype(str).head(10).tolist()
        print(f"🧪 Examining column '{col}': sample values (first 10) = {sample_values}")

        try:
            converted_numeric = pd.to_numeric(df[col].str.replace(",", "").str.strip(), errors='coerce')
            numeric_valid = converted_numeric.notna().sum() / len(df)
            if numeric_valid > 0.5:
                df[col] = converted_numeric
                print(f"✅ Converted to numeric ({numeric_valid:.0%} valid): {col}")
                continue

            converted_date = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
            datetime_valid = converted_date.notna().sum() / len(df)
            if datetime_valid > 0.5:
                df[col] = converted_date
                print(f"📅 Converted to datetime ({datetime_valid:.0%} valid): {col}")
                continue

        except Exception as e:
            print(f"⚠️ Skipped column {col} due to error: {e}")

    print("🔍 Object column conversion complete.")
    return df


def clean_dataset(file_path, save_cleaned=False, cleaned_file_path="cleaned_data.parquet"):
    df_original = load_data(file_path)
    original_shape = df_original.shape

    df = remove_duplicates(df_original)
    df = drop_unnamed_columns(df)
    df = clean_column_names(df)
    df = drop_high_null_columns(df)
    df = replace_empty_strings(df)
    df = convert_year_to_numeric(df)
    df = convert_object_columns(df)

    if save_cleaned:
        print(f"📂 Saving cleaned data to {cleaned_file_path}")
        df.to_parquet(cleaned_file_path, index=False)

    print("✅ Cleaning complete!")
    return df, original_shape