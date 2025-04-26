# --- SETUP ---
import os
import pandas as pd


# --- LOAD DATASETS ---
print("\n📥 Loading datasets...")
df_main = pd.read_parquet("merged_output_chunks/merged_all_chunks.parquet")
df_aeo = pd.read_parquet("melted_aeo_data_fixed/all_aeo_data_combined.parquet")
print("✅ Datasets loaded.")
print("- Main dataset shape:", df_main.shape)
print("- AEO dataset shape:", df_aeo.shape)

# --- CLEAN CATEGORIES ---
print("\n🧹 Cleaning AEO categories...")
df_aeo["clean_category"] = df_aeo["category"].str.split(":").str[-1]   # Remove prefixes like 'EPD009:ba_'
df_aeo["clean_category"] = df_aeo["clean_category"].str.split("_").str[-1]  # Remove anything before underscores too
print(f"✅ Total clean categories found: {df_aeo['clean_category'].nunique()}")

# --- PIVOT AEO ---
print("\n🔄 Pivoting AEO dataset...")
pivot_aeo = df_aeo.pivot_table(index=["object_id", "year"], 
                               columns="clean_category", 
                               values="value", 
                               aggfunc="first")

pivot_aeo.reset_index(inplace=True)
print("✅ Pivoted AEO dataset shape:", pivot_aeo.shape)

# --- MERGE ---
print("\n🔗 Performing full outer merge...")
merged = pd.merge(df_main, pivot_aeo, on=["object_id", "year"], how="outer")
print("✅ Merge completed.")
print("- Final merged dataset shape:", merged.shape)

# --- SAVE ---
output_path = "merged_output_chunks/merged_main_with_aeo.parquet"
merged.to_parquet(output_path)
print(f"\n💾 Saved final merged dataset to: {output_path}")
