# --- SETUP ---
import os
import pandas as pd

# --- LOAD DATASETS ---
print("\n📥 Loading datasets...")
df_main = pd.read_parquet("merged_output_chunks/merged_main_with_aeo.parquet")
ghg_df = pd.read_csv("global_ghg_emissions_world_CAIT.csv")
print("✅ Datasets loaded.")
print("- Main dataset shape:", df_main.shape)
print("- GHG dataset shape:", ghg_df.shape)

# --- QUICK CLEANING ---
print("\n🧹 Cleaning GHG data...")

# Force year to string, to match
ghg_df["year"] = ghg_df["year"].astype(str)

# Create a new object_id column from country
ghg_df["object_id"] = ghg_df["country"].str.strip() + "-CAIT"

# Create a composite column name: gas + sector
ghg_df["ghg_category"] = ghg_df["gas"].str.strip() + " - " + ghg_df["sector"].str.strip()

# Drop columns we don't need anymore
ghg_df = ghg_df.drop(columns=["country", "gas", "sector", "source"])

print("✅ Columns after cleaning:", ghg_df.columns.tolist())

# --- PIVOT ---
print("\n🔄 Pivoting GHG dataset...")
pivot_ghg = ghg_df.pivot_table(index=["object_id", "year"],
                               columns="ghg_category",
                               values="value",
                               aggfunc="first")

pivot_ghg.reset_index(inplace=True)
print("✅ Pivoted GHG dataset shape:", pivot_ghg.shape)

# --- MERGE ---
print("\n🔗 Performing full outer merge...")
merged = pd.merge(df_main, pivot_ghg, on=["object_id", "year"], how="outer")
print("✅ Merge completed.")
print("- Final merged dataset shape:", merged.shape)

# --- SAVE ---
output_path = "merged_output_chunks/merged_main_with_aeo_and_ghg.parquet"
merged.to_parquet(output_path)
print(f"\n💾 Saved final merged dataset to: {output_path}")
