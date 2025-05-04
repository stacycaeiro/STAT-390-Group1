import pandas as pd

# File paths
final_main = "all_data_raw_merged.parquet"
chunk6_states = "merged_chunks/chunk_6_states_only.parquet"
chunk6_us = "merged_chunks/chunk_6_us_only.parquet"
final_output = "all_data_raw_merged_with_chunk6.parquet"

def merge_chunk6():
    print("📥 Loading main merged dataframe...")
    df_main = pd.read_parquet(final_main)
    print(f"✅ Main shape: {df_main.shape}")

    print("📥 Loading chunk 6 state-level data...")
    df_states = pd.read_parquet(chunk6_states)
    print(f"✅ DF1 (states): {df_states.shape}")

    print("📥 Loading chunk 6 US-level data...")
    df_us = pd.read_parquet(chunk6_us)
    print(f"✅ DF2+3 (US): {df_us.shape}")

    print("🔗 Concatenating chunk 6 data...")
    df_chunk6 = pd.concat([df_states, df_us], ignore_index=True)
    df_chunk6 = df_chunk6.loc[:, ~df_chunk6.columns.duplicated()]
    print(f"✅ Chunk 6 combined shape: {df_chunk6.shape}")

    print("🔄 Merging with main dataset on ['year', 'object_id']...")
    merged = pd.merge(df_main, df_chunk6, on=['year', 'object_id'], how='outer')
    merged = merged.loc[:, ~merged.columns.duplicated()]
    print(f"✅ Final shape after merge: {merged.shape}")

    print(f"💾 Saving to: {final_output}")
    merged.to_parquet(final_output)
    print("✅ Done.")

if __name__ == "__main__":
    merge_chunk6()
