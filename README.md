# STAT-390-Group1
STAT 390 Final Project 

Order to run the files:
1. autdownload_*.py 
   - Downloads all the data either into power_data_bulk folder or a separate folder
2. build_wide_dataframe_full.py
   - Merges all the data in the power_data_bulk folder
4. merge_chunk_6_safe.py
   - Merges all the data in the power_data_bulk folder
5. merge_chunk_6_into_final.py
   - Merges all the data in the power_data_bulk folder to create big parquet file
   - Most up to date file at this point is called `all_data_raw_merged_with_chunk6.parquet`
6. eia_aeo2025_all_parquet.py
   - Merges all the EIA AEO 2025 data into one big file
7. merge2_aeo2025_w_lilly.py
   - Merges the file created in script #6 with the big parquet file that script #5 created
   - Most up to date file at this point is called `merged_main_with_aeo.parquet`\
   - This should be accessible from a folder that's called merged_output_chunks
8. merge3_GHG_with_main.py
   - Merges the GHG ClimateWatch data with the file that script #7 created
   - Most up to date file at this point is called `merged_main_with_aeo_and_ghg.parquet`
   - This should be accessible from a folder that's called merged_output_chunks
