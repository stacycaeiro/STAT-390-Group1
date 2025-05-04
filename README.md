# STAT-390-Group1
STAT 390 Final Project 

Order to run the files:
1. autodownload_*.py 
   - Downloads all the data either into power_data_bulk folder or a separate folder
2. build_wide_dataframe_full.py
   - Merges all the data in the power_data_bulk folder
   - Written by Lilly
3. merge_chunk_6_safe.py
   - Merges all the data in the power_data_bulk folder
   - Written by Lilly
4. merge_chunk_6_into_final.py
   - Merges all the data in the power_data_bulk folder to create big parquet file
   - Written by Lilly
   - Most up to date file at this point is called `all_data_raw_merged_with_chunk6.parquet`
5. eia_aeo2025_all_parquet.py
   - Merges all the EIA AEO 2025 data into one big file
   - Written by Emma
6. merge2_aeo2025_w_lilly.py
   - Merges the file created in script #6 with the big parquet file that script #5 created
   - Written by Emma
   - Most up to date file at this point is called `merged_main_with_aeo.parquet`\
      - This should be accessible from a folder that's called merged_output_chunks
7. merge3_GHG_with_main.py
   - Merges the GHG ClimateWatch data with the file that script #7 created
   - Written by Emma
   - Most up to date file at this point is called `merged_main_with_aeo_and_ghg.parquet`
      - This should be accessible from a folder that's called merged_output_chunks
8. merge_eia923_w_main.py
   - Merges the EIA 923 data with `merged_main_with_aeo_and_ghg.parquet`
   - Written by Sam Sword
   - Most up to date file at this point is called merged_main_with_aeo_and_ghg_with_923.parquet
9. usurdb_merge_test.py
    - Merges USURDB with merged_main_with_aeo_and_ghg_with_923.parquet
    - Written by Isabel Knight
    - Most up to date file at this point is: merged_main_data_with_usurdb.parquet
10. merge_CAMP_facilities_w_main.py
    - Merges CAMP facilities data with merged_main_data_with_usurdb.parquet
    - Written by Sam Sword
    - Most up to date file at this point is: merged_main_camp.parquet
