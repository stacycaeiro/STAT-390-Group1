# -*- coding: utf-8 -*-
"""
Created on Sun Apr 27 10:46:35 2025

@author: blueg
"""

import pandas as pd
import glob
import os

# #### the code below combines all of the data in each folder into one file by state

list_of_folders = ['weekend_schedule', 'weekday_schedule',
                     'energyratestructure', 'demandratestructure']

for i in range(len(list_of_folders)):
    # Path where your 50 files are stored
    folder_path = 'usurdb_flattened/'+list_of_folders[i]+'/' # <-- CHANGE THIS
    # Find all CSV files in the folder
    all_files = glob.glob(os.path.join(folder_path, '*.csv'))
    # Read and concatenate
    df_list = []
    for file in all_files:
        df = pd.read_csv(file)
        df_list.append(df)
    # Combine all into one DataFrame
    combined_df = pd.concat(df_list, ignore_index=True)

    # (Optional) Check combined size
    print(combined_df.shape)

    # (Optional) Save to one CSV
    combined_df.to_csv('usurdb_flattened/final_'+list_of_folders[i]+'.csv', index=False)

# ########
# ### combine all 4 datasets into one dataset to add to the parquet file l8r


# # 1. Load everything
weekday = pd.read_csv('usurdb_flattened/final_weekday_schedule.csv')
weekend = pd.read_csv('usurdb_flattened/final_weekend_schedule.csv')
energy = pd.read_csv('usurdb_flattened/final_energyratestructure.csv')
demand = pd.read_csv('usurdb_flattened/final_demandratestructure.csv')

# 3. Rename columns before merging to avoid conflicts
energy = energy.rename(columns={'rate': 'energy_rate', 'unit': 'energy_unit', 'max': 'energy_max', 'min': 'energy_min', 'adjustment': 'energy_adjustment', 'sell': 'energy_sell'})
demand = demand.rename(columns={'rate': 'demand_rate', 'unit': 'demand_unit', 'max': 'demand_max', 'min': 'demand_min', 'adjustment': 'demand_adjustment'})

weekday = weekday.rename(columns={'period': 'weekday_period'})
weekend = weekend.rename(columns={'period': 'weekend_period'})

# 4. Merge: Energy and Demand
merged = pd.merge(energy, demand, on=['utility_name', 'rate_name', 'sector', 'startdate', 'enddate', 'state', 'eiaid', 'rate_id', 'tier_index'], how='outer')

# 5. Merge: Add Weekday
merged = pd.merge(merged, weekday[['utility_name', 'rate_name', 'sector', 'startdate', 'enddate', 'state', 'eiaid', 'rate_id', 'tier_index', 'hour', 'weekday_period']], 
                  on=['utility_name', 'rate_name', 'sector', 'startdate', 'enddate', 'state', 'eiaid', 'rate_id', 'tier_index'], how='outer')

# 6. Merge: Add Weekend
merged = pd.merge(merged, weekend[['utility_name', 'rate_name', 'sector', 'startdate', 'enddate', 'state', 'eiaid', 'rate_id', 'tier_index', 'hour', 'weekend_period']], 
                  on=['utility_name', 'rate_name', 'sector', 'startdate', 'enddate', 'state', 'eiaid', 'rate_id', 'tier_index', 'hour'], how='outer')

# # # 7. Save it
merged.to_csv('usurdb_flattened/final_merged_dataset.csv', index=False)

######
##combine the final dataset made with the parquet file

# 1. Load the datasets
main_big = pd.read_parquet('merged_main_with_aeo_and_ghg_with_923.parquet')
merged = pd.read_csv('usurdb_flattened/final_merged_dataset.csv')
main_big['year'] = pd.to_numeric(main_big['year'], errors='coerce').astype('Int64')

#fix some of the year information
merged['start_year'] = pd.to_datetime(merged['startdate'], unit='s').dt.year
merged['end_year'] = pd.to_datetime(merged['enddate'], unit='s').dt.year
merged['year'] = merged['end_year'].fillna(merged['start_year']).astype('Int64')


#filter merged data
merged = merged[merged['year'].notna()]
# 2. Drop the 'hour' column
if 'hour' in merged.columns:
    merged = merged.drop(columns=['hour'])

# 3. Remove duplicates
# Keep only one row per: utility_name, rate_name, sector, state, eiaid, rate_id, tier_index, startdate, enddate, year
merged = merged.drop_duplicates(subset=[
    'utility_name', 'rate_name', 'sector', 'state', 'eiaid', 'rate_id', 'tier_index', 'startdate', 'enddate', 'year'
])

essential_cols = [
    'utility_name', 'rate_name', 'sector', 'state', 'eiaid', 'rate_id', 'year', 'energy_rate', 'demand_rate'
]

merged_reduced = merged[essential_cols].drop_duplicates()


merged_reduced.columns = ['object_id', 'rate_name', 'sector', 'state', 'eiaid', 'rate_id',
       'year', 'energy_rate', 'demand_rate']


final_combined = pd.merge(main_big,
    merged_reduced,
    how='left',  # or 'inner' if you only want matched years
    on=['year', 'object_id']
)

output_path = "merged_main_data_with_usurdb.parquet"
final_combined.to_parquet(output_path)
print(f"\n💾 Saved final merged dataset to: {output_path}")



