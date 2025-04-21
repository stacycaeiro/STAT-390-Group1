import os
import requests

# Change this to your desired save directory
SAVE_DIR = "eia_923_data"
os.makedirs(SAVE_DIR, exist_ok=True)

# Download from 2001 to latest available year
BASE_URL = "https://www.eia.gov/electricity/data/eia923/xls/f923_{year}.zip"
YEARS = range(2001, 2024 + 1)  # update '2024' as new years are added

for year in YEARS:
    url = BASE_URL.format(year=year)
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(os.path.join(SAVE_DIR, f"eia923_{year}.zip"), "wb") as f:
            f.write(response.content)
        print(f"Downloaded {year}")
    else:
        print(f"Data for {year} not found or not yet published.")
