import os
import requests
import zipfile
from io import BytesIO

# Base directory to save files
base_dir = "eia_923_bulk_data"
os.makedirs(base_dir, exist_ok=True)

# Years to download
years = range(2008, 2026)

for year in years:
    print(f"📁 Downloading year: {year}")
    try:
        # Adjust URL based on year
        if year == 2025:
            url = f"https://www.eia.gov/electricity/data/eia923/xls/f923_{year}.zip"
        else:
            url = f"https://www.eia.gov/electricity/data/eia923/archive/xls/f923_{year}.zip"
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Save and unzip in memory
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            year_folder = os.path.join(base_dir, str(year))
            os.makedirs(year_folder, exist_ok=True)
            z.extractall(year_folder)

        print(f"✅ Success: {year}")

    except Exception as e:
        print(f"❌ Failed for {year}: {e}")
