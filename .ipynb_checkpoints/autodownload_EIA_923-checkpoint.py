import os
import requests
import zipfile
from io import BytesIO
import pandas as pd

# Base directory to save files
base_dir = "EIA_Form_923"
os.makedirs(base_dir, exist_ok=True)

# Years to download (2001 to 2025)
years = range(2001, 2026)

for year in years:
    print(f"\n📁 Downloading and extracting year: {year}")
    try:
        # Adjust URL based on year
        if year == 2025:
            url = f"https://www.eia.gov/electricity/data/eia923/xls/f923_{year}.zip"
        elif year <= 2007:
            url = f"https://www.eia.gov/electricity/data/eia923/archive/xls/f906920_{year}.zip"            
        else:
            url = f"https://www.eia.gov/electricity/data/eia923/archive/xls/f923_{year}.zip"

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Extract ZIP file into year folder
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            year_folder = os.path.join(base_dir, str(year))
            os.makedirs(year_folder, exist_ok=True)
            z.extractall(year_folder)

        print(f"✅ Success: Extracted {year} into {year_folder}")

    except Exception as e:
        print(f"❌ Failed for {year}: {e}")
