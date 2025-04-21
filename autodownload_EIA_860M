import os
import requests

download_dir = "EIA_860M_All_Months"
os.makedirs(download_dir, exist_ok=True)

months = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]

# Try downloading from 2017 to 2025
for year in range(2017, 2026):
    for month in months:
        file_url = f"https://www.eia.gov/electricity/data/eia860m/xls/{month}_{year}.xls"
        file_path = os.path.join(download_dir, f"{month}_{year}.xls")

        print(f"📁 Downloading: {month.title()} {year}")
        try:
            response = requests.get(file_url, timeout=15)
            response.raise_for_status()
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"✅ Success: {file_path}")
        except Exception as e:
            print(f"❌ Failed: {file_url}\n   Reason: {e}")
