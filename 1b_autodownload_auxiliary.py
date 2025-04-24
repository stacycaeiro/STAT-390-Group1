import os
import requests
# needs to be run seperately due to 404 client error when run within the main download script (the headers are needed but takes longer to run)

# Save directory
download_dir = "power_data_bulk/EPA_FLIGHT_GHGRP"
os.makedirs(download_dir, exist_ok=True)

# Enhanced browser-like headers
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.epa.gov/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}

# Dataset links and filenames
datasets = {
    "power-plant-crosswalk.xlsx": "https://www.epa.gov/system/files/documents/2022-04/ghgrp_oris_power_plant_crosswalk_12_13_21.xlsx"
}

# Try downloading each file with updated headers
for filename, url in datasets.items():
    file_path = os.path.join(download_dir, filename)
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"✅ Downloaded: {filename}")
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP Error for {filename}: {http_err}")
    except Exception as e:
        print(f"❌ Failed to download {filename}: {e}")