# -*- coding: utf-8 -*-
"""
Created on Mon Apr 21 15:33:55 2025

@author: blueg
"""

import os
import requests
import json

# Replace with your actual API key if different
API_KEY = "3zjKYxV86AqtJWSRoAECir1wQFscVu6lxXnRVKG8"
DOWNLOAD_DIR = "power_data_bulk/EIA_Hourly_Electric_Grid"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Actual API endpoints
API_ENDPOINTS = {
    "daily-region-data": "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/",
    "daily-region-sub-ba-data": "https://api.eia.gov/v2/electricity/rto/daily-region-sub-ba-data/data/",
    "daily-fuel-type-data": "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/",
    "daily-interchange-data": "https://api.eia.gov/v2/electricity/rto/daily-interchange-data/data/"
}


#having issues with these URLS
FAILING_ENDPOINTS = {
    "region-data": "https://api.eia.gov/v2/electricity/rto/region-data/data/",
    "fuel-type-data": "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/",
    "region-sub-ba-data": "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/",
    "interchange-data": "https://api.eia.gov/v2/electricity/rto/interchange-data/data/"
    }

# Shared query parameters
params1 = {
    "frequency": "daily",
    "data[0]": "value",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "offset": 0,
    "length": 5000,
    "api_key": API_KEY
}

# Download and save each dataset
for name, url in API_ENDPOINTS.items():
    print(f"📥 Downloading {name}...")
    try:
        response = requests.get(url, params=params1)
        response.raise_for_status()
        outpath = os.path.join(DOWNLOAD_DIR, f"{name}.json")
        with open(outpath, "w") as f:
            json.dump(response.json(), f, indent=2)
        print(f"✅ Saved to {outpath}")
    except Exception as e:
        print(f"❌ Failed to download {name}: {e}")
        
        
params2 = {
    "frequency": "hourly",
    "data[0]": "value",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "offset": 0,
    "length": 5000,
    "api_key": API_KEY
}

# Download and save each dataset
for name, url in FAILING_ENDPOINTS.items():
    print(f"📥 Downloading {name}...")
    try:
        response = requests.get(url, params=params2)
        response.raise_for_status()
        outpath = os.path.join(DOWNLOAD_DIR, f"{name}.json")
        with open(outpath, "w") as f:
            json.dump(response.json(), f, indent=2)
        print(f"✅ Saved to {outpath}")
    except Exception as e:
        print(f"❌ Failed to download {name}: {e}")

