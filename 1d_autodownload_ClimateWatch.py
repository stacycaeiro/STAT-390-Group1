import requests
import pandas as pd

url = "https://www.climatewatchdata.org/api/v1/data/historical_emissions"

params = {
    "country": "WORLD",             # 🌍 This is the key!
    "source": "CAIT",               # Default source
    "gas": "all_ghg",               # Matches UI default
    "sector": "total_including_lucf",
    "start_year": 1990,
    "end_year": 2021
}

response = requests.get(url, params=params)
response.raise_for_status()

data = response.json()["data"]

# Flatten and display
records = []
for item in data:
    for emission in item.get("emissions", []):
        records.append({
            "year": emission["year"],
            "value": emission["value"],
            "country": item["country"],
            "gas": item["gas"],
            "sector": item["sector"],
            "source": item["data_source"]
        })

df = pd.DataFrame(records)
df.to_csv("global_ghg_emissions_world_CAIT.csv", index=False)