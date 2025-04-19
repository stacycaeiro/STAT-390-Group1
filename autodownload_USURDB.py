import requests
import pandas as pd
import time
import os
import json

API_KEY = 'lwbBNDlSx1Oyq67BZ7kYd5M0J4RzitNJezCX4XIj'

# Create folder to store CSVs
output_dir = 'usurdb_data'
os.makedirs(output_dir, exist_ok=True)

states = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]

base_url = 'https://api.openei.org/utility_rates'

# Nested fields to flatten
nested_fields = ['energyratestructure', 'energyweekdayschedule', 'energyweekendschedule', 'demandratestructure']

for state in states:
    params = {
        'version': 'latest',
        'format': 'json',
        'api_key': API_KEY,
        'state': state,
        'detail': 'full',
        'limit': 1000
    }

    print(f"Fetching data for {state}...")

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'items' in data and data['items']:
            records = []
            for item in data['items']:
                flat = item.copy()
                for key in nested_fields:
                    if key in flat:
                        flat[key] = json.dumps(flat[key])  # Convert nested lists/dicts to string
                records.append(flat)

            df = pd.DataFrame(records)
            filename = os.path.join(output_dir, f'usurdb_{state}_rates.csv')
            df.to_csv(filename, index=False)
            print(f"Saved: {filename}")

        else:
            print(f"No data for {state}.")

    except Exception as e:
        print(f"Error fetching data for {state}: {e}")

    time.sleep(1)