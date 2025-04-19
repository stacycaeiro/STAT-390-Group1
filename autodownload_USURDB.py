import requests
import pandas as pd
import time
import os
import json

API_KEY = 'lwbBNDlSx1Oyq67BZ7kYd5M0J4RzitNJezCX4XIj'

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

for state in states:
    params = {
        'version': 'latest',
        'format': 'json',
        'api_key': API_KEY,
        'state': state,
        'detail': 'full',
        'limit': 1000
    }

    print(f"Fetching {state}...")

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'items' in data and data['items']:
            flat_rows = []
            for item in data['items']:
                base_info = {
                    'utility_name': item.get('utility', ''),
                    'rate_name': item.get('name', ''),
                    'sector': item.get('sector', ''),
                    'startdate': item.get('startdate', ''),
                    'enddate': item.get('enddate', ''),
                    'state': state,
                    'eiaid': item.get('eiaid', ''),
                    'rate_id': item.get('uri', '')
                }

                # Flatten each tier in energyratestructure
                ers = item.get('energyratestructure', [])
                for rate_index, tier_group in enumerate(ers):
                    for tier_index, tier in enumerate(tier_group):
                        row = base_info.copy()
                        row.update({
                            'rate_index': rate_index,
                            'tier_index': tier_index,
                            'rate': tier.get('rate', None),
                            'unit': tier.get('unit', None),
                            'max': tier.get('max', None),
                            'min': tier.get('min', None),
                            'adjustment': tier.get('adjustment', None),
                            'sell': tier.get('sell', None)
                        })
                        flat_rows.append(row)

            df = pd.DataFrame(flat_rows)
            df.to_csv(os.path.join(output_dir, f'usurdb_{state}_energyratestructure_flat.csv'), index=False)
            print(f"Saved: usurdb_{state}_energyratestructure_flat.csv")
        else:
            print(f"No data for {state}.")

    except Exception as e:
        print(f"Error in {state}: {e}")

    time.sleep(1)