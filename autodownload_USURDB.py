import requests
import pandas as pd
import time
import os
import json

API_KEY = 'lwbBNDlSx1Oyq67BZ7kYd5M0J4RzitNJezCX4XIj'

# Output directory
base_dir = 'usurdb_flattened'

# Subfolders for each CSV type
subfolders = {
    'energyratestructure': os.path.join(base_dir, 'energyratestructure'),
    'demandratestructure': os.path.join(base_dir, 'demandratestructure'),
    'weekday_schedule': os.path.join(base_dir, 'weekday_schedule'),
    'weekend_schedule': os.path.join(base_dir, 'weekend_schedule')
}

# Create subfolders
for folder in subfolders.values():
    os.makedirs(folder, exist_ok=True)

states = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]

base_url = 'https://api.openei.org/utility_rates'

for state in states:
    print(f"Fetching {state}...")

    try:
        response = requests.get(base_url, params={
            'version': 'latest',
            'format': 'json',
            'api_key': API_KEY,
            'state': state,
            'detail': 'full',
            'limit': 1000
        })
        response.raise_for_status()
        data = response.json()

        if 'items' in data and data['items']:
            energy_rows, demand_rows, weekday_sched_rows, weekend_sched_rows = [], [], [], []

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

                # Energyratestructure
                for rate_index, tier_group in enumerate(item.get('energyratestructure', [])):
                    for tier_index, tier in enumerate(tier_group):
                        energy_rows.append({
                            **base_info,
                            'rate_index': rate_index,
                            'tier_index': tier_index,
                            'rate': tier.get('rate'),
                            'unit': tier.get('unit'),
                            'max': tier.get('max'),
                            'min': tier.get('min'),
                            'adjustment': tier.get('adjustment'),
                            'sell': tier.get('sell')
                        })

                # Demandratestructure
                for rate_index, tier_group in enumerate(item.get('demandratestructure', [])):
                    for tier_index, tier in enumerate(tier_group):
                        demand_rows.append({
                            **base_info,
                            'rate_index': rate_index,
                            'tier_index': tier_index,
                            'rate': tier.get('rate'),
                            'unit': tier.get('unit'),
                            'max': tier.get('max'),
                            'min': tier.get('min'),
                            'adjustment': tier.get('adjustment')
                        })

                # Weekday schedule
                for tier_index, hourly in enumerate(item.get('energyweekdayschedule', [])):
                    for hour, period in enumerate(hourly):
                        weekday_sched_rows.append({
                            **base_info,
                            'tier_index': tier_index,
                            'hour': hour,
                            'period': period
                        })

                # Weekend schedule
                for tier_index, hourly in enumerate(item.get('energyweekendschedule', [])):
                    for hour, period in enumerate(hourly):
                        weekend_sched_rows.append({
                            **base_info,
                            'tier_index': tier_index,
                            'hour': hour,
                            'period': period
                        })

            # Save to respective subfolder
            if energy_rows:
                pd.DataFrame(energy_rows).to_csv(os.path.join(subfolders['energyratestructure'], f'{state}.csv'), index=False)
            if demand_rows:
                pd.DataFrame(demand_rows).to_csv(os.path.join(subfolders['demandratestructure'], f'{state}.csv'), index=False)
            if weekday_sched_rows:
                pd.DataFrame(weekday_sched_rows).to_csv(os.path.join(subfolders['weekday_schedule'], f'{state}.csv'), index=False)
            if weekend_sched_rows:
                pd.DataFrame(weekend_sched_rows).to_csv(os.path.join(subfolders['weekend_schedule'], f'{state}.csv'), index=False)

            print(f"Saved all for {state}")
        else:
            print(f"No data for {state}.")

    except Exception as e:
        print(f"Error in {state}: {e}")

    time.sleep(1)