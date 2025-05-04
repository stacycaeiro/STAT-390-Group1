import os
import requests
import json

# --- Configuration ---
API_KEY = 'czkmbYSuAiDcMKbziPMJCTHlu9FLROufxWINNgvl'
BASE_URL = 'https://api.epa.gov/easey/bulk-files/'
DATA_DIR = "facility_data_simple"
LOG_FILE = "facility_downloaded_simple.txt"

# --- Load downloaded files ---
downloaded_files = set()
if os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'r') as f:
        downloaded_files = set(line.strip() for line in f)

# --- Fetch metadata ---
def fetch_facility_metadata():
    response = requests.get(
        "https://api.epa.gov/easey/camd-services/bulk-files",
        params={'api_key': API_KEY}
    )
    response.raise_for_status()
    data = json.loads(response.content.decode('utf8').replace("'", '"'))
    return [f for f in data if f['metadata'].get('dataType') == 'Facility']

# --- Download a file ---
def download_file(file_obj):
    filename = file_obj['filename']
    url = BASE_URL + file_obj['s3Path']
    path = os.path.join(DATA_DIR, filename)

    if filename in downloaded_files:
        print(f"⏩ Skipping: {filename}")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    response = requests.get(url)
    response.raise_for_status()
    with open(path, 'wb') as f:
        f.write(response.content)

    with open(LOG_FILE, 'a') as log:
        log.write(filename + '\n')
    print(f"✅ Downloaded: {filename}")

# --- Main ---
if __name__ == '__main__':
    files = fetch_facility_metadata()
    print(f"📦 Found {len(files)} facility files.")
    for file in files:
        download_file(file)
    print("✅ All done.")
