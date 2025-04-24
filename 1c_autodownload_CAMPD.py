# Needs to be run multiple times, waiting a while in between due to 429 Client errors with limit on downloads


import os
import requests
import json
import time
import random
from threading import Lock

# --- Configuration ---
API_KEYS = ['czkmbYSuAiDcMKbziPMJCTHlu9FLROufxWINNgvl', 'pVWeIc5vp4ejeGeRJzk9KN9G4qgD9CW2iaib7N76']  # Add more if available
BASE_URL = 'https://api.epa.gov/easey/bulk-files/'
LOG_FILE = "downloaded_files.txt"
FAIL_LOG_FILE = "failed_downloads.txt"
DATA_DIR = "CAMPD"
key_index = 0

# --- Load downloaded files ---
downloaded_files = set()
if os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'r') as f:
        downloaded_files = set(line.strip() for line in f)

failed_files = []

# --- Safe fetch of metadata with API key rotation ---
def fetch_metadata_with_rotation():
    global key_index
    attempts = 0
    max_attempts = len(API_KEYS)

    while attempts < max_attempts:
        key = API_KEYS[key_index]
        key_index = (key_index + 1) % len(API_KEYS)

        try:
            response = requests.get(
                "https://api.epa.gov/easey/camd-services/bulk-files",
                params={'api_key': key}
            )
            response.raise_for_status()
            return json.loads(response.content.decode('utf8').replace("'", '"'))
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                print(f"API key {key} rate-limited (429). Trying next...")
                attempts += 1
                time.sleep(1.5)
            else:
                raise
    raise Exception("All API keys exhausted due to rate-limiting.")

# --- Retry-enabled download with jittered exponential backoff ---
def safe_download_with_retry(url, retries=5, base_delay=2.0):
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                wait = base_delay * (attempt + 1) + random.uniform(0, 1.0)
                print(f"⚠️ Rate limited (429). Retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                raise
    raise Exception(f"❌ Failed after {retries} retries due to rate limiting.")

# --- Download single file ---
def download_file(file_obj):
    filename = file_obj['filename']
    data_type = file_obj['metadata'].get('dataType', 'Unknown').replace(" ", "_").replace("(", "").replace(")", "")
    state = file_obj['metadata'].get('stateCode', 'ALL')
    url = BASE_URL + file_obj['s3Path']
    dest_dir = os.path.join(DATA_DIR, data_type, state)
    dest_path = os.path.join(dest_dir, filename)

    if filename in downloaded_files:
        return

    try:
        os.makedirs(dest_dir, exist_ok=True)
    except Exception as e:
        print(f"⚠️ Failed to create directory {dest_dir}: {e}")
        return

    time.sleep(2.0)  # Rate-limiting protection

    try:
        r = safe_download_with_retry(url)
        with open(dest_path, 'wb') as f:
            f.write(r.content)

        with open(LOG_FILE, 'a') as log:
            log.write(filename + '\n')
        downloaded_files.add(filename)
        print(f"✅ Downloaded: {filename}")

    except Exception as e:
        print(f"❌ Failed to download {filename}: {e}")
        failed_files.append(filename)

# --- Main execution ---
if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    bulk_files = fetch_metadata_with_rotation()
    files_to_download = [f for f in bulk_files if f['filename'] not in downloaded_files]

    print(f"📦 Total files to download: {len(files_to_download)}")
    for file in files_to_download:
        download_file(file)

    if failed_files:
        with open(FAIL_LOG_FILE, 'w') as f:
            for fname in failed_files:
                f.write(fname + '\n')
        print(f"⚠️ {len(failed_files)} downloads failed. See {FAIL_LOG_FILE} for details.")
    else:
        print("✅ All files downloaded successfully.")
