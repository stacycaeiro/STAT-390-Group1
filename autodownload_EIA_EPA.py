import os
import time
import requests

# Output directory
download_dir = "eia_epa_all_xls"
os.makedirs(download_dir, exist_ok=True)

# EIA Electric Power Annual table filenames
table_filenames = [
    "epa_01_01", "epa_01_02", "epa_01_03",
    "epa_02_01", "epa_02_02", "epa_02_11",
    "epa_03_01_a", "epa_03_01_b", "epa_03_02_a", "epa_03_02_b", "epa_03_03_a", "epa_03_03_b",
    "epa_03_04_a", "epa_03_04_b", "epa_03_05_a", "epa_03_05_b", "epa_03_06", "epa_03_07",
    "epa_03_08", "epa_03_09", "epa_03_10", "epa_03_11", "epa_03_12", "epa_03_13", "epa_03_14",
    "epa_03_15", "epa_03_16", "epa_03_17", "epa_03_18", "epa_03_19", "epa_03_20", "epa_03_21",
    "epa_03_22", "epa_03_23", "epa_03_24", "epa_03_25", "epa_03_26", "epa_03_27",
    "epa_04_01", "epa_04_02_a", "epa_04_02_b", "epa_04_03", "epa_04_04", "epa_04_05",
    "epa_04_06", "epa_04_07_a", "epa_04_07_b", "epa_04_07_c", "epa_04_08_a", "epa_04_08_b",
    "epa_04_08_c", "epa_04_09_a", "epa_04_09_b", "epa_04_10", "epa_04_11", "epa_04_12",
    "epa_04_13", "epa_04_14",
    "epa_05_01_a", "epa_05_01_b", "epa_05_01_c", "epa_05_01_d", "epa_05_01_e", "epa_05_01_f",
    "epa_05_02_a", "epa_05_02_b", "epa_05_02_c", "epa_05_02_d", "epa_05_02_e", "epa_05_02_f",
    "epa_05_03_a", "epa_05_03_b", "epa_05_03_c", "epa_05_03_d", "epa_05_03_e", "epa_05_03_f",
    "epa_05_04_a", "epa_05_04_b", "epa_05_04_c", "epa_05_04_d", "epa_05_04_e", "epa_05_04_f",
    "epa_05_05_d", "epa_05_05_e", "epa_05_05_f",
    "epa_05_06_a", "epa_05_06_b", "epa_05_06_c", "epa_05_06_d", "epa_05_06_e", "epa_05_06_f",
    "epa_05_07_a", "epa_05_07_b", "epa_05_07_c", "epa_05_07_d", "epa_05_07_e", "epa_05_07_f",
    "epa_05_08_d", "epa_05_08_e", "epa_05_08_f",
    "epa_05_09", "epa_05_10", "epa_05_11", "epa_05_12", "epa_05_13", "epa_05_14",
    "epa_10_01", "epa_10_02", "epa_10_03", "epa_10_04", "epa_10_05"
]

# Base URL
base_url = "https://www.eia.gov/electricity/annual/xls/{filename}.xlsx"

# Download loop
for name in table_filenames:
    url = base_url.format(filename=name)
    filepath = os.path.join(download_dir, f"{name}.xlsx")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"✅ Downloaded: {name}.xlsx")
    except requests.exceptions.HTTPError as http_err:
        print(f"⚠️ Skipped {name}.xlsx: HTTP error {http_err}")
    except Exception as e:
        print(f"❌ Failed {name}.xlsx: {e}")

    time.sleep(1)  # Politeness delay
