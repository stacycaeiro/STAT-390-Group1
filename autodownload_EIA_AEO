import os
import requests

# Output directory
download_dir = "eia_aeo2025_all_xlsx"
os.makedirs(download_dir, exist_ok=True)

# Custom table IDs (you provided)
table_ids = [
    "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "16", "17", "18",
    "2.1", "2.2", "2.3", "2.4", "2.5", "2.6", "2.7", "2.8", "2.9",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
    "31", "32", "33", "34",
    "54", "54.1", "54.2", "54.3", "54.4", "54.5", "54.6", "54.7",
    "54.8", "54.9", "54.10", "54.11", "54.12", "54.13", "54.14",
    "54.15", "54.16", "54.17", "54.18", "54.19", "54.20", "54.21",
    "54.22", "54.23", "54.24", "54.25",
    "55",
    "56", "56.1", "56.2", "56.3", "56.4", "56.5", "56.6", "56.7",
    "56.8", "56.9", "56.10", "56.11", "56.12", "56.13", "56.14",
    "56.15", "56.16", "56.17", "56.18", "56.19", "56.20", "56.21",
    "56.22", "56.23", "56.24", "56.25",
    "70", "71"
]

# Determine if ID is supplemental (has a decimal or is not 1–18 inclusive)
def is_supplemental(table_id):
    try:
        return float(table_id) not in range(1, 19)
    except ValueError:
        return True

# Base URLs
main_base = "https://www.eia.gov/outlooks/aeo/excel/aeotab{ID}.xlsx"
supp_base = "https://www.eia.gov/outlooks/aeo/supplement/excel/suptab_{ID}.xlsx"

# Download loop
for table_id in table_ids:
    if is_supplemental(table_id):
        url = supp_base.format(ID=table_id)
        filename = f"suptab_{table_id.replace('.', '_')}.xlsx"
    else:
        url = main_base.format(ID=table_id)
        filename = f"aeotab{table_id}.xlsx"

    filepath = os.path.join(download_dir, filename)

    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"✅ Downloaded: {filename}")
    except requests.exceptions.HTTPError as http_err:
        print(f"⚠️ Skipped {filename}: HTTP error {http_err}")
    except Exception as e:
        print(f"❌ Failed {filename}: {e}")
