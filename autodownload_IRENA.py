import os
import requests

# Set up download location
download_dir = "power_data_bulk/IRENA"
os.makedirs(download_dir, exist_ok=True)

# Download URL and filename
file_url = "https://www.irena.org/-/media/Files/IRENA/Agency/Publication/2024/Mar/IRENA_Stats_Extract_%202024_H1_V1.xlsx"
file_path = os.path.join(download_dir, "IRENA_Stats_Extract_2024_H1_V1.xlsx")

# Use browser-like headers
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.irena.org/"
}

# Download file
try:
    print("⬇️ Downloading IRENA spreadsheet...")
    response = requests.get(file_url, headers=headers)
    response.raise_for_status()

    with open(file_path, "wb") as f:
        f.write(response.content)

    print(f"✅ File saved to: {file_path}")

except Exception as e:
    print(f"❌ Download failed: {e}")

