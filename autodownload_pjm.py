import requests

def download_pjm_csv(feed_name):
    url = f"https://dataminer2.pjm.com/feed/{feed_name}/csv"
    response = requests.get(url)
    if response.ok:
        filename = f"{feed_name}.csv"
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"✅ Downloaded: {filename}")
    else:
        print(f"❌ Failed: {feed_name} | Status code: {response.status_code}")

download_pjm_csv("gen_by_fuel")
download_pjm_csv("day_gen_capacity")