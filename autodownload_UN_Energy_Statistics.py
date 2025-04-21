import requests

# URL to download data from
url = "https://data.un.org/ws/rest/data/UNSD,DF_UNDATA_ENERGY,1.2/.840../ALL/?detail=full&dimensionAtObservation=TIME_PERIOD&format=csv"

# Send HTTP GET request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Write the content to a local file
    filename = "UN_Energy_USA.csv"
    with open(filename, 'wb') as file:
        file.write(response.content)
    print(f"File downloaded successfully as {filename}")
else:
    print(f"Failed to download file. Status code: {response.status_code}")