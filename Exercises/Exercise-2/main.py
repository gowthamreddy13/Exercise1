def main():
    # your code here
    pass

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd

# Step 1: Web scraping to find the file URL

# URL of the target webpage
url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

# Send a GET request to the webpage
response = requests.get(url)

# Parse the HTML content
soup = BeautifulSoup(response.content, "html.parser")

# Find the link with the target date (e.g., 2022-02-07 14:03)
target_date = "2022-02-07 14:03"
target_link = None

# Find all the links on the webpage
links = soup.find_all("a")

# Iterate over the links and find the one corresponding to the target date
for link in links:
    if target_date in link.text:
        target_link = link
        break

# Check if the target link is found
if target_link is None:
    print(f"No file found for the date: {target_date}")
    exit()

# Step 2: Build the URL required to download the file

# Construct the download URL by joining the base URL with the target link's href
download_url = urljoin(url, target_link.get("href"))

# Step 3: Download the file and save it locally

# Send a GET request to download the file
file_response = requests.get(download_url)

# Determine the filename based on the URL
filename = download_url.split("/")[-1]

# Save the file locally
with open(filename, "wb") as file:
    file.write(file_response.content)

print(f"File '{filename}' downloaded successfully!")

# Step 4: Open the file with Pandas and find records with the highest HourlyDryBulbTemperature

# Read the downloaded file using Pandas
df = pd.read_csv(filename)

# Find the record(s) with the highest HourlyDryBulbTemperature
max_temp = df["HourlyDryBulbTemperature"].max()
max_temp_records = df[df["HourlyDryBulbTemperature"] == max_temp]

# Print the records with the highest temperature
print("Records with the highest HourlyDryBulbTemperature:")
print(max_temp_records)


if __name__ == "__main__":
    main()
