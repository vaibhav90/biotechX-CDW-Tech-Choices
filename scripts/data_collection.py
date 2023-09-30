import requests
import pandas as pd
from time import sleep
from datetime import datetime, timedelta

# Define the base URL and parameters
url = "https://api.stackexchange.com/2.3/questions"
params = {
    "order": "desc",
    "sort": "creation",  # Changed sort parameter to "creation"
    "tagged": "dbt",
    "site": "stackoverflow",
    "pagesize": 100,
    "key": "",
}

# Initialize an empty list to store the data
data = []

# Calculate date range for the last 6 months
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=5000)
params['fromdate'] = int(start_date.timestamp())
params['todate'] = int(end_date.timestamp())

# Make API calls to collect data with pagination
page = 1
has_more = True
while has_more:
    params['page'] = page
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        response_data = response.json()
        data.extend(response_data['items'])

        # Check if there are more pages to fetch
        has_more = response_data['has_more']
        page += 1

        # Add a sleep time to avoid hitting rate limit
        sleep(2)
    else:
        print(f"Failed to fetch data for page {page}. Status code: {response.status_code}")
        break

# Convert the data to a pandas DataFrame
df = pd.DataFrame(data)

# Save the data to a CSV file
df.to_csv('data/dbt-questions.csv', index=False)

print(f"Data collection completed. {len(data)} questions collected.")
