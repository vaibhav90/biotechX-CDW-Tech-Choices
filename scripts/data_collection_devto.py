import requests
import json
import pandas as pd

# API endpoint for searching articles on dev.to
URL = "https://dev.to/api/articles"

# Parameters for the GET request
params = {
    "tag": "etl",  # Search articles tagged with "etl"
    "per_page": 100  # Number of articles per page
}

# Make the GET request
response = requests.get(URL, params=params)

# Initialize list to hold article titles related to "ETL pipelines"
etl_pipeline_articles_titles = []

# Check if the request was successful
if response.status_code == 200:
    articles = json.loads(response.text)

    # Filter articles to include only those that contain "pipeline" in the title
    etl_pipeline_articles = [article for article in articles if "ETL" in article["title"].lower()]

    # Populate list with titles of the filtered articles
    for article in etl_pipeline_articles:
        etl_pipeline_articles_titles.append(article["title"])

    # If no articles match the criteria, print a message
    if not etl_pipeline_articles_titles:
        print("No articles related to 'ETL pipelines' were found.")
    else:
        # Convert the list of titles to a DataFrame
        etl_titles_df = pd.DataFrame(etl_pipeline_articles_titles, columns=["Article Titles"])

        # Save the DataFrame to a CSV file
        etl_titles_df.to_csv('etl_pipeline_articles_titles.csv', index=False)

        print("Article titles have been saved to 'etl_pipeline_articles_titles.csv'")
else:
    print("Failed to fetch data.")
