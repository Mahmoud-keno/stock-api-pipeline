import requests
import logging
import json
import pandas as pd
from dotenv import load_dotenv
import os
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='stock_api_pipeline.log')
API_URL = f"https://api.polygon.io/v3/reference/dividends?apiKey={os.getenv('API_KEY')}&limit={os.getenv('LIMIT')}"
def fetch_data(API_URL):
    logging.info(f"Fetching data from polygon.io API")
    response = requests.get(API_URL)
    data = response.json()
    json_data = json.dumps(data, indent=4)
    # print first result from results key
    #print(data['results'][0])
    logging.info(f"Data fetched successfully")

    df = pd.json_normalize(data['results'])
    logging.info(f"Data normalized successfully")
    df.to_csv('dividends_data.csv', index=False, mode='a')
    logging.info(f"Data saved to dividends_data.csv successfully")
    



if __name__ == "__main__":
    fetch_data(API_URL)