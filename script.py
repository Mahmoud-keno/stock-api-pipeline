import requests
import logging
import json
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
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
    
    # connect to snowflake
    conn = snow.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    #check connection
    logging.info(f"Connecting to Snowflake")
    cs = conn.cursor()
    try:
        cs.execute("SELECT CURRENT_VERSION()")
        one_row = cs.fetchone()
        logging.info(f"Snowflake version: {one_row[0]}")
    finally:
        cs.close()
    logging.info(f"Connected to Snowflake successfully")

    # write data to snowflake
    success, nchunks, nrows, _ = write_pandas(
    conn, 
    df, 
    'DIVIDENDS',
    overwrite=False,
    auto_create_table=False  # Assuming table already exists
    )
    logging.info(f"Data written to Snowflake successfully: {success}, {nchunks} chunks, {nrows} rows")
    conn.close()
    logging.info(f"Connection to Snowflake closed")

if __name__ == "__main__":
    fetch_data(API_URL)