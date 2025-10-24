import requests
import logging
import json
import pandas as pd
import polars as pl
from datetime import datetime
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
    ## fetch first page
    df = pd.json_normalize(data['results'])
    df_pl = pl.from_pandas(df)
    logging.info(f"Data normalized successfully")
    # this Parquet file will be used for store all data retrieved from API locally
    filename = 'dividends_data.parquet'
    if os.path.exists(filename):
        existing_df = pl.scan_parquet(filename)
        combined_df = pl.concat([existing_df, df_pl.lazy()])
        combined_df.sink_parquet(filename)  # Direct write without collect
        logging.info(f"Data appended to existing Parquet file successfully")
    else:
        df_pl.write_parquet(filename)
        logging.info(f"Data written to new Parquet file successfully")
    #save_historical_data(df, filename)    
    logging.info(f"Data saved to dividends_data.parquet successfully")
    # this CSV file will be used for load data to Snowflake
    df.to_csv('dividends_data.csv', index=False, mode='w')
    logging.info(f"Data saved to dividends_data.csv successfully")
    df_csv = pd.read_csv('dividends_data.csv',dtype={
            'cash_amount': 'string',
            'currency': 'string',  # or 'object' for older pandas
            'dividend_type': 'string',
            'ex_dividend_date': 'string',
            'frequency': 'string',
            'id': 'string',
            'pay_date': 'string',
            'record_date': 'string',
            'ticker': 'string',
            'declaration_date': 'string'
        })
    logging.info(f"Data read from dividends_data.csv successfully")
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
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
    logging.info(f"Connected to Snowflake successfully")

    # create table if not exists
    create_table_query = """
    CREATE TABLE if not exists POLYGON.dividends (
        cash_amount string,
        currency string,
        dividend_type string,
        ex_dividend_date string,
        frequency string,
        id string,
        pay_date string,
        record_date string,
        ticker string,
        declaration_date string
    );
    """
    cs.execute(create_table_query)
    logging.info(f"Table DIVIDENDS created or already exists")
    # write data to snowflake
    success, nchunks, nrows, _ = write_pandas(
    conn, 
    df_csv, 
    'DIVIDENDS',
    overwrite=False,
    auto_create_table=False, # Assuming table already exists
    quote_identifiers=False)
    logging.info(f"Data written to Snowflake successfully: {success}, {nchunks} chunks, {nrows} rows")
    conn.close()
    logging.info(f"Connection to Snowflake closed")
"""def save_historical_data(df, filename='dividends_data.parquet'):
    if os.path.exists(filename):
        existing_df = pl.scan_parquet(filename)
        combined_df = pl.concat([existing_df, df.lazy()])
        combined_df.sink_parquet(filename)  # Direct write without collect
    else:
        df.write_parquet(filename)"""
if __name__ == "__main__":
    fetch_data(API_URL)