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
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    filename='D:/Python/Polygon.api/stock-api-pipeline/logs/stock_api_pipeline.log'
)

def fetch_data(API_URL):
    logging.info(f"Fetching data from polygon.io API")
    response = requests.get(API_URL)
    
    # Check if request was successful
    if not response.ok:
        logging.error(f"API request failed with status code: {response.status_code}")
        return
    
    data = response.json()
    json_data = json.dumps(data, indent=4)
    logging.info(f"Data fetched successfully")
    
    # Define expected schema with ALL columns as String
    expected_columns = [
        'cash_amount', 'currency', 'declaration_date', 'dividend_type',
        'ex_dividend_date', 'frequency', 'id', 'pay_date', 'record_date', 'ticker'
    ]
    
    if 'results' in data and data['results']:
        # Create DataFrame from results
        df = pl.DataFrame(data['results'])
        
        # Convert all existing columns to string
        for col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.String))
        
        # Add missing columns with null string values
        for col in expected_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))
        
        # Select only the expected columns in consistent order
        df = df.select(expected_columns)
        
        logging.info(f"Data normalized successfully with {len(df)} records")
        
        # Convert to pandas for CSV operations
        df_pandas = df.to_pandas()
        
        # Ensure all pandas columns are string type
        for col in df_pandas.columns:
            df_pandas[col] = df_pandas[col].astype('string')
        
    else:
        logging.warning("No results found in API response")
        # Create empty DataFrame with expected schema
        df_pandas = pd.DataFrame(columns=expected_columns)
        for col in expected_columns:
            df_pandas[col] = df_pandas[col].astype('string')
        df = pl.DataFrame(schema={col: pl.String for col in expected_columns})
    
    # ===== PARQUET: Append to historical data =====
    parquet_filename = 'D:/Python/Polygon.api/stock-api-pipeline/Data/dividends_data.parquet'
    try:
        if os.path.exists(parquet_filename):
            # Read existing data and ensure schema consistency
            existing_df = pl.read_parquet(parquet_filename)
            
            # Convert existing data to string types and ensure consistent columns
            for col in existing_df.columns:
                existing_df = existing_df.with_columns(pl.col(col).cast(pl.String))
            
            # Add any missing columns to existing data
            for col in expected_columns:
                if col not in existing_df.columns:
                    existing_df = existing_df.with_columns(pl.lit(None).cast(pl.String).alias(col))
            
            # Combine with new data
            combined_df = pl.concat([existing_df.select(expected_columns), df.select(expected_columns)])
            combined_df.write_parquet(parquet_filename)
            logging.info(f"Data appended to existing Parquet file successfully. Total records: {len(combined_df)}")
        else:
            df.write_parquet(parquet_filename)
            logging.info(f"Data written to new Parquet file successfully. Records: {len(df)}")
    except Exception as e:
        logging.error(f"Error handling Parquet file: {e}")
        return
    
    # ===== CSV: Write ONLY new data (overwrite, don't append) =====
    csv_filename = 'D:/Python/Polygon.api/stock-api-pipeline/Data/dividends_data.csv'
    try:
        # Write only the NEW data to CSV (overwriting any existing data)
        df_pandas.to_csv(csv_filename, index=False)
        logging.info(f"New data written to CSV file (overwrote previous). Records: {len(df_pandas)}")
    except Exception as e:
        logging.error(f"Error writing CSV file: {e}")
        return
    
    # ===== SNOWFLAKE: Insert data from CSV =====
    try:
        # Read the CSV we just created (contains only new data)
        df_csv = pd.read_csv(csv_filename, dtype='string')
        
        # Ensure all expected columns exist in CSV
        for col in expected_columns:
            if col not in df_csv.columns:
                df_csv[col] = pd.NA
        
        df_csv = df_csv[expected_columns]
        logging.info(f"Data read from CSV for Snowflake insertion. Records: {len(df_csv)}")
        
        # Connect to Snowflake
        conn = snow.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        
        # Check connection
        logging.info(f"Connecting to Snowflake")
        cs = conn.cursor()
        cs.execute("SELECT CURRENT_VERSION()")
        one_row = cs.fetchone()
        logging.info(f"Snowflake version: {one_row[0]}")
        
        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS POLYGON.DIVIDENDS (
            cash_amount STRING,
            currency STRING,
            dividend_type STRING,
            ex_dividend_date STRING,
            frequency STRING,
            id STRING,
            pay_date STRING,
            record_date STRING,
            ticker STRING,
            declaration_date STRING
        );
        """
        cs.execute(create_table_query)
        logging.info(f"Table DIVIDENDS created or already exists")
        
        # Write data to Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            df_csv, 
            'DIVIDENDS',
            overwrite=False,
            auto_create_table=False,
            quote_identifiers=False
        )
        logging.info(f"Data written to Snowflake successfully: {success}, {nchunks} chunks, {nrows} rows")
        
        conn.close()
        logging.info(f"Connection to Snowflake closed")
        
        # ===== Clear CSV after successful Snowflake insertion =====
        try:
            # Create empty DataFrame with schema and write to CSV
            empty_df = pd.DataFrame(columns=expected_columns)
            for col in expected_columns:
                empty_df[col] = empty_df[col].astype('string')
            
            empty_df.to_csv(csv_filename, index=False)
            logging.info(f"CSV file cleared successfully after Snowflake insertion")
        except Exception as e:
            logging.error(f"Error clearing CSV file: {e}")
        
    except Exception as e:
        logging.error(f"Error with Snowflake operations: {e}")
        logging.warning(f"CSV file was NOT cleared due to Snowflake error")