from .get_URLs import get_next_url
from .fetch_data import fetch_data
import os
import dotenv
import logging
import time

dotenv.load_dotenv()
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    filename='/usr/local/airflow/include/Logs/stock_api_pipeline.log'
)

def main():
    urls = get_next_url()
    logging.info(f"Retrieved {len(urls)} URLs to fetch data from. --main_script.py")
    
    for i, url in enumerate(urls):
        fetch_data(url)
        logging.info(f"Data fetched and processed for URL: {url} --main_script.py")
        
        if i < len(urls) - 1:
            time.sleep(15)
            logging.info(f"Waiting 15 seconds before next request...")

if __name__ == "__main__":
    main()