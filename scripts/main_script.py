from get_URLs import get_next_url
from fetch_data import fetch_data
import os
import dotenv
import logging
dotenv.load_dotenv()
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    filename='D:/Python/Polygon.api/stock-api-pipeline/logs/stock_api_pipeline.log'
)
def main():
    urls = get_next_url()
    logging.info(f"Retrieved {len(urls)} URLs to fetch data from. --main_script.py")
    for url in urls:
        fetch_data(url)
        logging.info(f"Data fetched and processed for URL: {url} --main_script.py")

if __name__ == "__main__":
    main()   