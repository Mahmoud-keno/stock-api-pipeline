import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd
import logging
import time
load_dotenv()
from fetch_data import fetch_data
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    filename='D:/Python/Polygon.api/stock-api-pipeline/logs/stock_api_pipeline.log'
)
def get_next_url():
    API_URL = f"https://api.polygon.io/v3/reference/dividends?apiKey={os.getenv('API_KEY')}&limit={os.getenv('LIMIT')}"
    successful_urls = []
    
    current_url = API_URL
    
    try:
        # Get initial response
        response = requests.get(current_url)
        response.raise_for_status()  # This will raise an exception for 4xx/5xx status codes
        data = response.json()
        
        successful_urls.append(current_url)
        
        # Process next URLs with rate limiting
        for count in range(4):  # Maximum 4 next URLs
            # Check if there's a next_url in the response
            if 'next_url' not in data or not data['next_url']:
                break
                
            # Construct next URL with API key
            next_url = data['next_url'] + "&apiKey=" + os.getenv('API_KEY')
            
            # Add delay to avoid rate limiting (adjust as needed)
            time.sleep(2)  # 2 second delay between requests
            
            # Make the next request
            response = requests.get(next_url)
            
            # Only add to successful URLs if the request was successful
            if response.status_code >= 200 and response.status_code < 300:
                successful_urls.append(next_url)
                data = response.json()
            else:
                logging.warning(f"Request failed with status {response.status_code} for URL: {next_url} -- get_URLs.py")
                break
                
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logging.error("Rate limit exceeded. Please wait before making more requests. -- get_URLs.py")
        else:
            logging.error(f"HTTP error occurred: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error occurred: {e}")
    except KeyError as e:
        logging.error(f"Key error - expected key not found in response: {e} -- get_URLs.py")
    except Exception as e:
        logging.error(f"Unexpected error: {e} -- get_URLs.py")
    
    return successful_urls



