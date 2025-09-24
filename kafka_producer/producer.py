import time
import requests
import json
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from a .env file
load_dotenv()

# --- Configuration from Environment Variables ---
# API key for authentication with the Finnhub API
API_KEY = os.getenv('API_KEY')

# Base URL for the Finnhub stock quotes API endpoint
BASE_URL = os.getenv('BASE_URL')

# Kafka broker address and topic name from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

# List of stock symbols to fetch data for
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']  
# Fetch data every 60 seconds (default value if not in .env)
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL',60))  

# Initialize the Kafka producer
# The bootstrap_servers parameter specifies the Kafka broker to connect to.
# The value_serializer converts the Python dictionary to a JSON string and encodes it to UTF-8 bytes.
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_stock_data(symbol):
    """
    Fetches real-time stock data for a given symbol from the Finnhub API.

    Args:
        symbol (str): The stock ticker symbol (e.g., 'AAPL').

    Returns:
        dict or None: A dictionary containing the formatted stock data if the request is successful,
                      otherwise returns None.
    """
    # Construct the full API URL with the symbol and API key
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    print(f"Fetching data for {symbol} from {url}")
    
    # Make a GET request to the API endpoint
    response = requests.get(url)
    print(f"Response status: {response.status_code}")
    
    # Check if the API request was successful (status code 200)
    if response.status_code == 200:
        data = response.json()
        
        # Create a new dictionary with user-friendly key names and a formatted timestamp
        renamed_data = {
            'current_price': data.get('c'),
            'change': data.get('d'),
            'percent_change': data.get('dp'),
            'high': data.get('h'),
            'low': data.get('l'),
            'open': data.get('o'),
            'previous_close_price': data.get('pc'),
            'symbol': symbol,
            'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return renamed_data
    else:
        # Print an error message if the API request failed
        print(f"Error fetching data for {symbol}: {response.status_code}")
        return None

def producer_main():
    """
    Main function to continuously fetch stock data and send it to Kafka.
    """
    # Infinite loop to keep the script running
    while True:
        try:
            # Iterate through each stock symbol in the list
            for symbol in SYMBOLS:
                stock_data = fetch_stock_data(symbol)
                
                # If data was successfully fetched, send it to the Kafka topic
                if stock_data:
                    print(f"Fetched data for {symbol}: {stock_data}")
                    producer.send(KAFKA_TOPIC, value=stock_data)
                    print(f"Sent data to Kafka for {symbol}")
            
            # Wait for the specified interval before fetching the next set of data
            time.sleep(int(FETCH_INTERVAL))
        
        except Exception as e:
            # Catch and print any exceptions that occur to prevent the script from crashing
            print(f"An error occurred: {e}")
            time.sleep(5)  # Wait a few seconds before retrying

# Entry point for the script
if __name__ == "__main__":
    producer_main()
