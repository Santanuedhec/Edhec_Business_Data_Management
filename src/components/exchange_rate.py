import requests
import os
import logging
import pandas as pd
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load API settings from .env file
load_dotenv()

API_URL = os.getenv("EXCHANGE_API_URL")
if not API_URL:
    raise ValueError(" EXCHANGE_API_URL is not set in the .env file.")

def fetch_exchange_rates():
    """
    Fetches the latest exchange rates with EUR as the base currency.

    :return: Dictionary of exchange rates or None if request fails.
    """
    try:
        response = requests.get(API_URL, timeout=10) 
        response.raise_for_status()  

        try:
            data = response.json()
        except ValueError:
            logger.error(" Invalid JSON response from Exchange Rate API.")
            return None

        if "rates" in data:
            logger.info(" Successfully fetched exchange rates.")
            return data["rates"]
        else:
            logger.error(" 'rates' key missing in API response.")
            return None

    except requests.exceptions.Timeout:
        logger.error(" Exchange Rate API request timed out.")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f" Error fetching exchange rates: {e}")
        return None

def convert_price_to_eur(price, currency, exchange_rates):
    """
    Converts a given price from another currency to EUR.

    :param price: Price in original currency.
    :param currency: The original currency code (e.g., 'USD', 'GBP').
    :param exchange_rates: Dictionary of exchange rates with EUR as base.
    :return: Converted price in EUR.
    """
    try:
        # Convert non-numeric prices to NaN and replace with 0
        price = pd.to_numeric(price, errors="coerce")
        if pd.isna(price) or price is None:
            logger.error(f" Invalid price: {price}. Must be a numeric value. Defaulting to 0.")
            return 0  # Ensures non-numeric values don’t break conversion

        if currency in exchange_rates:
            converted_price = round(price / exchange_rates[currency], 2)
            logger.info(f" Converted {price} {currency} to {converted_price} EUR.")
            return converted_price
        else:
            logger.warning(f" Currency {currency} not found in exchange rates. Defaulting to 0 EUR.")
            return 0  # Ensures missing currency doesn’t return an incorrect value
    except Exception as e:
        logger.error(f" Error converting currency: {e}")
        return 0  # Fails safely


# Self-Test Code (for debugging)
if __name__ == "__main__":
    print("\n Testing exchange_rate.py...")

    # Test Fetch Exchange Rates
    rates = fetch_exchange_rates()
    if rates:
        print(" Exchange rates fetched successfully:", rates)
    else:
        print(" Failed to fetch exchange rates.")

    # Test Currency Conversion
    test_price = "100"  # Simulating a string input to check conversion
    test_currency = "USD"

    if rates:
        converted_price = convert_price_to_eur(test_price, test_currency, rates)
        print(f" Converted {test_price} {test_currency} to EUR: {converted_price}")
    else:
        print(" Currency conversion test skipped due to missing exchange rates.")
