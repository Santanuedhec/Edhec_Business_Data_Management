import requests
import os
import logging
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load API settings from .env file
load_dotenv()

API_URL = os.getenv("EXCHANGE_API_URL")
if not API_URL:
    raise ValueError("EXCHANGE_API_URL is not set in the .env file.")

def fetch_exchange_rates():
    """
    Fetches the latest exchange rates with EUR as the base currency.

    :return: Dictionary of exchange rates or None if request fails.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Catch HTTP errors

        try:
            data = response.json()
        except ValueError:
            logger.error("Invalid JSON response from Exchange Rate API.")
            return None

        if "rates" in data:
            return data["rates"]
        else:
            logger.error("Error: 'rates' key missing in API response.")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching exchange rates: {e}")
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
        if not isinstance(price, (int, float)):
            logger.error(f"Invalid price: {price}. Must be a number.")
            return price

        if currency in exchange_rates:
            return round(price / exchange_rates[currency], 2)
        else:
            logger.warning(f"Warning: Currency {currency} not found. Returning original price.")
            return price  # If currency is not found, return original price
    except Exception as e:
        logger.error(f"Error converting currency: {e}")
        return price
    
if __name__ == "__main__":
    print(" Testing exchange_rate.py...")

    # Test Fetch Exchange Rates
    rates = fetch_exchange_rates()
    if rates:
        print(" Exchange rates fetched successfully:", rates)
    else:
        print(" Failed to fetch exchange rates.")

    # Test Currency Conversion
    test_price = 100  # Example price
    test_currency = "USD"

    if rates:
        converted_price = convert_price_to_eur(test_price, test_currency, rates)
        print(f" Converted {test_price} {test_currency} to EUR: {converted_price}")
    else:
        print(" Currency conversion test skipped due to missing rates.")
