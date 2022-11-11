## Crypto currency ETL

The repository contains simple script which implements the following scenario:
1. Download from CoinGecko free of charge service:
    b. List of supported coins and
    a. Market data of all coins with British Pound as a target currency.
2. Load the data in SQLite tables using Python and generate reports to:
    a. List all coins not traded in British Pounds
    b. Show the coins whose market capitalization changed by more than 5% in the last twenty four hours. The change could be positive or negative.


### To run the script:

1. Create virtual environment:  
    windows: `python -m venv venv`  
    *nix: `python3 -m venv venv`
2. Activate it:  
   windows: `venv\Scripts\activate`  
   *nix:  `source venv/bin/activate`
3. Install requirements:
    `pip install -r requirements.txt`
4. Run script:
   `python crypto_etl.py`


### Notes:
Please use `3.9` or higher version of python






