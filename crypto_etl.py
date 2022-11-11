import json
import pandas as pd
import requests
import sqlite3
from time import sleep


API_URL_BASE = 'https://api.coingecko.com/api/v3/'
SUCCESS_CODE = 200
PAGINATION_START = 1
MAX_PER_PAGE = 250
RETRY_TIMEOUT = 125
API_LIMIT_ERROR_CODES = [429, 502, 503, 504]
VS_CUR = 'gbp'


class ApiLimitError(Exception):
    pass


def retry_after_timeout(f):
    def retry(*args, **kwargs):
        retry_ = True
        while retry_:
            try:
                result = f(*args, **kwargs)
                retry_ = False
            except ApiLimitError as e:
                sleep(RETRY_TIMEOUT)
            except Exception as e:
                raise e
        return result
    return retry
        

@retry_after_timeout
def get_coin_list():
    response = requests.get(f'{API_URL_BASE}coins/list')

    if response.status_code in API_LIMIT_ERROR_CODES:
        raise ApiLimitError
    elif response.status_code != SUCCESS_CODE:
        raise ConnectionError(f'Code {response.status_code} received. Reason: {response.reason}\n URL: {response.url}')
    return json.loads(response.content)


@retry_after_timeout
def get_market_data_page(currency, price_change_period, page):
    payload = {'vs_currency': currency.lower(), 'price_change_percentage': price_change_period, 
                'page': page, 'per_page': MAX_PER_PAGE, 'order': 'id_asc'}
    response = requests.get(f'{API_URL_BASE}coins/markets', params=payload)
    
    if response.status_code  in API_LIMIT_ERROR_CODES:
        raise ApiLimitError
    elif response.status_code != SUCCESS_CODE:
        raise ConnectionError(f'Code {response.status_code} received. Reason: {response.reason}\n URL: {response.url}')
    return json.loads(response.content)


def get_market_data_all(currency, price_change_period='24h'):
    page = PAGINATION_START
    next_page = True
    market_data = []

    while next_page:
        next_data = get_market_data_page(currency, price_change_period, page)
        if next_data:
            market_data.extend(next_data)
            print(f'Page {page} done...')
            page += 1
        else:
            next_page = False

    return market_data


def add_coins_to_db(crypto_currencies, conn):
    coins_df = pd.DataFrame(crypto_currencies)
    coins_df = coins_df.set_index('id')
    coins_df.to_sql(name='crypto_cur', con=conn, if_exists='replace', method='multi')


def prepare_db(conn):
    conn.executescript('''
            CREATE TABLE IF NOT EXISTS crypto_cur (
                id	TEXT NOT NULL PRIMARY KEY UNIQUE,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL );
            ''')
    conn.commit()


def connect_to_db(db_name='crypto_market.sqlite'):
    conn = sqlite3.connect(db_name)
    prepare_db(conn)
    return conn


if __name__ == '__main__':
    conn = connect_to_db()

    coins_list = get_coin_list()
    add_coins_to_db(coins_list, conn)

    market_data_gbp = get_market_data_all(VS_CUR)

    print(len(coins_list))
    print(len(market_data_gbp))
