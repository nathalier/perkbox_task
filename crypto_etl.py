import json
import pandas as pd
import requests
import sqlite3
from time import time, sleep


API_URL_BASE = 'https://api.coingecko.com/api/v3/'
SUCCESS_CODE = 200
PAGINATION_START = 1
MAX_PER_PAGE = 250
RETRY_TIMEOUT = 125
API_LIMIT_ERROR_CODES = [429, 502, 503, 504]
VS_CUR = 'gbp'
DB_INSERT_CHUNKS = 100


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
    
    if response.status_code in API_LIMIT_ERROR_CODES:
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
    coins_df.to_sql(name='crypto_cur', con=conn, if_exists='replace', 
                    chunksize=DB_INSERT_CHUNKS, method='multi')


def update_latest_timestamp(timestamp, vs_cur, conn):
    conn.execute('''INSERT OR REPLACE INTO latest_timestamp (for_cur, last_request_at) VALUES (?, ?)''', 
        (vs_cur, timestamp))
    conn.commit()


def add_market_data_to_db(data, vs_cur, conn, request_time=None):
    if request_time is None:
        request_time = int(time() * 1000)

    market_data_df = pd.DataFrame(data)
    market_data_df = market_data_df[['id', 'current_price', 'price_change_percentage_24h_in_currency']]
    market_data_df = market_data_df.rename(columns={'id': 'crypto_cur_id'})
    market_data_df['vs_cur'] = vs_cur
    market_data_df['added_at'] = request_time
    market_data_df.to_sql(name='market_price', con=conn, if_exists='replace', 
                          chunksize=DB_INSERT_CHUNKS, method='multi')
    update_latest_timestamp(request_time, vs_cur, conn)


def prepare_db(conn):
    conn.executescript('''
            CREATE TABLE IF NOT EXISTS crypto_cur (
                id	TEXT NOT NULL PRIMARY KEY UNIQUE,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL );
            CREATE TABLE IF NOT EXISTS market_price (
                crypto_cur_id	TEXT NOT NULL,
                vs_cur  TEXT NOT NULL,
                added_at  INTEGER,
                current_price  REAL,
                price_change_percentage_24h_in_currency  REAL
                );
            CREATE TABLE IF NOT EXISTS latest_timestamp (
                for_cur  TEXT NOT NULL PRIMARY KEY UNIQUE,
                last_request_at  INTEGER NOT NUll
            );
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
    add_market_data_to_db(market_data_gbp, VS_CUR, conn)

    print(len(coins_list))
    print(len(market_data_gbp))
