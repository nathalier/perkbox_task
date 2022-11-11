import json
import pandas as pd
import requests
import sqlite3


API_URL_BASE = 'https://api.coingecko.com/api/v3/'
SUCCESS_CODE = 200


def get_coin_list():
    response = requests.get(f'{API_URL_BASE}coins/list')

    if response.status_code != SUCCESS_CODE:
        raise ConnectionError(f'Code {response.status_code} received. Reason: {response.reason}\n URL: {response.url}')
    return json.loads(response.content)


def add_coins_to_db(crypto_currencies, conn):
    coins_df = pd.DataFrame(crypto_currencies)
    coins_df.set_index('id')
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

    print(len(coins_list))
