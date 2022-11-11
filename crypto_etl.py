from datetime import datetime
import json
import pandas as pd
from pathlib import Path
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
REPORTS_DIR = "reports"


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
        request_time = int(time())

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


def _get_last_price_timestamp(conn, vs_cur):
    last_price_timestamp = next(conn.execute('''
                SELECT last_request_at FROM latest_timestamp WHERE for_cur = ?
                ''', (vs_cur, )))[0]
    return last_price_timestamp


def _get_report_path(report_dir, report_name, timestamp):
    data_dt = datetime.fromtimestamp(timestamp)
    report_time = data_dt.strftime("%Y_%m_%d__%H_%M_%S")

    report_dir = Path(report_dir) / report_time
    report_dir.mkdir(parents=True, exist_ok=True)

    return report_dir / f'{report_name}.csv'


def _get_no_trade_in_cur_query(vs_cur, timestamp):
    sql_query = '''
        SELECT cc.id, cc.symbol, cc.name 
        FROM crypto_cur cc 
        LEFT JOIN market_price mp ON cc.id = mp.crypto_cur_id
        WHERE added_at = ? AND vs_cur = ? AND current_price is NULL
    '''
    params = (timestamp, vs_cur)

    return sql_query, params


def _get_24h_perc_change_report_query(vs_cur, perc, timestamp):
    sql_query = '''
        SELECT cc.id, cc.symbol, cc.name, price_change_percentage_24h_in_currency
        FROM crypto_cur cc 
        LEFT JOIN market_price mp ON cc.id = mp.crypto_cur_id
        WHERE added_at = ? AND vs_cur = ? AND 
            ABS(price_change_percentage_24h_in_currency) > ?
    '''
    params=(timestamp, vs_cur, perc)

    return sql_query, params


def generate_report(conn, report_type, **kwargs):
    vs_cur = kwargs.get('vs_cur', VS_CUR)
    timestamp = kwargs.get('timestamp', _get_last_price_timestamp(conn, vs_cur))

    match report_type:
        case 'no_trade_in_cur':
            report_name = f'crypto_cur_not_traded_vs_{vs_cur}'
            sql_query, query_params = _get_no_trade_in_cur_query(vs_cur, timestamp)
        case 'more_than_x_per_change_in_24h':
            perc = kwargs.get('perc', 0)
            report_name = f'crypto_cur_price_change_more_than_{perc}%_in_24h_vs_{vs_cur}'
            sql_query, query_params = _get_24h_perc_change_report_query(vs_cur, perc, timestamp)
        case _:
            raise NotImplementedError(f'Unknown Report type has been requested: {report_type}')

    report_base_dir = kwargs.get('report_dir', REPORTS_DIR)
    report_path =_get_report_path(report_base_dir, report_name, timestamp)

    report_data_df = pd.read_sql_query(sql_query, con=conn, params=query_params)
    report_data_df.to_csv(report_path, index=False)
    print(f'Report {report_name} has been generated.\nReport path: {report_path}\n')


if __name__ == '__main__':
    conn = connect_to_db()

    coins_list = get_coin_list()
    add_coins_to_db(coins_list, conn)

    market_data_gbp = get_market_data_all(VS_CUR)
    add_market_data_to_db(market_data_gbp, VS_CUR, conn)

    generate_report(conn, report_type='no_trade_in_cur', vs_cur=VS_CUR)
    generate_report(conn, report_type='more_than_x_per_change_in_24h', vs_cur=VS_CUR, perc=5.)
