import json
import requests


API_URL_BASE = 'https://api.coingecko.com/api/v3/'
SUCCESS_CODE = 200


def get_coin_list():
    response = requests.get(f'{API_URL_BASE}coins/list')

    if response.status_code != SUCCESS_CODE:
        raise ConnectionError(f'Code {response.status_code} received. Reason: {response.reason}\n URL: {response.url}')
    return json.loads(response.content)


if __name__ == '__main__':
    coins_list = get_coin_list()
    print(len(coins_list))
