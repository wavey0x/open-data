import constants 
from brownie import ZERO_ADDRESS, Contract, web3, accounts, chain
import constants, requests, json
from datetime import datetime

DAY = 60 * 60 * 24
WEEK = DAY * 7
token_locker = Contract(constants.TOKEN_LOCKER)

def get_week_by_ts(ts):
    first_week = 1691625600
    if ts < first_week:
        raise Exception("timestamp is before protocol launch")
    diff = ts - first_week
    return diff // WEEK

def get_week_start_block(week_number=0):
    ts = get_week_start_ts(week_number)
    return closest_block_after_timestamp(ts)

def get_week_start_ts(week_number=0):
    current_week = token_locker.getWeek()
    offset = abs(current_week - week_number)
    current_week_start_ts = int(chain.time() / WEEK) * WEEK
    if week_number <= current_week:
        return int(current_week_start_ts - (WEEK * offset))
    else:
        return int(current_week_start_ts + (WEEK * offset))

def get_week_end_block(week_number=0):
    current_week = token_locker.getWeek()
    if week_number == current_week:
        return chain.height
    ts = get_week_start_ts(week_number) + WEEK
    return closest_block_after_timestamp(ts) - 1

def get_week_end_ts(week_number=0):
    """
        This will always be precise. Never returns chain.time()
    """
    start = get_week_start_ts(week_number + 1)
    return start - 1

def block_to_date(b):
    time = chain[b].timestamp
    return datetime.fromtimestamp(time)

def closest_block_after_timestamp(timestamp: int) -> int:
    height = chain.height
    lo, hi = 0, height

    while hi - lo > 1:
        mid = lo + (hi - lo) // 2
        if get_block_timestamp(mid) > timestamp:
            hi = mid
        else:
            lo = mid

    if get_block_timestamp(hi) < timestamp:
        raise Exception("timestamp is in the future")

    return hi

def closest_block_before_timestamp(timestamp: int) -> int:
    return closest_block_after_timestamp(timestamp) - 1

def get_block_timestamp(height):
    return chain[height].timestamp

def timestamp_to_date_string(ts):
    return datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S")

def timestamp_to_string(ts):
    dt = datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S")
    return dt

def get_prices(tokens=[]):
    # Query DefiLlama for all of our coin prices
    coins = ','.join(f'ethereum:{k}' for k in tokens)
    url = f'https://coins.llama.fi/prices/current/{coins}?searchWidth=40h'
    response = requests.get(url).json()['coins']
    response = {key.replace('ethereum:', ''): value for key, value in response.items()}
    prices = {}
    for t in tokens:
        prices[t] = response[t]['price']
    return prices

def get_token_logo_urls(token_address):
    url = 'https://raw.githubusercontent.com/SmolDapp/tokenLists/main/lists/coingecko.json'
    data = requests.get(url).json()
    logo_url = ''
    for d in data['tokens']:
        if token_address == d['address']:
            logo_url = d['logoURI']
            return logo_url

def get_ens_from_cache(address):
    ens_data = load_from_json('ens_cache.json')
    if address in ens_data:
        return ens_data[address]
    return ''

def cache_ens():
    ens_data = load_from_json('ens_cache.json')
    if ens_data is None:
        ens_data = {}

    records = load_from_json('raw_boost_data.json')['data']

    count = 0
    for record in records:
        a, r, d = record['account'], record['receiver'], record['boost_delegate']
        count += 1
        for address in [a, r, d]:
            if address == ZERO_ADDRESS:
                continue
            # if address not in ens_data or ens_data[address] is '':
            if address not in ens_data:
                ens = web3.ens.name(address)
                ens = '' if ens is None or ens == 'null' else ens
                ens_data[address] = ens
                print(address, ens)
    cache_to_json('ens_cache.json', ens_data)

# Loading the dictionary from a JSON file
def load_from_json(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return None
    
# Saving the dictionary to a JSON file
def cache_to_json(file_path, data_dict):
    with open(file_path, 'w') as file:
        json.dump(data_dict, file, indent=4)