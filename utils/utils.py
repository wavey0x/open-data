import constants 
from brownie import ZERO_ADDRESS, Contract, web3, accounts, chain
import constants, requests
from datetime import datetime

WEEK = 60 * 60 * 24 * 7

def get_week_start_block(week_number=0):
    ts = get_week_start_ts(week_number)
    return closest_block_after_timestamp(ts)

def get_week_start_ts(week_number=0):
    token_locker = Contract(constants.TOKEN_LOCKER)
    current_week = token_locker.getWeek()
    if week_number > current_week:
        raise Exception('Week is in the future')
    offset = current_week - week_number
    current_week_start_ts = int(chain.time() / WEEK) * WEEK
    return current_week_start_ts - (WEEK * offset)

def get_week_end_block(week_number=0):
    token_locker = Contract(constants.TOKEN_LOCKER)
    current_week = token_locker.getWeek()
    if week_number == current_week:
        return chain.height
    ts = get_week_start_ts(week_number) + WEEK
    return closest_block_after_timestamp(ts) - 1

def get_week_end_ts(week_number=0):
    token_locker = Contract(constants.TOKEN_LOCKER)
    current_week = token_locker.getWeek()
    if week_number == current_week:
        return chain.time()
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
        raise IndexError('timestamp is in the future')

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