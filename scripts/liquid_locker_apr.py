from ape import Contract
import matplotlib.pyplot as plt
import matplotlib.dates as mdates  # Importing the date formatter
import pandas as pd
import requests
from datetime import datetime, timezone

DAY = 60 * 60 * 24
WEEK = DAY * 7
MONTH = DAY * 30
YEAR = DAY * 365

def main():
    # Get the unix timestamp of current day
    date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_start = int(date.replace(tzinfo=timezone.utc).timestamp())

    # Sample rate: Distance between dots
    # APR: Measure of APR in terms of underlying at the holding period from `point - sample_rate` and `point`

    # Sample holding period: Dot minus X
    # find APR between any two given blocks
    cvxcrv_apr(block)

def asdcrv_apr(from_block, to_block):
    starting_asset_value = asdcrv.convertToAssets(
        1e18,
        block_identifier=sample_block
    )
    ending_asset_value = asdcrv.convertToAssets(
        1e18,
        block_identifier=to_block
    )
    gain = (ending_asset_value - starting_asset_value) / 1e18
    # print(elapsed_time/60/60/24, gain)
    apr_sdcrv = gain / (starting_asset_value / 1e18) / (elapsed_time / (60 * 60 * 24 * 365))
    print(f'WEEK: {i}, APR: {apr_sdcrv*100:,.2f}%')
    sample['apr_sdcrv'] = apr_sdcrv

def stycrv_apr(from_block, to_block):
    st_ycrv = Contract('0x27B5739e22ad9033bcBf192059122d163b60349D')
    old_pps = st_ycrv.pricePerShare(block_identifier=from_block)
    new_pps = st_ycrv.pricePerShare(block_identifier=to_block)
    gain = new_pps - old_pps
    apr_ycrv = gain / old_pps / (elapsed_time / (60 * 60 * 24 * 365))

def cvxcrv_apr(block):
    wrapper = Contract('0xaa0C3f5F7DFD688C6E646F66CD2a6B66ACdbE434') # this one has crv, cvx, 3crv
    helper = Contract('0xadd2F542f9FF06405Fabf8CaE4A74bD0FE29c673')
    cvx_crv = '0x62B9c7356A2Dc64a1969e19C23e4f579F9810Aa7'

    helper.singleRewardRate(wrapper.cvxCrvStaking())
    helper.extraRewardRates()
    info = helper.mainRewardRates()

    rewards = {key: {'rate': val1, 'groups': val2} for key, val1, val2 in zip(info['tokens'], info['rates'], info['groups'])}

    for token in rewards:
        symbol = Contract(token).symbol()
        rewards[token]['symbol'] = symbol
        rewards[token]['is_stable'] = symbol in ['3Crv']
        if symbol == 'CVX':
            rewards[token]['rate'] += helper.extraRewardRates()['rates'][0]

    # Query DefiLlama for all of our coin prices
    coins = ','.join(f'ethereum:{k}' for k in list(rewards.keys()) + [cvx_crv])
    url = f'https://coins.llama.fi/prices/current/{coins}?searchWidth=4h'
    data = requests.get(url).json()['coins']
    data = {key.replace('ethereum:', ''): value for key, value in data.items()}

    # Update our dict with prices and APRs
    cvx_crv_price = data[cvx_crv]['price']
    for key, reward in rewards.items():
        price = data[key]['price']
        reward['price'] = price
        rewards[key]['apr'] = helper.apr(
            reward['rate'], 
            int(price * 10 ** 18),
            int(cvx_crv_price * 10 ** 18)
        ) / 10 ** 18
        print(rewards[key]['symbol'],rewards[key]['apr'])

    # Calculate group APRs
    # avg_system_apr = sum([rewards[x]['apr'] for x in rewards]) / 2 # WRONG way to calc this
    stable_apr = sum([rewards[x]['apr'] if rewards[x]['is_stable'] else 0 for x in rewards])
    gov_apr = sum([rewards[x]['apr'] if not rewards[x]['is_stable'] else 0 for x in rewards])

    assert False
