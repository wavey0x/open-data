from brownie import interface, chain, web3
from web3._utils.events import construct_event_topic_set
import os, json, datetime, time, utils
import duckdb

MAX_RANGE = 50_000
DEPLOY_BLOCK = 18_029_884
CONTRACT_ADDRESS = '0x06bDF212C290473dCACea9793890C5024c7Eb02c'
FILE_PATH = 'raw_boost_data.json'
    
def get_logs():
    vault = interface.PrismaVault(CONTRACT_ADDRESS)
    contract = web3.eth.contract(address=vault.address, abi=vault.abi)
    topics = construct_event_topic_set(contract.events.BoostConsumed().abi, web3.codec)

    last_item = None
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH) as file:
            data = json.load(file)
            last_item = data[-1] if data else None

    to_block = last_item['block'] if last_item else DEPLOY_BLOCK
    overall_start_time = time.time()

    while to_block < chain.height:
        from_block, to_block = to_block + 1, min(to_block + MAX_RANGE, chain.height)
        print(f'Searching from block {from_block:_} to {to_block:_}')
        logs = web3.eth.get_logs({'topics': topics, 'fromBlock': from_block, 'toBlock': to_block})
        events = contract.events.BoostConsumed().processReceipt({'logs': logs})
        formatted_events = [{
            'account': e.args['account'],
            'receiver': e.args['receiver'],
            'boost_delegate': e.args['boostDelegate'],
            'amount': e.args['amount'] / 1e18,
            'adjusted_amount': e.args['adjustedAmount'] / 1e18,
            'fee': e.args['fee'] / 1e18,
            'txn_hash': e.transactionHash.hex(),
            'block': e.blockNumber
        } for e in events]

        mode = 'w' if not os.path.exists(FILE_PATH) else 'r+'
        with open(FILE_PATH, mode) as file:
            existing_data = json.load(file) if mode == 'r+' else []
            file.seek(0)
            json.dump(existing_data + formatted_events, file, indent=2)
            file.truncate()

        print(f'Processed {len(events)} events. Loop took {time.time() - overall_start_time} seconds.')

    print(f'Total execution time: {time.time() - overall_start_time} seconds.')

def process_logs():
    if not os.path.exists(FILE_PATH):
        return

    with open(FILE_PATH) as file:
        data = json.load(file)

    for d in data:
        if 'timestamp' not in d:
            block_info = chain[d['block']]
            ts = block_info.timestamp
            d.update({
                'timestamp': ts,
                'date_str': datetime.datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S"),
                'system_week': utils.utils.get_week_by_ts(ts)
            })

    with open(FILE_PATH, 'w') as file:
        json.dump(data, file, indent=4)

def run_queries():
    process_logs()
    dir_path = 'query_results/'
    FILE_PATH = 'raw_boost_data.json'
    os.makedirs(dir_path, exist_ok=True)
    duckdb_conn = duckdb.connect()

    queries = [
        ("top_accounts_by_fees_paid.json", f"SELECT account, SUM(fee) AS total_fees_paid FROM {FILE_PATH} GROUP BY account ORDER BY total_fees_paid DESC"),
        ("top_boost_delegates_by_fees_earned.json", f"SELECT boost_delegate, SUM(fee) AS earned_fees FROM {FILE_PATH} GROUP BY boost_delegate ORDER BY earned_fees DESC"),
        ("top_accounts_by_total_emissions_claimed.json", f"SELECT account, SUM(adjusted_amount) AS amount FROM {FILE_PATH} GROUP BY account ORDER BY amount DESC"),
        ("top_receivers_by_emissions_claimed.json", f"SELECT receiver, SUM(adjusted_amount) AS amount FROM {FILE_PATH} GROUP BY receiver ORDER BY amount DESC")
    ]

    for file_name, sql in queries:
        output_file = f'{dir_path}{file_name}'
        duckdb_conn.execute(f"COPY ({sql}) TO '{output_file}'")
        print(f'Query results saved to {output_file}')
