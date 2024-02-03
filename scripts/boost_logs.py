from brownie import ZERO_ADDRESS, Contract, accounts, web3, interface, chain
from web3._utils.events import construct_event_topic_set
import requests, os, json, datetime, time, utils

# We limit ourselves to this block range to avoid timeouts
MAX_RANGE = 50_000
DEPLOY_BLOCK = 18_029_884
contract_address = '0x06bDF212C290473dCACea9793890C5024c7Eb02c'
file_path = 'boost_consumed.json'
    
def get_logs():
    vault = interface.PrismaVault(contract_address)
    contract = web3.eth.contract(vault.address, abi=vault.abi)
    topics = construct_event_topic_set(
        contract.events.BoostConsumed().abi, 
        web3.codec,
        {}
    )

    last_item = None
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
            last_item = data[-1] if data else None

    to_block = DEPLOY_BLOCK if last_item is None else last_item['block']
    height = chain.height

    overall_start_time = time.time()
    while to_block < height:
        from_block = to_block + 1
        to_block = min(to_block + MAX_RANGE, height)
        print(f'Searching from block {from_block:_} -> {to_block:_} of total {height:_}')
        logs = web3.eth.get_logs(
            { 'topics': topics, 'fromBlock': from_block, 'toBlock': to_block }
        )
        events = contract.events.BoostConsumed().processReceipt({'logs': logs})
        print(f'Processing {len(events)} found events')
        formatted_events = []
        for i, event in enumerate(events):
            e = {}
            e['account'] = event.args['account']
            e['receiver'] = event.args['receiver']
            e['boost_delegate'] = event.args['boostDelegate']
            e['amount'] = event.args['amount']/1e18
            e['adjusted_amount'] = event.args['adjustedAmount']/1e18
            e['fee'] = event.args['fee']/1e18
            e['txn_hash'] = event.transactionHash.hex()
            e['block'] = event.blockNumber
            formatted_events.append(e)

        # Check if file exists
        if not os.path.exists(file_path):
            # If file does not exist, create it and write the first array of data
            with open(file_path, 'w') as file:
                json.dump(formatted_events, file)
        else:
            # If file exists, append the new data array to the existing array
            with open(file_path, 'r+') as file:
                existing_data = json.load(file)  # Load existing data array
                updated_data = existing_data + formatted_events  # Concatenate new data array
                file.seek(0)  # Rewind to the start of the file
                json.dump(updated_data, file)  # Write back the updated data array
                file.truncate()  # Truncate file to new size if necessary
        print(f'Successfully written to file.')
        print(f'Loop finished in {execution_time} seconds.\n')

    overall_end_time = time.time()
    execution_time = overall_end_time - overall_start_time
    print(f'Script took {execution_time} seconds to complete.')

def process_logs():
    """
    We do some post processing using a non overlay node.
    """
    data = None
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
    
    for d in data:
        if 'timestamp' not in d:
            block = d['block']
            ts = chain[block].timestamp
            dt = datetime.datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S")
            d['timestamp'] = ts
            d['date_str'] = dt
            d['system_week'] = utils.utils.get_week_by_ts(ts)

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)


def run_queries():
    import duckdb
    input_file = 'boost_consumed.json'
    dir = 'query_results/'
    duckdb.read_json(input_file)
    
    # Top accounts by fees paid
    output_file = f'{dir}top_accounts_by_fees_paid.json'
    sql = f"""
        COPY (
            SELECT account, SUM(fee) AS total_fees_paid
            FROM {input_file}
            GROUP BY account
            ORDER BY total_fees_paid DESC
        ) TO '{output_file}';
    """
    results = duckdb.sql(f'{sql}')

    # Top boost delegates by fees earned
    output_file = f'{dir}top_boost_delegates_by_fees_earned.json'
    sql = f"""
        COPY (
            SELECT boost_delegate, SUM(fee) AS earned_fees
            FROM {input_file}
            GROUP BY boost_delegate
            ORDER BY earned_fees DESC
        ) TO '{output_file}';
    """
    results = duckdb.sql(f'{sql}')

    # Total Emissions claimed
    print('Top accounts by total emissions claimed')
    output_file = f'{dir}top_accounts_by_total_emissions_claimed.json'
    sql = f"""
        COPY (
            SELECT account, SUM(adjusted_amount) AS amount
            FROM {input_file}
            GROUP BY account
            ORDER BY amount DESC
        ) TO '{output_file}';
    """
    results = duckdb.sql(f'{sql}')

    # Top Receivers By Emissions Claimed
    print('Top Receivers By Emissions Claimed')
    output_file = f'{dir}top_receivers_by_emissions_claimed.json'
    sql = f"""
        COPY (
            SELECT receiver, SUM(adjusted_amount) AS amount
            FROM {input_file}
            GROUP BY receiver
            ORDER BY amount DESC
        ) TO '{output_file}';
    """
    results = duckdb.sql(f'{sql}')

    assert False