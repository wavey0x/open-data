from brownie import interface, chain, web3
from web3._utils.events import construct_event_topic_set
import os, json, datetime, time, utils, subprocess
import duckdb
import pandas as pd

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
            data = data['data'] if isinstance(data, dict) else data
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
            existing_data = json.load(file) if mode == 'r+' else {}
            existing_data = existing_data['data'] if isinstance(existing_data, dict) else existing_data
            file.seek(0)
            data = {
                'last_updated': chain.time(),
                'data': existing_data + formatted_events
            }
            json.dump(data, file, indent=2)
            file.truncate()

        print(f'Found {len(events)} events. Loop took {time.time() - overall_start_time} seconds.')

    print(f'Total execution time: {time.time() - overall_start_time} seconds.')

def process_logs():
    if not os.path.exists(FILE_PATH):
        return

    with open(FILE_PATH) as file:
        data = json.load(file)

    data = data['data'] if isinstance(data, dict) else data

    for d in data:
        if 'timestamp' not in d:
            d['account_ens'] = utils.utils.get_ens_from_cache(d['account'])
            d['receiver_ens'] = utils.utils.get_ens_from_cache(d['receiver'])
            d['boost_delegate_ens'] = utils.utils.get_ens_from_cache(d['boost_delegate'])
            block_info = chain[d['block']]
            ts = block_info.timestamp
            d.update({
                'timestamp': ts,
                'date_str': datetime.datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S"),
                'system_week': utils.utils.get_week_by_ts(ts)
            })

    with open(FILE_PATH, 'w') as file:
        data = {
            'last_updated': chain.time(),
            'data': data
        }
        json.dump(data, file, indent=4)

def run_queries():
    process_logs()
    dir_path = 'query_results/'
    TABLE = 'boost_data'

    with open(FILE_PATH) as file:
        data = json.load(file)

    df = pd.DataFrame(data['data'])
    last_updated = data['last_updated']
    conn = duckdb.connect(database=':memory:', read_only=False)
    conn.register(TABLE, df)

    queries = [
        (
            "top_accounts_by_fees_paid.json", 
            f"SELECT account, account_ens as ens, SUM(fee) AS total_fees_paid FROM {TABLE} GROUP BY account, account_ens ORDER BY total_fees_paid DESC, account"
        ),
        (
            "top_boost_delegates_by_fees_earned.json",
            f"SELECT boost_delegate, boost_delegate_ens as ens, SUM(fee) AS earned_fees FROM {TABLE} GROUP BY boost_delegate, boost_delegate_ens ORDER BY earned_fees DESC, boost_delegate"
        ),
        (
            "top_accounts_by_total_emissions_claimed.json", 
            f"SELECT account, account_ens as ens, SUM(adjusted_amount) AS amount FROM {TABLE} GROUP BY account, account_ens ORDER BY amount DESC, account"
        ),
        (
            "top_receivers_by_emissions_claimed.json", 
            f"SELECT receiver, receiver_ens as ens, SUM(adjusted_amount) AS amount FROM {TABLE} GROUP BY receiver, receiver_ens ORDER BY amount DESC, receiver"
        )
    ]

    for file_name, sql in queries:
        output_file = f'{dir_path}{file_name}'
        result = conn.execute(f"{sql}").fetchdf()
        data = result.to_dict(orient='records')
        output = {
            'data': data,
            'last_updated': last_updated
        }
        # Write the list of JSON objects as a valid JSON array to the output file
        with open(output_file, 'w') as file:
            json.dump(output, file, indent=4)

        print(f"Formatted query results to json an saved results to '{output_file}'.")

    project_directory = os.getenv('TARGET_PROJECT_DIRECTORY')
    if os.getenv('ENV') != 'dev':
        push_to_gh(project_directory)

def push_to_gh(project_directory):
    home_dir = os.getenv('HOME')
    key = os.getenv('KEY')
    os.environ['GIT_SSH_COMMAND'] = f'ssh -i {home_dir}/.ssh/{key}' 

    os.chdir(project_directory)

    # Git commands to commit and push the changes
    try:
        # Add the file to staging
        subprocess.run(['git', 'add', 'query_results/', 'raw_boost_data.json'], check=True)

        # Commit the changes
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f'{formatted_datetime} boost logs'

        subprocess.run(['git', 'commit', '-m', commit_message], check=True)

        # Push the changes
        subprocess.run(['git', 'push', '--force-with-lease', '--force'], check=True)

        print("Changes committed and pushed to GitHub successfully.")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")