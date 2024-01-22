
from brownie import web3, Contract, chain
import constants, utils, requests, json, os, subprocess
from dotenv import load_dotenv
import time, datetime

load_dotenv()

start_time = time.time()
height = chain.height
vault = Contract(constants.VAULT)
current_week = vault.getWeek()
DAY = 24 * 60 * 60
YEAR = DAY * 365
data = {
    'yprisma': {
        'token': '0xe3668873d944e4a949da05fc8bde419eff543882',
        'pool' : '0x69833361991ed76f9e8dbbcdf9ea1520febfb4a7',
        'gauge': '0xf1ce237a1e1a88f6e289cd7998a826138aeb30b0',
    },
    'cvxprisma': {
        'token': '0x34635280737b5bfe6c7dc2fc3065d60d66e78185',
        'pool' : '0x3b21c2868b6028cfb38ff86127ef22e68d16d53b',
        'gauge': '0x13e58c7b1147385d735a06d14f0456e54c2debc8',
    }
}

def main():
    data = stats()

    json_filename = os.getenv('JSON_FILE')
    project_directory = os.getenv('TARGET_PROJECT_DIRECTORY')
    write_data_as_json(data, project_directory, json_filename)

    if os.getenv('ENV') != 'dev':
        # fetch_from_gh(project_directory)
        push_to_gh(data, project_directory, json_filename)

    end_time = time.time()  # Get the end time
    duration = end_time - start_time  # Calculate the duration

    print(f"Total run time: {duration:.2f} seconds")

def stats():
    START_WEEK = 12
    token_locker = Contract(constants.TOKEN_LOCKER)
    last_run_data = get_last_run_data() # Re-using old data helps us speed up expensive repetitive block queries
    data = {
        'prisma_week': token_locker.getWeek(),
        'updated_at': chain.time(),
        'liquid_lockers': {
            'cvxPrisma': {
                'locker': constants.CONVEX_LOCKER,
                'token': '0x34635280737b5bfe6c7dc2fc3065d60d66e78185',
                'pool' : '0x3b21c2868b6028cfb38ff86127ef22e68d16d53b',
                'gauge': '0x13e58c7b1147385d735a06d14f0456e54c2debc8',
                'weekly_data': {},
            },
            'yPRISMA': {
                'locker': constants.YEARN_LOCKER,
                'token': '0xe3668873d944e4a949da05fc8bde419eff543882',
                'pool' : '0x69833361991ed76f9e8dbbcdf9ea1520febfb4a7',
                'gauge': '0xf1ce237a1e1a88f6e289cd7998a826138aeb30b0',
                'weekly_data': {},
            },
        }
    }

    token_locker = Contract(constants.TOKEN_LOCKER)
    

    liquid_lockers = {
        'cvxPrisma': constants.CONVEX_LOCKER,
        'yPRISMA': constants.YEARN_LOCKER
    }

    for l in liquid_lockers:
        account = liquid_lockers[l]
        d = data['liquid_lockers'][l]
        weekly_data = []
        boost_fees_cache = {}
        try:
            weekly_data_cache = last_run_data['liquid_lockers'][l]['weekly_data']
            boost_fees_cache = {item['week_number']: item['boost_fees_collected'] for item in weekly_data_cache}
        except:
            print(f'Cannot parse past data.')
        for target_week in range(START_WEEK, current_week + 1):
            week_data = {}
            print(f'Week: {target_week}')
            token_locker.getTotalWeightAt(target_week)
            start_block = utils.utils.get_week_start_block(target_week)
            end_block = utils.utils.get_week_end_block(target_week)
            start_amt = token_locker.getAccountWeightAt(account, target_week - 1)/52
            end_amt = token_locker.getAccountWeightAt(account, target_week)/52
            w = token_locker.getAccountWeightAt(account, target_week)
            total_weight = token_locker.getTotalWeightAt(target_week)
            week_data['week_number'] = target_week
            week_data['peg'] = get_peg(d['pool'], block=end_block)
            week_data['lock_gain'] = end_amt - start_amt
            week_data['current_boost_multiplier'] = get_boost(account, target_week, block=end_block)
            week_data['global_weight_ratio'] = w / total_weight
            week_data['global_weight'] = total_weight
            week_data['weight']= w
            week_data['remaining_boost_data'] = get_remaining_weekly_boost(account, target_week)
            if (
                target_week == current_week or len(boost_fees_cache) == 0 or not target_week in boost_fees_cache
            ):
                week_data['boost_fees_collected'] = get_boost_delegation_fees(account, start_block=start_block, end_block=end_block)
            else:
                week_data['boost_fees_collected'] = boost_fees_cache[target_week]
            weekly_data.append(week_data)
        data['liquid_lockers'][l]['weekly_data'] = weekly_data
        
        

    data['liquid_lockers']['cvxPrisma']['current_staking_apr'] = cvxprisma_staking_apr()
    data['liquid_lockers']['cvxPrisma']['current_lp_apr'] = cvxprisma_lp_apr()
    data['liquid_lockers']['yPRISMA']['current_staking_apr'] = yprisma_staking_apr()
    data['liquid_lockers']['yPRISMA']['current_lp_apr'] = yprisma_lp_apr()

    return data

def get_remaining_weekly_boost(account, week=current_week):
    block=height
    if week != current_week:
        block = utils.utils.get_week_end_block(week)
    data = vault.getClaimableWithBoost(account, block_identifier=block).dict()
    remaining_boost_data = {
        'max_boost_allocation': data['boosted']/1e18,
        'max_boost_remaining': data['maxBoosted']/1e18,
        'pct_consumed': (data['boosted'] - data['maxBoosted'])/data['boosted']*100
    }
    return remaining_boost_data

def get_boost(user, week, block=height):
    vault = Contract(constants.VAULT)
    calculator = Contract(vault.boostCalculator(block_identifier=block))

    key = web3.keccak(
        hexstr="00" * 12
        + user[2:]
        + "0000000000000000000000000000000000000000000000000000000000009005"
    )
    data = int(web3.eth.getStorageAt(vault.address, int(key.hex(), 16) + week // 2, block_identifier=block).hex(), 16)
    
    if week % 2:
        account_weekly_earned = data >> 128
    else:
        account_weekly_earned = data % 2**128

    boost = calculator.getBoostedAmount(
        user,
        2e18,
        account_weekly_earned,
        vault.weeklyEmissions(week, block_identifier=block),
        block_identifier=block
    ) / 1e18

    return boost

def get_peg(pool, amt=100, block=chain.height):
    pool = Contract(pool)
    try:
        price = pool.price_oracle(block_identifier=block) / 1e18
        out = pool.get_dy(1, 0, amt*1e18, block_identifier=block) / 1e18
    except:
        return 0
    return price

def cvxprisma_staking_apr(block=chain.height):
    stake_contract = Contract('0x0c73f1cFd5C9dFc150C8707Aa47Acbd14F0BE108')
    apr = 0
    
    reward_tokens = []
    supply = stake_contract.totalSupply(block_identifier=block)
    for i in range(0,200):
        try:
            token = stake_contract.rewardTokens(i)
            reward_tokens.append(token)
        except:
            break

    # Query DefiLlama for all of our coin prices
    cvxprisma = stake_contract.cvxprisma()
    coins = ','.join(f'ethereum:{k}' for k in reward_tokens + [cvxprisma])
    url = f'https://coins.llama.fi/prices/current/{coins}?searchWidth=4h'
    response = requests.get(url).json()['coins']
    response = {key.replace('ethereum:', ''): value for key, value in response.items()}
    price_stake = response[cvxprisma]['price']

    for token in reward_tokens:
        data = stake_contract.rewardData(token).dict()
        if chain.time() > data['periodFinish']:
            continue
        price_reward = response[token]['price']
        reward_apr = data['rewardRate'] / 1e18 * price_reward * YEAR / (price_stake * supply / 1e18)
        apr += reward_apr

    return apr

def yprisma_staking_apr(block=chain.height):
    staker = Contract('0xE3EE395C9067dD15C492Ca950B101a7d6c85b5Fc')
    mkusd = '0x4591DBfF62656E7859Afe5e45f6f47D3669fBB28'

    staking_token = staker.stakingToken()
    supply = staker.totalSupply()


    # Query DefiLlama for all of our coin prices
    coins = ','.join(f'ethereum:{k}' for k in [mkusd, staking_token])
    url = f'https://coins.llama.fi/prices/current/{coins}?searchWidth=40h'
    response = requests.get(url).json()['coins']
    response = {key.replace('ethereum:', ''): value for key, value in response.items()}
    price_staking_token = response[staking_token]['price']
    price_reward_token = response[mkusd]['price']
    reward_apr = staker.rewardRate() / 1e18 * price_reward_token * YEAR / (price_staking_token * supply / 1e18)
    return reward_apr

def yprisma_lp_apr(block=chain.height):
    receiver = Contract('0xb8Fa880840a64c25318989B907cCb58FD7A324Df')
    lp = Contract(receiver.lpToken())
    ll_price = lp.price_oracle()
    lp_value_to_prisma = lp.calc_withdraw_one_coin(1e18,0) / 1e18
    prisma = '0xdA47862a83dac0c112BA89c6abC2159b95afd71C'
    crv = '0xD533a949740bb3306d119CC777fa900bA034cd52'
    cvx = '0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B'

    rewards = [prisma, crv, cvx]

    prices = utils.utils.get_prices(rewards)

    prices[lp.address] = lp_value_to_prisma * prices[prisma]

    reward_apr = 0
    for i, reward in enumerate(rewards):
        reward_apr += (
            receiver.rewardRate(i) / 1e18 * prices[reward] * YEAR /
            (prices[lp.address] * receiver.totalSupply() / 1e18)
        )

    return reward_apr

def cvxprisma_lp_apr(block=chain.height):
    receiver = Contract('0xd91fBa4919b7BF3B757320ea48bA102F543dE341')
    lp = Contract(receiver.lpToken())
    ll_price = lp.price_oracle()
    lp_value_to_prisma = lp.calc_withdraw_one_coin(1e18,0) / 1e18
    prisma = '0xdA47862a83dac0c112BA89c6abC2159b95afd71C'
    crv = '0xD533a949740bb3306d119CC777fa900bA034cd52'
    cvx = '0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B'

    rewards = [prisma, crv, cvx]

    prices = utils.utils.get_prices(rewards)
    
    prices[lp.address] = lp_value_to_prisma * prices[prisma]

    reward_apr = 0
    for i, reward in enumerate(rewards):
        reward_apr += (
            receiver.rewardRate(i) / 1e18 * prices[reward] * YEAR /
            (prices[lp.address] * receiver.totalSupply() / 1e18)
        )

    return reward_apr

def get_boost_delegation_fees(account, start_block=0, end_block=0):
    print('SEARCHING BOOST FEES!')
    start_block = 18501009 if start_block == 0 else start_block
    target_block = chain.height if end_block == 0 else end_block
    block = start_block
    resolution = 500
    total = 0
    last = vault.claimableBoostDelegationFees(
        account, 
        block_identifier = start_block - 1
    ) / 1e18
    while block < target_block:
        claimable = vault.claimableBoostDelegationFees(
            account, 
            block_identifier=block
        ) / 1e18
        if claimable > last:
            total += (claimable - last)
        last = claimable
        block += resolution
    return total

def get_last_run_data():
    fn = 'prisma_liquid_locker_data.json'
    if os.path.exists(fn):
        # Read the JSON file and convert it to a dictionary
        with open(fn, 'r') as file:
            json_data = json.load(file)
        result = json_data
    else:
        print(f"Previous run file {fn} not found")
        return {}
    return result

def write_data_as_json(data, project_directory="", json_filename=os.getenv('JSON_FILE')):
    json_file_path = os.path.join(project_directory,json_filename)
    with open(json_file_path, 'w') as file:
        json.dump(data, file, indent=4)

def fetch_from_gh(project_directory):
    home_dir = os.getenv('HOME')
    key = os.getenv('KEY')
    os.environ['GIT_SSH_COMMAND'] = f'ssh -i {home_dir}/.ssh/{key}' 
    os.chdir(project_directory)
    try:
        # Add the file to staging
        subprocess.run(['git', 'fetch', '--all'], check=True)
        subprocess.run(['git', 'reset', '--hard', 'origin/master'], check=True)
        print("Local project synced")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")

def push_to_gh(data, project_directory, json_file_path):
    home_dir = os.getenv('HOME')
    key = os.getenv('KEY')
    os.environ['GIT_SSH_COMMAND'] = f'ssh -i {home_dir}/.ssh/{key}' 

    os.chdir(project_directory)

    # Git commands to commit and push the changes
    try:
        # Add the file to staging
        subprocess.run(['git', 'add', json_file_path], check=True)

        # Commit the changes
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f'{formatted_datetime} prisma_liquid_locker_data.json'

        subprocess.run(['git', 'commit', '-m', commit_message], check=True)

        # Push the changes
        subprocess.run(['git', 'push', '--force-with-lease', '--force'], check=True)

        print("Changes committed and pushed to GitHub successfully.")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")