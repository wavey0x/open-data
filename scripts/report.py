
from brownie import web3, Contract, chain
import constants, utils, requests, json, os, subprocess

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
    # print(data)
    
    push_to_gh(data)


def stats():
    START_WEEK = 12
    token_locker = Contract(constants.TOKEN_LOCKER)
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
    current_week = token_locker.getWeek()
    

    liquid_lockers = {
        'cvxPrisma': constants.CONVEX_LOCKER,
        'yPRISMA': constants.YEARN_LOCKER
    }

    
    for l in liquid_lockers:
        d = data['liquid_lockers'][l]
        weekly_data = []
        for target_week in range(START_WEEK, current_week + 1):
            week_data = {}
            print(f'Week: {target_week}')
            token_locker.getTotalWeightAt(target_week)
            start_block = utils.utils.get_week_start_block(target_week)
            end_block = utils.utils.get_week_end_block(target_week)
            start_amt = token_locker.getAccountWeightAt(liquid_lockers[l], target_week - 1)/52
            end_amt = token_locker.getAccountWeightAt(liquid_lockers[l], target_week)/52
            w = token_locker.getAccountWeightAt(liquid_lockers[l], target_week)
            total_weight = token_locker.getTotalWeightAt(target_week)
            week_data['week_number'] = target_week
            week_data['peg'] = get_peg(d['pool'], block=end_block)
            week_data['lock_gain'] = end_amt - start_amt
            week_data['current_boost_multiplier'] = get_boost(liquid_lockers[l], target_week)
            week_data['global_weight_ratio'] = w / total_weight
            week_data['global_weight'] = total_weight
            week_data['weight']= w
            
            weekly_data.append(week_data)
        data['liquid_lockers'][l]['weekly_data'] = weekly_data

    data['liquid_lockers']['cvxPrisma']['current_staking_apr'] = cvxprisma_staking_apr()
    data['liquid_lockers']['cvxPrisma']['current_lp_apr'] = cvxprisma_lp_apr()
    data['liquid_lockers']['yPRISMA']['current_staking_apr'] = yprisma_staking_apr()
    data['liquid_lockers']['yPRISMA']['current_lp_apr'] = yprisma_lp_apr()

    return data

def get_boost(user, week):
    vault = Contract(constants.VAULT)
    key = web3.keccak(
        hexstr="00" * 12
        + user[2:]
        + "0000000000000000000000000000000000000000000000000000000000009005"
    )
    data = int(web3.eth.getStorageAt(vault.address, int(key.hex(), 16) + week // 2).hex(), 16)
    calculator = Contract(constants.BOOST_CALCULATOR)

    if week % 2:
        account_weekly_earned = data >> 128
    else:
        account_weekly_earned = data % 2**128

    boost = calculator.getBoostedAmount(user,2e18,account_weekly_earned,vault.weeklyEmissions(week)) / 1e18

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


    # Query DefiLlama for all of our coin prices
    coins = ','.join(f'ethereum:{k}' for k in rewards)
    url = f'https://coins.llama.fi/prices/current/{coins}?searchWidth=40h'
    response = requests.get(url).json()['coins']
    response = {key.replace('ethereum:', ''): value for key, value in response.items()}

    prices = {}
    for reward in rewards:
        prices[reward] = response[reward]['price']
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


    # Query DefiLlama for all of our coin prices
    coins = ','.join(f'ethereum:{k}' for k in rewards)
    url = f'https://coins.llama.fi/prices/current/{coins}?searchWidth=40h'
    response = requests.get(url).json()['coins']
    response = {key.replace('ethereum:', ''): value for key, value in response.items()}

    prices = {}
    for reward in rewards:
        prices[reward] = response[reward]['price']
    prices[lp.address] = lp_value_to_prisma * prices[prisma]

    reward_apr = 0
    for i, reward in enumerate(rewards):
        reward_apr += (
            receiver.rewardRate(i) / 1e18 * prices[reward] * YEAR /
            (prices[lp.address] * receiver.totalSupply() / 1e18)
        )

    return reward_apr

def push_to_gh(data):
    from dotenv import load_dotenv
    load_dotenv()

    project_directory = os.getenv('TARGET_PROJECT_DIRECTORY')
    json_file_directory = project_directory+'/data'
    json_filename = os.getenv('JSON_FILE')

    os.chdir(project_directory)
    try:
        # Add the file to staging
        subprocess.run(['git', 'fetch', '--all'], check=True)
        subprocess.run(['git', 'reset', '--hard', 'origin/master'], check=True)
        print("Local project synced")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")

    # Write the JSON object to the file
    json_file_path = os.path.join(json_file_directory, json_filename)
    with open(json_file_path, 'w') as file:
        json.dump(data, file, indent=4)

    # Change to the project directory
    os.chdir(project_directory)

    # Git commands to commit and push the changes
    try:
        # Add the file to staging
        subprocess.run(['git', 'add', f'{json_file_directory}/{json_filename}'], check=True)

        # Commit the changes
        commit_message = 'Update prisma_liquid_locker_data.json'
        subprocess.run(['git', 'commit', '-m', commit_message], check=True)

        # Push the changes
        subprocess.run(['git', 'push', '--force-with-lease', '--force'], check=True)

        print("Changes committed and pushed to GitHub successfully.")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")