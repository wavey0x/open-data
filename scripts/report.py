
from brownie import web3, Contract, chain, ZERO_ADDRESS
import constants, utils, requests, json, os, subprocess
from dotenv import load_dotenv
import time, datetime
import pandas as pd
from constants import YEAR, WEEK, DAY, EMISSIONS_START_WEEK

load_dotenv()

start_time = time.time()
height = chain.height

vault = Contract(constants.VAULT)
emissions_schedule = Contract(constants.EMISSIONS_SCHEDULE)
token_locker = Contract(constants.TOKEN_LOCKER)
prisma_fee_distributor = Contract(constants.PRISMA_FEE_DISTRIBUTOR)
current_week = vault.getWeek()

TOKEN_INFO = {
    '0x4591DBfF62656E7859Afe5e45f6f47D3669fBB28': {
        'symbol':'mkUSD',
        'decimals':'18',
        'price': utils.utils.get_prices(['0x4591DBfF62656E7859Afe5e45f6f47D3669fBB28'])['0x4591DBfF62656E7859Afe5e45f6f47D3669fBB28'],
        'token_logo_url': 'https://assets.smold.app/api/token/1/0x4591DBfF62656E7859Afe5e45f6f47D3669fBB28/logo-32.png'
    }
}

def main():
    data = stats()
    data['emissions_schedule'] = emissions_by_week()
    data['distribution_schedule'] = distribution_schedule()
    data['active_fowarders'] = get_active_forwarders()
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

    for l in data['liquid_lockers'].keys():
        d = data['liquid_lockers'][l]
        account = d['locker']
        weekly_data = []
        boost_fees_cache = {}
        try:
            weekly_data_cache = last_run_data['liquid_lockers'][l]['weekly_data']
            boost_fees_cache = {item['week_number']: item['boost_fees_collected'] for item in weekly_data_cache}
        except:
            print(f'Cannot parse past data.')
        for target_week in range(EMISSIONS_START_WEEK, current_week + 1):
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
            boost_data = get_maxboost_and_decay(account, target_week, block=start_block)
            # assert False
            week_data['remaining_boost_data'] = get_remaining_weekly_boost(account, target_week)
            if (
                target_week in [current_week, current_week - 1] or # We want to refresh last two weeks in case of overwrite.
                len(boost_fees_cache) == 0 
                or not target_week in boost_fees_cache
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
    start_block = utils.utils.get_week_start_block(week)
    
    week_start_data = get_maxboost_and_decay(account, week, block=start_block)
    week_end_data = get_maxboost_and_decay(account, week, block=block)

    remaining_boost_data = {
        'max_boost_allocation': week_start_data['maxBoosted']/1e18,
        'decay_boost_allocation': week_start_data['boosted']/1e18,
        'max_boost_remaining': week_end_data['maxBoosted']/1e18,
        'decay_boost_remaining': week_end_data['boosted']/1e18,
        'pct_max_consumed': (week_start_data['maxBoosted'] - week_end_data['maxBoosted'])/week_start_data['maxBoosted']*100,
        # The following will be bugged using the initial calculator. Only week 24+ will return proper amounts.
        'pct_decay_consumed': (week_start_data['boosted'] - week_end_data['boosted'])/week_start_data['boosted']*100
    }
    return remaining_boost_data

def get_active_forwarders():
    ens_data = load_from_json('ens_cache.json')
    week = vault.getWeek()
    factory = Contract(constants.BOOST_FACTORY)
    logs = factory.events.ForwarderConfigured.getLogs(fromBlock=0)
    fee = 0
    active_delegates = []
    for log in logs:
        d = log.args['boostDelegate']
        # fwd = Contract(factory.forwarder(d))
        if factory.isForwarderActive(d) and d not in active_delegates:
            active_delegates.append(d)

    active_delegate_list = []
    for d in active_delegates:
        boost_data = get_remaining_weekly_boost(d, week)
        fee_callback = factory.feeCallback(d)
        if fee_callback == ZERO_ADDRESS:
            fee = Contract(constants.VAULT).boostDelegation(d)['feePct']
        else:
            fee_callback = Contract(fee_callback)
            fee = fee_callback.getFeePct(
                ZERO_ADDRESS,
                ZERO_ADDRESS,
                d,
                1_000e18,
                0,
                0,
            )
        boost_data['fee'] = fee
        boost_data['boost_delegate'] = d
        boost_data['ens'] = ''
        if d in ens_data and not ens_data[d] is None:
            boost_data['ens'] = ens_data[d]
        boost_data['delegate_ens'] = ens_data[d]
        active_delegate_list.append(boost_data)
    
    return active_delegate_list

def get_boost(user, week, block=height):
    calculator = Contract(vault.boostCalculator(block_identifier=block))

    account_weekly_earned = get_account_weekly_earned(user, week, block=height)

    boost = calculator.getBoostedAmount(
        user,
        2e18,
        account_weekly_earned,
        vault.weeklyEmissions(week, block_identifier=block),
        block_identifier=block
    ) / 1e18

    return boost

def get_maxboost_and_decay(user, week, block=height):
    total_weekly = vault.weeklyEmissions(week)
    account_weekly_earned = get_account_weekly_earned(user, week, block)
    calculator = Contract(vault.boostCalculator(block_identifier=block))
    return calculator.getClaimableWithBoost(user, account_weekly_earned, total_weekly).dict()

def get_account_weekly_earned(user, week, block=height):
    # compute accountWeeklyEarned storage key
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

    return account_weekly_earned

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

def get_fees_by_week():
    logs = prisma_fee_distributor.events.FeesReceived.getLogs(fromBlock=0)
    fee_data = {}
    for l in logs:
        log_data = l.args
        week = log_data.week + 1
        if week not in fee_data:
            fee_data[week] = []
        amount = log_data.amount / 10 ** Contract(log_data.token).decimals()
        value = 0
        try:
            value = TOKEN_INFO[log_data.token]['price'] * amount
        except:
            value = 0
        fee_data[week].append(
            {
                'token':log_data.token,
                'amount': amount,
                'value': value,
                'token_price': TOKEN_INFO[log_data.token]['price'],
                'token_logo_url': TOKEN_INFO[log_data.token]['token_logo_url'],
                'symbol': TOKEN_INFO[log_data.token]['symbol']
            }
        )
    return fee_data

def emissions_by_week():
    MAX_PCT = 10_000
    fee_distro_by_week = get_fees_by_week()
    current_week = vault.getWeek()
    emissions_week = 0
    weeks = []
    net_emissions_notes = {
        0: 'Example note to appear as tooltip.',
        12: 'During first week, all claims were 2x boosted. Therefore, nothing was returned to vault as unallocated.',
        15: 'Biggest week of returned emissions. Largely due to this chad making a massive unboosted claim (1x boost). [Transaction](https://etherscan.io/tx/0xbc37f09cd66896e9f1e3f2b3f56ce5783cb1438ef0010da6396e617b738bdbc4)',
    }
    for i in range(0, current_week + 2):
        end_block = chain.height
        rate_change = False            
        weekly_data = {}
        lock_weeks = 0
        if vault.weeklyEmissions(i) > 0:
            end_block = utils.utils.get_week_end_block(i)
            lock_weeks = emissions_schedule.lockWeeks(block_identifier=end_block)
            emissions_week += 1
            weekly_data['projected'] = False
            weekly_data['allocated_emissions'] = vault.weeklyEmissions(i)/1e18
            weekly_data['emissions_week'] = emissions_week
            weekly_data['system_week'] = i
            pct = emissions_schedule.weeklyPct(block_identifier=end_block)
            next_update = emissions_schedule.getWeeklyPctSchedule(block_identifier=end_block)[-1]
            if next_update[0] == i:
                pct = next_update[1]
                rate_change = True
        else:
            if i < current_week:
                continue
            emissions_week += 1
            weekly_data['projected'] = True
            weekly_data['emissions_week'] = emissions_week
            weekly_data['system_week'] = i
            # Calc projected
            decay_weeks = emissions_schedule.lockDecayWeeks(block_identifier=end_block)
            lock_weeks = emissions_schedule.lockWeeks(block_identifier=end_block)
            if lock_weeks > 0 and i % decay_weeks == 0:
                lock_weeks -= 1
            
            unallocated_total = vault.unallocatedTotal(block_identifier=end_block)
            pct = emissions_schedule.weeklyPct(block_identifier=end_block)
            next_update = emissions_schedule.getWeeklyPctSchedule(block_identifier=end_block)[-1]
            if next_update[0] == i:
                pct = next_update[1]
                rate_change = True
            weekly_data['allocated_emissions'] = (unallocated_total * pct) / MAX_PCT / 1e18

        if i <= current_week:
            unallocated_total_start = vault.unallocatedTotal(block_identifier=utils.utils.get_week_start_block(i)) / 1e18
            unallocated_total_end = vault.unallocatedTotal(block_identifier=end_block) / 1e18
            # First four weeks of emissions were special due to init params. They did not impact the unallocated supply.
            if i in [12, 13, 14, 15]:
                weekly_data['net_emissions_returned'] = unallocated_total_end - unallocated_total_start
            else:
                weekly_data['net_emissions_returned'] = (
                    weekly_data['allocated_emissions'] - 
                    (unallocated_total_start - unallocated_total_end) # Decline in allocated
                )
        else:
            weekly_data['net_emissions_returned'] = 0

        total_protocol_fees = 0
        if i in fee_distro_by_week:
            for x in fee_distro_by_week[i]:
                total_protocol_fees += x['value']
        protocol_fee_distribution = {
            'total_value': total_protocol_fees,
            'distros': [] if i not in fee_distro_by_week else fee_distro_by_week[i]
        }
        weekly_data['protocol_fee_distribution'] = protocol_fee_distribution
        weekly_data['lock_weeks'] = lock_weeks
        weekly_data['emissions_rate_change_week'] = rate_change
        weekly_data['emissions_rate_pct'] = pct
        weekly_data['penalty_pct'] = 0 if not token_locker.penaltyWithdrawalsEnabled(block_identifier=end_block) else (
            lock_weeks / 52 * 100
        )
        weekly_data['net_emissions_notes'] = '' if not i in net_emissions_notes else net_emissions_notes[i]
        weekly_data['week_start_ts'] = utils.utils.get_week_start_ts(i)
        weekly_data['week_end_ts'] = utils.utils.get_week_end_ts(i)
        weeks.append(weekly_data)

    # Creating a DataFrame from the list of dictionaries
    df = pd.DataFrame(weeks)

    # Renaming columns to match the requested names
    column_mapping = {
        'system_week': 'system week',
        'emissions_week': 'emissions week',
        'allocated_emissions': 'allocated emissions',
        'net_emissions_returned': 'net emissions returned',
        'lock_weeks': 'lock weeks',
        'penalty_pct': 'instant withdraw penalty',
        'net_emissions_notes': 'net emissions notes',
    }

    # Renaming and selecting the columns
    df = df.rename(columns=column_mapping)[list(column_mapping.values())]

    df['allocated emissions'] = df['allocated emissions'].astype(int).apply(lambda x: f"{x:,}")  # No decimal precision, add commas
    df['net emissions returned'] = df['net emissions returned'].astype(int).apply(lambda x: f"{x:,}")  # No decimal precision, add commas
    df['instant withdraw penalty'] = df['instant withdraw penalty'].round(2).astype(str) + '%'
    # print(df)
    return weeks

def distribution_schedule():
    checkpoints = emissions_schedule.getWeeklyPctSchedule(block_identifier=utils.utils.get_week_end_block(12))
    schedule = []
    initial_emissions_end = utils.utils.get_week_start_ts(3 + EMISSIONS_START_WEEK) - 1
    schedule.append(
        {
            'week': EMISSIONS_START_WEEK,
            'rate': 0,
            'start_ts': utils.utils.get_week_start_ts(EMISSIONS_START_WEEK),
            'end_ts': initial_emissions_end,
        }
    )
    schedule.append(
        {
            'week': 4 + EMISSIONS_START_WEEK,
            'rate': 1.2,
            'start_ts': initial_emissions_end + 1,
        }
    )
    for item in reversed(checkpoints):
        week = item[0]
        start = utils.utils.get_week_start_ts(week)
        schedule[-1]['end_ts'] = start - 1
        schedule.append(
            {
                'week': week,
                'rate': item[1] / 100,
                'start_ts': utils.utils.get_week_start_ts(week),
            }
        )
    schedule[-1]['end_ts'] = 0
    return schedule

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

# Saving the dictionary to a JSON file
def cache_to_json(file_path, data_dict):
    with open(file_path, 'w') as file:
        json.dump(data_dict, file)

# Loading the dictionary from a JSON file
def load_from_json(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return None
    
def cache_ens():
    ens_data = load_from_json('ens_cache.json')
    if ens_data is None:
        ens_data = {}
    logs = vault.events.BoostDelegationSet.getLogs(fromBlock=0)
    for log in logs:
        d = log.args['boostDelegate']
        if d not in ens_data:
            ens = web3.ens.name(d)
            ens_data[d] = ens
            print(d, ens)

    cache_to_json('ens_cache.json', ens_data)
    
