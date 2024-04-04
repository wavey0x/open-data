from brownie import Contract, chain, web3
import pandas as pd
import duckdb
import requests
import utils
from constants import VAULT

vault = Contract(VAULT)

def query():
    # fetch raw txn data from wavey repo and put into dataframe
    url = 'https://raw.githubusercontent.com/wavey0x/open-data/master/raw_boost_data.json'
    data = requests.get(url).json()['data']
    df = pd.DataFrame(data)

    # load data into virtual db
    con = duckdb.connect(database=':memory:')
    con.register('boost_data', df)

    # Write any SQL to query the raw data

    sql = f"""
        SELECT account, boost_delegate, adjusted_amount as amt, fee
        FROM boost_data 
        WHERE
                (receiver_ens = 'prisma.cvx.eth' AND
                boost_delegate_ens != 'prisma.cvx.eth') OR
                (receiver_ens = 'yprisma.eth' AND
                boost_delegate_ens != 'yprisma.eth')
        ORDER BY block DESC 
    """

    sql = f"""
        SELECT txn_hash, boost_delegate_ens, adjusted_amount, fee, date_str
        FROM boost_data 
        WHERE
            system_week >= 33 AND
            adjusted_amount > 10000
        ORDER BY adjusted_amount desc
    """

    sql = f"""
        SELECT 
            boost_delegate, boost_delegate_ens as ens, 
            SUM(fee) AS earned_fees 
        FROM
            boost_data
        WHERE
            boost_delegate_ens = 'yprisma.eth' OR
            boost_delegate_ens = 'prisma.cvx.eth'
        GROUP BY 
            boost_delegate, boost_delegate_ens 
        ORDER BY 
            earned_fees DESC, boost_delegate
    """

    # sql = f"""
    #     SELECT account, boost_delegate, adjusted_amount, fee, date_str
    #     FROM boost_data 
    #     WHERE
    #         system_week = 26 AND
    #             (receiver_ens = 'prisma.cvx.eth' OR
    #             boost_delegate_ens = 'prisma.cvx.eth')
        
    #     ORDER BY block DESC 
    # """
    # sql = f"""
    #     SELECT txn_hash, account, boost_delegate, receiver
    #     FROM boost_data 
    #     WHERE account = boost_delegate
        
    #     ORDER BY block DESC 
    # """
    results = con.execute(sql).fetchdf()
    pd.set_option('display.max_colwidth', None)
    print(results)
    print(results.iloc[0, 1])

    # sums = results.select_dtypes(include=['number']).sum()
    # print(sums)

    assert False

def query_claim_data_pct():
    
    WEEK = utils.utils.WEEK  # Assuming this is the duration of a week in seconds
    DAY = utils.utils.DAY  # Assuming this is the duration of a day in seconds

    weeks = [{'week_number': i, 'start_ts': utils.utils.get_week_start_ts(week_number=i)} for i in range(12, vault.getWeek()+1)]

    sql_queries = []

    for week in weeks:
        day_calculations = []
        for day_number in range(7):
            day_start_ts = week['start_ts'] + (DAY * day_number)
            day_end_ts = day_start_ts + DAY
            day_calculations.append(f"SUM(CASE WHEN timestamp >= {day_start_ts} AND timestamp < {day_end_ts} THEN amount ELSE 0 END) / (SELECT SUM(amount) FROM boost_data WHERE timestamp >= {week['start_ts']} AND timestamp < {week['start_ts'] + WEEK}) * 100 AS day_{day_number}_pct")

        day_calculations_str = ', '.join(day_calculations)

        sql_query = f"""
        SELECT
            {week['week_number']} AS week_number,
            {day_calculations_str}
        FROM
            boost_data
        WHERE
            timestamp >= {week['start_ts']} AND timestamp < {week['start_ts'] + WEEK}
        GROUP BY
            week_number
        """

        sql_queries.append(sql_query)

    # Concatenate all week queries with "UNION ALL" and add an ORDER BY clause
    final_query = f"SELECT * FROM ({' UNION ALL '.join(sql_queries)}) AS weekly_data ORDER BY week_number ASC"



    # fetch raw txn data from wavey repo and put into dataframe
    url = 'https://raw.githubusercontent.com/wavey0x/open-data/master/raw_boost_data.json'
    data = requests.get(url).json()['data']
    df = pd.DataFrame(data)

    # load data into virtual db
    con = duckdb.connect(database=':memory:')
    con.register('boost_data', df)

    # Write any SQL to query the raw data

    results = con.execute(final_query).fetchdf()
    
    
    results = results.round(2)
    results['week_number'] = results['week_number'].astype(int)
    print(results)

def query_claim_data_amounts():
    WEEK = utils.utils.WEEK
    DAY = utils.utils.DAY

    weeks = []
    for i in range(12, 26):
        week = {
            'week': i,
            'days': []
        }
        week_start = utils.utils.get_week_start_ts(week_number=i)
        for x in range(0, 7):
            day = (x, week_start + (DAY * x), week_start + (DAY * (x+1)))
            week['days'].append(day)
        weeks.append(week)

    sql_queries = []

    base_query = """
    SELECT 
        '{week_number}' AS week_number,
    """

    for week in weeks:
        week_number = week['week']
        day_columns = []
        for day in week['days']:
            day_number, start_ts, end_ts = day
            column_name = f"day_{day_number+1}"
            day_query = f"SUM(CASE WHEN timestamp >= {start_ts} AND timestamp < {end_ts} THEN amount ELSE 0 END) AS {column_name}"
            day_columns.append(day_query)
        day_columns_str = ",\n".join(day_columns)
        full_query = base_query.format(week_number=week_number) + day_columns_str + "\nFROM boost_data"
        sql_queries.append(full_query)
    
    # Combine all week queries with UNION ALL
    final_query = "\nUNION ALL\n".join(sql_queries)

    # fetch raw txn data from wavey repo and put into dataframe
    url = 'https://raw.githubusercontent.com/wavey0x/open-data/master/raw_boost_data.json'
    data = requests.get(url).json()['data']
    df = pd.DataFrame(data)

    # load data into virtual db
    con = duckdb.connect(database=':memory:')
    con.register('boost_data', df)

    # Write any SQL to query the raw data

    results = con.execute(final_query).fetchdf()
    results = results.astype(int)
    formatted_df = results.applymap(lambda x: f"{x:,}")
    print()
    print(formatted_df)

def trim_str(s):
    return s[:4] + '..' + s[-4:]