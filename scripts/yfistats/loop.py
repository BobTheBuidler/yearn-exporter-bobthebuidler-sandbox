from brownie import run
import time
from tqdm import tqdm

loop_interval_minutes = 60

neverending = 1
while neverending == 1:
    run('yfistats/export_strategies_to_sql')
    run('yfistats/scrape_harvests')
    run('yfistats/scrape_curve_pools')
    run('yfistats/balances_in_contracts')
    for second in tqdm(range(1,loop_interval_minutes * 60)):
        time.sleep(1)