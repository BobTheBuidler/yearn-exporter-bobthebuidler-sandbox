from brownie import run
import time
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)

loop_interval_minutes = 1

neverending = 1
#while neverending == 1:
def loop():
    run('yfistats/scrape_curve_pools')
    run('yfistats/balances_in_contracts')
    run('yfistats/yla_prices')
    run('yfistats/scrape_function_names')
    run('yfistats/scrape_vault_api_versions')
    #run('yfistats/export_strategies_to_sql')
    run('yfistats/scrape_harvests')
    run('yfistats/export_strategies_to_sql')
    print('[Entering Sleep Mode] zzz...')
    #for second in tqdm(range(1,int(loop_interval_minutes * 60))):
    #    time.sleep(1)

def main():
    loop()