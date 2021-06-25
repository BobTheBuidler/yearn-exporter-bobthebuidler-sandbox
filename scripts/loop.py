from brownie import run
import time
from tqdm import tqdm

loop_interval_minutes = 60

neverending = 1
while neverending == 1:
    run('export_strategies_to_sql')
    run('scrape_harvests')
    run('scrape_curve_pools')
    for second in tqdm(range(1,loop_interval_minutes * 60)):
        time.sleep(1)