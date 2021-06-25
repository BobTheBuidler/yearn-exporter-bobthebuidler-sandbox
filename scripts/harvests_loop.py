from brownie import run
import time
import os
from ..specialpurpose.bobskeys import ETHERSCAN_TOKEN

neverending = 1
while neverending == 1:
    os.environ["ETHERSCAN_TOKEN"] = ETHERSCAN_TOKEN
    #import MakerDebt as step0

    # these should already all be cached
    #import ScrapeControllers as step1
    #import ScrapeStrategiesV1 as step2

    run('export_strategies_to_sql')
    run('scrape_harvests')    
    time.sleep(60*240)

print('exited loop')
