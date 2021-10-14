from decimal import Decimal
from ...sql.mssqlserver.utils import cursor, conn, sqla_engine
from brownie import accounts, chain, Contract, convert, web3
import os, requests, time
import pandas as pd
import pyodbc, csv, json, ast, logging
from tqdm import tqdm
from ypricemagic.utils.events import decode_logs, get_logs_asap
from ypricemagic.utils.utils import contract_creation_block

WETHABI = [{"constant":True,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[],"name":"deposit","outputs":[],"payable":True,"stateMutability":"payable","type":"function"},{"constant":True,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"payable":True,"stateMutability":"payable","type":"fallback"},{"anonymous":False,"inputs":[{"indexed":True,"name":"src","type":"address"},{"indexed":True,"name":"guy","type":"address"},{"indexed":False,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"name":"src","type":"address"},{"indexed":True,"name":"dst","type":"address"},{"indexed":False,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"name":"dst","type":"address"},{"indexed":False,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"name":"src","type":"address"},{"indexed":False,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]

startdate = '2020-06-01'

def Addresses():
    return cursor.execute(
        """
        SELECT trackedAddress, isnull(yfistats_display_name,public_notes)
        FROM ethTools.trackedAddresses a left join eth.addresses b on a.trackedAddress = b.address
        WHERE yearn = 1
    """
    ).fetchall()

def Blocks(startdate):
    return cursor.execute(
        f"""
        SELECT DISTINCT 
            Calendar_Date, 
        (
            SELECT MAX(blockHeight)
            FROM eth.blockMetrics
            WHERE date = a.Calendar_Date
        ) block, 
            b.*
        FROM Dim_Date a
            LEFT JOIN yfi.EthWethBalanceFixer b ON a.Calendar_Date = b.Date
        WHERE Calendar_Date >= '2020-06-01'
            AND Calendar_Date <= GETUTCDATE()
            AND b.address_dbid IS NULL
        ORDER BY Calendar_Date ASC;
    """
    ).fetchall()


def get_weth_xfers(blocks):
    print('fetching weth deposits + withdrawals')
    dep = [web3.keccak(text="Deposit(address,uint256)").hex()]
    wd = [web3.keccak(text="Withdrawal(address,uint256)").hex()]
    addresses = [convert.to_address(address[0]) for address in Addresses()]
    weth = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
    deps, wds = [],[]
    for block in tqdm(blocks):
        block = int(block)
        events = decode_logs(get_logs_asap(weth, dep, from_block=block, to_block=block))
        deposits = [event for event in events]
        deposits = [event for event in events if event['dst'] in addresses]
        for event in deposits:
            event = {
                'blockNumber': block,
                'timeStamp': chain[block].timestamp,
                'hash': event.transaction_hash.hex(),
                'from': event['src'],
                'to': weth,
                'value': event['wad'],
                'token_dbid': 13600
            }
            deps.append(event)
        events = decode_logs(get_logs_asap(weth, wd, from_block=block, to_block=block))
        withdrawals = [event for event in events if event['src'] in addresses]
        for event in withdrawals:
            block = event.block_number
            event = {
                'blockNumber': block,
                'timeStamp': chain[block].timestamp,
                'hash': event.transaction_hash.hex(),
                'from': event['src'],
                'to': weth,
                'value': Decimal(event['wad']),
                'token_dbid': 13600
            }
            wds.append(event)
    df = pd.DataFrame(deps).append(pd.DataFrame(wds))
    print(df)
    return df

def disperse_app_handler(internal_txs_df):
    print('fetching internal txs sent thru disperse.app')
    ETHERSCAN_TOKEN = os.environ['ETHERSCAN_TOKEN']
    filtered = internal_txs_df[internal_txs_df['to'] == '0xd152f549545093347a162dce210e7293f1452150']
    filtered = filtered[filtered['token_dbid'] == -1]
    all = []
    for hash in tqdm(filtered['hash'].unique()):
        proceed = False
        while not proceed:
            url = f'https://api.etherscan.io/api?module=account&action=txlistinternal&txhash={hash}&apikey={ETHERSCAN_TOKEN}'
            response = requests.get(url)
            if response.json()['result'] == 'Max rate limit reached':
                time.sleep(1)
            else:
                #if len(response.json()['result']) == 0:
                #    proceed = True
                #    break
                for int_tx in response.json()['result']:
                    int_tx['token_dbid'] = -1
                    int_tx['hash'] = hash
                    all.append(int_tx)
                proceed = True
    df = pd.DataFrame(all)
    df = df[df['from'] == '0xd152f549545093347a162dce210e7293f1452150']
    print(df)
    return df

def get_internal_transactions():
    ETHERSCAN_TOKEN = os.environ['ETHERSCAN_TOKEN']
    all = []
    for address in Addresses():
        print(address)
        address = address[0]
        nextStartBlock = 0
        current_block = chain[-1].number - 100
        done, last_int_tx = False, None
        while nextStartBlock < current_block and not done:
            proceed = False
            while not proceed:
                url = f'https://api.etherscan.io/api?module=account&action=txlistinternal&address={address}&startblock={nextStartBlock}&sort=asc&apikey={ETHERSCAN_TOKEN}'
                response = requests.get(url)
                if response.json()['result'] == 'Max rate limit reached':
                    time.sleep(1)
                else:
                    if len(response.json()['result']) == 0:
                        proceed, done = True, True
                        break
                    for int_tx in response.json()['result']:
                        int_tx['token_dbid'] = -1
                        if last_int_tx == int_tx:
                            proceed, done = True, True
                            break
                        all.append(int_tx)
                        if int(int_tx['blockNumber']) > nextStartBlock:
                            nextStartBlock = int(int_tx['blockNumber'])
                            last_int_tx = int_tx
                    proceed = True
    df = pd.DataFrame(all)
    print(df)
    df = df.append(disperse_app_handler(df))
    return df


def main():
    logging.basicConfig(level=logging.INFO)
    addresses = Addresses()
    blocks = Blocks(startdate)
    weth = Contract.from_abi('WETH9','0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',WETHABI)
    
    for block in blocks:
        for address in addresses:
            #ethbalance = accounts.at(adhdress[0], force=True).balance(block_identifier = block[1])
            ethbalance = web3.eth.get_balance(convert.to_address(address[0]), block_identifier = block[1])/10 ** 18
            wethbalance = weth.balanceOf(address[0], block_identifier = block[1])/10 ** 18
            print(f"{block[0]} {address[1]}")
            print(f"ETH Balance: {ethbalance}")
            print(f"WETH Balance: {wethbalance}")
            print('')
            cursor.execute(f"""
                declare @address_dbid bigint = (select address_dbid from eth.addresses where address = '{address[0]}')
                Insert into yfi.EthWethBalanceFixer (token_dbid,Date,address_dbid,balance)
                values (-1,'{block[0]}',@address_dbid,{ethbalance * 10 ** 18})
                    ,(13600,'{block[0]}',@address_dbid,{wethbalance * 10 ** 18})
            """)
            conn.commit()

    df = get_internal_transactions()
    print(df[df['blockNumber'] == 11711756])
    # check weth
    df_weth = get_weth_xfers(df['blockNumber'].unique())
    df = df.append(df_weth)
    df.to_sql('internal_transactions',sqla_engine,'yfi',if_exists='replace',index=False,)
    print('sql insert complete')
    
            