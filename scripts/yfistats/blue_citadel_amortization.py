from brownie import Contract, web3
from ypricemagic.utils.utils import contract_creation_block
from ...sql.mssqlserver.utils import cursor, sqla_engine
from ypricemagic.utils.events import decode_logs,get_logs_asap
import pandas as pd
import sqlalchemy
from decimal import Decimal
from tqdm import tqdm

def fetch_packages():
    blue_citadel = Contract('0xF124534bfa6Ac7b89483B401B4115Ec0d27cad6A')
    topic = [web3.keccak(text="Deposit(address,uint256)").hex()]
    topic = ['0x4d924f2be6d90da83be47ca6bc3c90e0f5c5e365d7e9797faeb4b9f823505a78']
    events = decode_logs(get_logs_asap(blue_citadel.address, topic))
    df = pd.json_normalize(pd.DataFrame(events)[0])
    # NOTE: Filter out non-YFI packages
    df = df[df['token'] == '0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e'].reset_index().drop('index',axis=1)
    df['amount'] = df['amount'].apply(Decimal)
    return df

def fetch_query_blocks_for_escrow(contract):
    creation_block = contract_creation_block(contract)
    return cursor.execute(f"""
        DECLARE @startblock INT= {creation_block};
        DECLARE @startdate DATE=
        (
            SELECT date
            FROM eth.blockMetrics
            WHERE BlockHeight = @startblock
        );
        SELECT MAX(blockHeight)
        FROM
        (
            SELECT Last_Date_of_Month
            FROM Dim_Date a
            WHERE Calendar_Date >= @startdate
                AND Last_Date_of_Month <= GETUTCDATE()
            GROUP BY Last_Date_of_Month
        ) a
        LEFT JOIN eth.blockMetrics b ON a.Last_Date_of_Month = b.date
        GROUP BY Last_Date_of_Month;
    """).fetchall()

def main():
    df = fetch_packages()
    df.to_sql('blue_citadel',sqla_engine,'yfi',if_exists='replace',index=False,dtype={"amount": sqlalchemy.DECIMAL(38, 0)},)
    all = []
    for contract in tqdm(df['escrow']):
        print(contract)
        blocks = fetch_query_blocks_for_escrow(contract)
        contract = Contract(contract)
        for block in blocks:
            block = block[0]
            deets = {
                'escrow': contract.address,
                'block': block,
                'locked': contract.locked(block_identifier = block),
                'claimable': contract.unclaimed(block_identifier = block),
                'claimed': contract.total_claimed(block_identifier = block)
            }
            all.append(deets)
    df = pd.DataFrame(all)
    df['locked'] = df['locked'].apply(Decimal)
    df['claimable'] = df['claimable'].apply(Decimal)
    df['claimed'] = df['claimed'].apply(Decimal)
    print(df)
    df.to_sql('vesting_package_stats',sqla_engine,'yfi',if_exists='replace',index=False,dtype={"amount": sqlalchemy.DECIMAL(38, 0)},)