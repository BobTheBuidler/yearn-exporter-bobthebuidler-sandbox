from ...sql.mssqlserver.utils import cursor, conn
from brownie import accounts, Contract, convert, web3

startdate = '2020-06-01'

def Addresses():
    addresses = cursor.execute("""
        SELECT trackedAddress, isnull(yfistats_display_name,public_notes)
        FROM ethTools.trackedAddresses a left join eth.addresses b on a.trackedAddress = b.address
        WHERE yearn = 1
    """).fetchall()
    return addresses

def Blocks(startdate):
    blocks = cursor.execute(f"""
        SELECT DISTINCT 
            Last_Date_of_Month, (select max(blockHeight) from eth.blockMetrics where date = a.Last_Date_of_Month) block
        FROM Dim_Date a
        WHERE Last_Date_of_Month >= '{str(startdate)}'
            AND Last_Date_of_Month <= GETUTCDATE()
            ORDER BY Last_Date_of_Month asc
    """).fetchall()
    return blocks

def main():
    addresses = Addresses()
    blocks = Blocks(startdate)
    weth = Contract('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
    
    for block in blocks:
        for address in addresses:
            #ethbalance = accounts.at(adhdress[0], force=True).balance(block_identifier = block[1])
            ethbalance = web3.eth.get_balance(convert.to_address(address[0]), block_identifier = block[1])/10 ** 18
            wethbalance = weth.balanceOf(address[0], block_identifier = block[1])/10 ** 18
            print(f"{block[0]} {address[1]}")
            print(f"ETH Balance: {ethbalance}")
            print(f"WETH Balance: {wethbalance}")
            print('')