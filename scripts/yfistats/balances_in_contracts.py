from brownie import Contract, network
from eth_abi import encode_single, encode_abi
import pyodbc

from datetime import timedelta
import os

from ...sql.mssqlserver.utils import conn, cursor, getLastBlockOnDate

def CheckMakerDebtAtBlock(block_number):
    try:
        proxy_registry = Contract('0x4678f0a6958e4D2Bc4F1BAF7Bc52E8F3564f3fE4')
        cdp_manager = Contract('0x5ef30b9986345249bc32d8928B7ee64DE9435E39')
        ychad = Contract('ychad.eth')
        vat = Contract('0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B')
    except:
        proxy_registry = Contract.from_explorer('0x4678f0a6958e4D2Bc4F1BAF7Bc52E8F3564f3fE4')
        cdp_manager = Contract.from_explorer('0x5ef30b9986345249bc32d8928B7ee64DE9435E39')
        ychad = Contract.from_explorer('ychad.eth')
        vat = Contract.from_explorer('0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B')
    proxy = proxy_registry.proxies(ychad)
    cdp = cdp_manager.first(proxy)
    urn = cdp_manager.urns(cdp)
    ilk = encode_single('bytes32', b'YFI-A')
    art = vat.urns(ilk, urn, block_identifier = block_number).dict()["art"]
    rate = vat.ilks(ilk, block_identifier = block_number).dict()["rate"]
    debt = art * rate / 1e27
    print("block: " + str(block_number))
    print("art: " + str(art))
    print("rate: " + str(rate))
    print("debt: " + str(debt))
    return debt

def CheckMakerDebtAtDate(date_string):
    print("date: " + date_string)
    block = getLastBlockOnDate(date_string)
    debt = CheckMakerDebtAtBlock(block)
    return debt 

def CheckMakerYFICollatAtBlock(block_number):
    try:
        proxy_registry = Contract('0x4678f0a6958e4D2Bc4F1BAF7Bc52E8F3564f3fE4')
        cdp_manager = Contract('0x5ef30b9986345249bc32d8928B7ee64DE9435E39')
        ychad = Contract('ychad.eth')
        vat = Contract('0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B')
    except:
        proxy_registry = Contract.from_explorer('0x4678f0a6958e4D2Bc4F1BAF7Bc52E8F3564f3fE4')
        cdp_manager = Contract.from_explorer('0x5ef30b9986345249bc32d8928B7ee64DE9435E39')
        ychad = Contract.from_explorer('ychad.eth')
        vat = Contract.from_explorer('0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B')
    proxy = proxy_registry.proxies(ychad)
    cdp = cdp_manager.first(proxy)
    urn = cdp_manager.urns(cdp)
    ilk = encode_single('bytes32', b'YFI-A')
    ink = vat.urns(ilk, urn, block_identifier = block_number).dict()["ink"]
    return ink

def CheckMakerYFICollatAtDate(date_string):
    print("date: " + date_string)
    block = getLastBlockOnDate(date_string)
    print(block)
    ink = CheckMakerYFICollatAtBlock(block)
    return ink

def CheckUnitDebtAtBlock(block_number):
    try:
        ychad = Contract('ychad.eth')
        unitVault = Contract("0xb1cff81b9305166ff1efc49a129ad2afcd7bcf19")
    except:
        ychad = Contract.from_explorer('ychad.eth')
        unitVault = Contract.from_explorer("0xb1cff81b9305166ff1efc49a129ad2afcd7bcf19")

    yfi_address = "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e"
    debt = unitVault.getTotalDebt(yfi_address,ychad, block_identifier = block_number) 
    print("block: " + str(block_number))
    print("debt: " + str(debt))
    return debt

def CheckUnitDebtAtDate(date_string):
    print("date: " + date_string)
    block = getLastBlockOnDate(date_string)
    debt = CheckUnitDebtAtBlock(block)
    return debt

def CheckUnitYFICollatAtBlock(block_number):
    try:
        ychad = Contract('ychad.eth')
        unitVault = Contract("0xb1cff81b9305166ff1efc49a129ad2afcd7bcf19")
    except:
        ychad = Contract.from_explorer('ychad.eth')
        unitVault = Contract.from_explorer("0xb1cff81b9305166ff1efc49a129ad2afcd7bcf19")

    yfi_address = "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e"
    bal = unitVault.collaterals(yfi_address,ychad, block_identifier = block_number) 
    print("block: " + str(block_number))
    print("bal: " + str(bal))
    return bal

def CheckUnitYFICollatAtDate(date_string):
    print("date: " + date_string)
    block = getLastBlockOnDate(date_string)
    debt = CheckUnitYFICollatAtBlock(block)
    return debt

def CheckKp3rEscrowBalanceAtBlock(block_number):
    yearnKp3rWallet = "0x5f0845101857d2a91627478e302357860b1598a1"
    try:
        escrow = Contract("0xf14cb1feb6c40f26d9ca0ea39a9a613428cdc9ca")
        kp3rLPtoken = Contract("0xaf988aff99d3d0cb870812c325c588d8d8cb7de8")
    except:
        escrow = Contract.from_explorer("0xf14cb1feb6c40f26d9ca0ea39a9a613428cdc9ca")
        kp3rLPtoken = Contract.from_explorer("0xaf988aff99d3d0cb870812c325c588d8d8cb7de8")
    bal = escrow.userLiquidityTotalAmount(yearnKp3rWallet,kp3rLPtoken, block_identifier = block_number)
    print('escrow: ' + str(bal))
    return bal

def CheckKp3rEscrowBalanceAtDate(date_string):
    print('date: ' + date_string)
    block = getLastBlockOnDate(date_string)
    bal = CheckKp3rEscrowBalanceAtBlock(block)
    return bal

# NOTE: Block lists

def GetMakerBlockList():
    query = cursor.execute("""
        SELECT a.*
        FROM
        (
            SELECT date,
                MAX(blockHeight) maxBlockHeight
            FROM eth.blockMetrics
            WHERE date >= '2021-02-09' and date < cast(getdate() as date)
            GROUP BY date
        ) AS a
        LEFT JOIN
        (
            SELECT *
            FROM yfi.cdps
            WHERE token_dbid = 14175
        ) AS b ON a.maxBlockHeight = b.blockHeight
        where b.blockHeight is null
        """)
    blockList = query.fetchall()
    return blockList

def GetMakerYFICollatBlockList():
    query = cursor.execute("""
        SELECT a.*
        FROM
        (
            SELECT date,
                MAX(blockHeight) maxBlockHeight
            FROM eth.blockMetrics
            WHERE date >= '2021-02-09' and date < cast(getdate() as date)
            GROUP BY date
        ) AS a
        LEFT JOIN
        (
            SELECT *
            FROM yfi.cdpCollat
            WHERE token_dbid = 14079 and platform = 'Maker'
        ) AS b ON a.maxBlockHeight = b.blockHeight
        where b.blockHeight is null
        """)
    blockList = query.fetchall()
    return blockList

def GetUnitBlockList():
    query = cursor.execute("""
        SELECT a.*
        FROM
        (
            SELECT date,
                MAX(blockHeight) maxBlockHeight
            FROM eth.blockMetrics
            WHERE date >= '2021-03-05' and date < cast(getdate() as date)
            GROUP BY date
        ) AS a
        LEFT JOIN
        (
            SELECT *
            FROM yfi.cdps
            WHERE token_dbid = 21364
        ) AS b ON a.maxBlockHeight = b.blockHeight
        where b.blockHeight is null
        """)
    blockList = query.fetchall()
    return blockList
    
def GetUnitYFICollatBlockList():
    query = cursor.execute("""
        SELECT a.*
        FROM
        (
            SELECT date,
                MAX(blockHeight) maxBlockHeight
            FROM eth.blockMetrics
            WHERE date >= '2021-03-05' and date < cast(getdate() as date)
            GROUP BY date
        ) AS a
        LEFT JOIN
        (
            SELECT *
            FROM yfi.cdpCollat
            WHERE token_dbid = 14079 and platform = 'Unit'
        ) AS b ON a.maxBlockHeight = b.blockHeight
        where b.blockHeight is null
        """)
    blockList = query.fetchall()
    return blockList

def GetKp3RBlockList():
    query = cursor.execute("""
        SELECT a.*
        FROM
        (
            SELECT date,
                MAX(blockHeight) maxBlockHeight
            FROM eth.blockMetrics
            WHERE date >= '2021-03-25' and date < cast(getdate() as date)
            GROUP BY date
        ) AS a
        LEFT JOIN
        (
            SELECT * 
            FROM yfi.balancesInContracts
            WHERE token_dbid = 22123
             and depositor_address_dbid = 29648049
             and foreign_contract_dbid = 3469081
        ) AS b ON a.maxBlockHeight = b.blockHeight
        where b.blockHeight is null
        """)
    blockList = query.fetchall()
    return blockList

def GetKp3RBlockList2():
    query = cursor.execute("""
        SELECT a.*
        FROM
        (
            SELECT date,
                MAX(blockHeight) maxBlockHeight
            FROM eth.blockMetrics
            WHERE date >= '2021-01-23' and date < '2021-04-08'
            GROUP BY date
        ) AS a
        LEFT JOIN
        (
            SELECT * 
            FROM yfi.balancesInContracts
            WHERE token_dbid = 22123
             and depositor_address_dbid = 29648049
             and foreign_contract_dbid = 1757978
        ) AS b ON a.maxBlockHeight = b.blockHeight
        where b.blockHeight is null
        """)
    blockList = query.fetchall()
    return blockList

def GetKp3RBlockList3():
    query = cursor.execute("""
        SELECT a.*
        FROM
        (
            SELECT date,
                MAX(blockHeight) maxBlockHeight
            FROM eth.blockMetrics
            WHERE date >= '2021-01-23' and date < '2021-04-08'
            GROUP BY date
        ) AS a
        LEFT JOIN
        (
            SELECT * 
            FROM yfi.balancesInContracts
            WHERE token_dbid = 22123
             and depositor_address_dbid = 29648049
             and foreign_contract_dbid = 1757965
        ) AS b ON a.maxBlockHeight = b.blockHeight
        where b.blockHeight is null
        """)
    blockList = query.fetchall()
    return blockList

def GetMakerCollateralBlockList(collateral_token_address):
    pass

def GetUnitCollateralBlockList(collateral_token_address):
    pass

def main():
    # MakerDAO

    for datapoint in GetMakerBlockList():
        datetime = datapoint[0]
        block = datapoint[1]

        debt = CheckMakerDebtAtBlock(block)
        print(debt)
        cursor.execute("""
            insert into yfi.cdps (blockHeight, token_dbid, debt)
            VALUES (""" + str(block) + """,14175,""" + str(debt) + """)
                """)
        conn.commit()

    for datapoint in GetMakerYFICollatBlockList():
        datetime = datapoint[0]
        block = datapoint[1]

        ink = CheckMakerYFICollatAtBlock(block)
        print('block: ' + str(block))
        print('ink: ' + str(ink))
        cursor.execute("""
            insert into yfi.cdpCollat (blockHeight, token_dbid, amount, platform)
            VALUES (""" + str(block) + """,14079,""" + str(ink) + """,'Maker')
                """)
        conn.commit()

    # Unit.xyz

    for datapoint in GetUnitBlockList():
        datetime = datapoint[0]
        block = datapoint[1]

        debt = CheckUnitDebtAtBlock(block)
        print(debt)
        cursor.execute("""
            insert into yfi.cdps (blockHeight, token_dbid, debt)
            VALUES (""" + str(block) + """,21364,""" + str(debt) + """)
                """)
        conn.commit()

    for datapoint in GetUnitYFICollatBlockList():
        datetime = datapoint[0]
        block = datapoint[1]

        bal = CheckUnitYFICollatAtBlock(block)
        cursor.execute("""
            insert into yfi.cdpCollat (blockHeight, token_dbid, amount, platform)
            VALUES (""" + str(block) + """,14079,""" + str(bal) + """,'Unit')
                """)
        conn.commit()

    # Kp3r Escrow

    for datapoint in GetKp3RBlockList():
        datetime = datapoint[0]
        block = datapoint[1]

        bal = CheckKp3rEscrowBalanceAtBlock(block)
        print(bal)
        cursor.execute("""
            insert into yfi.balancesInContracts (blockHeight, token_dbid, balance, foreign_contract_dbid, depositor_address_dbid)
            VALUES (""" + str(block) + """,22123,""" + str(bal) + """,3469081,29648049)
                """)
        conn.commit()

    for datapoint in GetKp3RBlockList2():
        datetime = datapoint[0]
        block = datapoint[1]

        cursor.execute("""
            insert into yfi.balancesInContracts (blockHeight, token_dbid, balance, foreign_contract_dbid, depositor_address_dbid)
            VALUES (""" + str(block) + """,22123,209000000000000000000,1757978,29648049)
                """)
        conn.commit()

    for datapoint in GetKp3RBlockList3():
        datetime = datapoint[0]
        block = datapoint[1]

        cursor.execute("""
            insert into yfi.balancesInContracts (blockHeight, token_dbid, balance, foreign_contract_dbid, depositor_address_dbid)
            VALUES (""" + str(block) + """,22123,209352935856684835314,1757965,29648049)
                """)
        conn.commit()
    






        