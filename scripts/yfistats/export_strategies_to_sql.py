import os
from brownie import convert
from pprint import pprint
import pyodbc

def main():
    known = []
    knownvaults = []
    conn = pyodbc.connect(os.environ['DB_CONN_STRING'])
    cursor = conn.cursor()
    result = cursor.execute("""
        select distinct e.address vault_address, d.address strategy_address
        from sucks.vault_strategies a 
        left join sucks.vault_strategy_pairs b on a.strategy_dbid = b.strategy_dbid
        left join sucks.vaults c on b.vault_dbid = c.vault_dbid
        left join eth.addresses d on a.strategy_address_dbid = d.address_dbid
        left join eth.addresses e on c.vault_address_dbid = e.address_dbid
    """).fetchall()
    for item in result:
        if item[0] is not None and item[1] is not None:
            vaddress = convert.to_address(item[0])
            saddress = convert.to_address(item[1])
            pair = (vaddress,saddress)
            known.append(pair)
    for item in known:
        vaddress = item[0]
        if vaddress not in knownvaults:
            knownvaults.append(vaddress)
    from yearn.v1.registry import Registry as RegistryV1
    unknownpairs = []
    for vault in RegistryV1().vaults:          
        vaddress = convert.to_address(vault.vault.address)
        saddress = convert.to_address(vault.strategy.address)
        vname = vault.name
        sname = vault.strategy._name
        if (vaddress,saddress) not in known:
            unknownpairs.append((vname,vaddress, saddress,sname))
    pprint(unknownpairs)
    print(len(unknownpairs))

    for item in unknownpairs:
        cursor.execute("""
            declare @vault varchar(70) = '""" + item[1] + """'
            declare @strat varchar(70) = '""" + item[2] + """'

            declare @vaultaddressdbid bigint = (select address_dbid from eth.addresses where address = @vault)
            if @vaultaddressdbid is null
                insert into eth.addresses (address) values (@vault)
            declare @vaultdbid bigint = (select vault_dbid from sucks.vaults where vault_address_dbid = @vaultaddressdbid)
            if @vaultdbid is null
                insert into sucks.vaults (vault_address_dbid) values (@vaultaddressdbid)

            declare @strataddressdbid bigint = (select address_dbid from eth.addresses where address = @strat)
            if @strataddressdbid is null
                insert into eth.addresses (address) values (@strat)
            declare @stratdbid bigint = (select strategy_dbid from sucks.vault_strategies where strategy_address_dbid = @strataddressdbid)
            if @stratdbid is null
                insert into sucks.vault_strategies(strategy_address_dbid) values (@strataddressdbid)

            insert into sucks.vault_strategy_pairs (vault_dbid,strategy_dbid,blockHeight,v2index)
            values (@vaultdbid,@stratdbid,0,-1)
        """)
        conn.commit()
        print(str(item) + ' added')

    from yearn.v2.registry import Registry
    registry = Registry()
    registry.load_strategies()
    unknownpairs = []
    for vault in registry.vaults:
        for strategy in vault.strategies + vault.revoked_strategies:            
            vaddress = convert.to_address(vault.vault.address)
            saddress = convert.to_address(strategy.strategy.address)
            vname = vault.name
            sname = strategy.name
            if (vaddress,saddress) not in known:
                unknownpairs.append((vname,vaddress, saddress,sname))
    pprint(unknownpairs)
    print(len(unknownpairs))

    for item in unknownpairs:
        cursor.execute("""
            declare @vault varchar(70) = '""" + item[1] + """'
            declare @strat varchar(70) = '""" + item[2] + """'

            declare @vaultaddressdbid bigint = (select address_dbid from eth.addresses where address = @vault)
            if @vaultaddressdbid is null
                insert into eth.addresses (address) values (@vault)
            declare @vaultdbid bigint = (select vault_dbid from sucks.vaults where vault_address_dbid = @vaultaddressdbid)
            if @vaultdbid is null
                insert into sucks.vaults (vault_address_dbid) values (@vaultaddressdbid)

            declare @strataddressdbid bigint = (select address_dbid from eth.addresses where address = @strat)
            if @strataddressdbid is null
                insert into eth.addresses (address) values (@strat)
            declare @stratdbid bigint = (select strategy_dbid from sucks.vault_strategies where strategy_address_dbid = @strataddressdbid)
            if @stratdbid is null
                insert into sucks.vault_strategies(strategy_address_dbid) values (@strataddressdbid)

            insert into sucks.vault_strategy_pairs (vault_dbid,strategy_dbid,blockHeight,v2index)
            values (@vaultdbid,@stratdbid,0,0)
        """)
        conn.commit()
        print(str(item) + ' added')
