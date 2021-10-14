import pyodbc
import os
from brownie import convert, Contract
from sqlalchemy.engine import create_engine

dbconnstring = os.environ['DB_CONN_STRING']

conn = pyodbc.connect(dbconnstring)

cursor = conn.cursor()

def sqla():
        return pyodbc.connect(os.environ['DB_CONN_STRING'])

sqla_engine = create_engine('mssql://',creator=sqla, fast_executemany=False)

### Get data from other data ###
###     Using 'token_symbol' ###

def sqlGetTokenAddressAndABIFromSymbol(token_symbol):
    query = cursor.execute("""        
        SELECT address, contract_abi
        FROM eth.tokens_join a left join eth.contract_abi b on a.contract_abi_dbid = b.contract_abi_dbid
        WHERE onchain_symbol = '""" + token_symbol + """'
        """)
    return query.fetchall()

def sqlGetReserveTokenAddressAndABIFromVaultTokenSymbol(token_symbol):
    query = cursor.execute("""        
        SELECT c.address,d.contract_abi
        FROM eth.tokens_join a left join pricedata.lptokencomposition b on a.token_dbid = b.lptokendbid left join eth.tokens_join c on b.reservetokendbid = c.token_dbid left join eth.contract_abi d on c.contract_abi_dbid = d.contract_abi_dbid
        WHERE a.onchain_symbol = '""" + token_symbol + """'
        """)
    return query.fetchall()

###     Using 'address'

def sqlGetAddressDbidFromAddress(address):
    query = cursor.execute("""
        SELECT address_dbid 
        FROM eth.addresses 
        WHERE address = '""" + address + """' 
        """ )
    return query.fetchall()[0][0]

def sqlGetTokenDbidFromAddress(address):
    query = cursor.execute("""
        SELECT token_dbid 
        FROM eth.tokens_join 
        WHERE address = '""" + address + """' 
        """ )
    return query.fetchall()[0][0]

###     USING 'address_dbid'

def sqlGetAddressFromAddressDbid(address_dbid):
    query = cursor.execute("""
        SELECT address 
        FROM eth.addresses 
        WHERE address_dbid = '""" + address_dbid + """' 
        """ )
    return query.fetchall()[0][0]

### Initialize contract from data ###

def initializeContractFromSymbol(token_symbol):
        data = sqlGetTokenAddressAndABIFromSymbol(token_symbol)
        address = convert.to_address(data[0][0])
        abi = data[0][1]
        return Contract(address)

def initializeReserveTokenContractFromVaultTokenSymbol(token_symbol):
        data = sqlGetReserveTokenAddressAndABIFromVaultTokenSymbol(token_symbol)
        address = convert.to_address(data[0][0])
        abi = data[0][1]
        return Contract(address)

def getLastBlockOnDate(date_string):
    query = cursor.execute("""
        SELECT max(blockHeight)
        FROM eth.blockMetrics
        WHERE date = '""" + date_string + """'
        """)
    return query.fetchone()[0]

def execStoredProcNoInput(proc_name):
    query = cursor.execute("""
        EXEC """ + proc_name)