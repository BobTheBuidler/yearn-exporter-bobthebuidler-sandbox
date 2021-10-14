from tqdm import tqdm
from ...sql.mssqlserver.utils import (conn, cursor, execStoredProcNoInput)
import time
from brownie import Contract, chain
from joblib import Parallel, delayed

def nonVerifiedContracts():
    nvc = []
    #nvc.append('0x153Fe8894a76f14bC8c8B02Dd81eFBB6d24E909f')
    #nvc.append('0xAa12d6c9d680EAfA48D8c1ECba3FCF1753940A12')
    nvc.append('0xbd5fDda17bC27bB90E37Df7A838b1bFC0dC997F5')
    #nvc.append('0xD1D5A4c0eA98971894772Dcd6D2f1dc71083C44E')
    nvclist = []
    for contract in nvc:
        nvclist.append('Source for ' + contract + ' has not been verified')

def GetBrownieReceipt(_txHash):
        go = 0
        while go == 0:
            try:
                brownieReceipt = chain.get_transaction(_txHash)
                go = 1
            except Exception as e:
                print(str(e))
                time.sleep(15)
        return brownieReceipt

def brownieDownloadContractObjects(brownieReceipt):
        nonVerified = nonVerifiedContracts()
        for thing in brownieReceipt.events:
            proceed = 0
            while proceed == 0:
                try:
                    newcachedcontract = Contract(thing.address)
                    proceed = 1
                except Exception as e:
                    print(str(e))
                    try:
                        newcachedcontract = Contract.from_explorer(thing.address)
                        proceed = 1
                    except ValueError:
                        proceed = 1
                    except Exception as e:
                        print(str(e))
                        if str(e) in nonVerified:
                            proceed = 1
                        else:
                            time.sleep(60)

def GetFunctionName(_txHash):
    brownieReceipt = GetBrownieReceipt(_txHash)
    functionName = brownieReceipt.fn_name
    print(functionName)
    if functionName in ['withdraw','withdrawAll','withdrawETH','withdrawAllETH','ZapOut','migrateAll']:
        try:
            cursor.execute("""
            insert into yfi.ignoreNotHarvests
            select blockheight,transaction_index
            from eth.transactions
            where transactionhash = '""" + _txHash + """'  
            """)
            conn.commit()
        except:
            pass
    elif functionName is None:
        brownieDownloadContractObjects(brownieReceipt)
    return functionName

def sqlUpdateFunctionName(_txHash):
    print(_txHash)
    functionName = GetFunctionName(_txHash)
    if functionName == None:
        functionName = GetFunctionName(_txHash)
    if functionName is None:
        functionName = '@ NULL Fn Name @'
    if functionName == '':
        functionName = '@ Fallback Function @'
    cursor.execute("""
        DECLARE @txHash varchar(70) = '""" + _txHash + """'
        DECLARE @function_name varchar(max) = '""" + functionName + """'
        DECLARE @function_name_dbid bigint = (select function_name_dbid from function_names where function_name = @function_name)

        IF @function_name_dbid is null 
            insert into function_names (function_name) values (@function_name)
        SET @function_name_dbid = (select function_name_dbid from function_names where function_name = @function_name)
        UPDATE eth.transactions set function_name_dbid = @function_name_dbid where transactionHash = @txHash
    """)
    conn.commit()

def main():
    possibleWithdrawals = cursor.execute("""
        select distinct transactionHash
        from z_ethTools.trackedTxsUnion a
        left join eth.transactions b on a.blockHeight = b.blockHeight 
        and a.transaction_index = b.transaction_index  
        where function_name_dbid is null and yearn = 1 and transactionHash not in ('0x1fa860268243e47fb0d1e85d1fd408807fbf43b1b68c721f1f8e98498736345e','0x990412f9d496ad561dc55d05695eade79e1a669acce8b9854cd7d464dc9272bc','0x40ddbb7f28128b2d9ec932ad81bf04dffe57e1e162dc744ad467eb61ec39ec01')
    """).fetchall()

    
    Parallel(1, 'threading')(delayed(sqlUpdateFunctionName)(tx[0]) for tx in tqdm(possibleWithdrawals))    