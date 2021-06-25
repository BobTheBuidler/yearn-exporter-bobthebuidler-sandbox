import math
import os
import time
from tqdm import tqdm

import pyodbc
from brownie import Contract, chain, network, convert
from web3 import Web3

def eventlessStrategies():
    def new(address):
        eventlessStrats.append(address)
    eventlessStrats = []
    new('0x07DB4B9b3951094B9E278D336aDf46a036295DE7')
    new('0x112570655b32A8c747845E0215ad139661e66E7F')
    new('0x134c08fAeE4F902999a616e31e0B7e42114aE320')
    new('0x2EE856843bB65c244F527ad302d6d2853921727e')
    new('0x39AFF7827B9D0de80D86De295FE62F7818320b76')
    new('0x40BD98e3ccE4F34c087a73DD3d05558733549afB')
    new('0x4FEeaecED575239b46d70b50E13532ECB62e4ea8')
    new('0x594a198048501A304267E63B3bAd0f0638da7628')
    new('0x6D6c1AD13A5000148Aa087E7CbFb53D402c81341')
    new('0x787C771035bDE631391ced5C083db424A4A64bD8')
    new('0x8C6698dC64f69231E3dC509CD7Ad72164D2389F7')
    new('0xa069E33994DcC24928D99f4BBEDa83AAeF00B5f3')
    new('0xA30d1D98C502378ad61Fe71BcDc3a808CF60b897')
    new('0xAa12d6c9d680EAfA48D8c1ECba3FCF1753940A12')
    new('0xb15Ee8e74dac2d77F9d1080B32B0F3562954aeE9')
    new('0xc999fb87AcA383A63D804A575396F65A55aa5aC8')
    new('0xd643cf07344428770b84973e049A1c18B5d47edE')
    return eventlessStrats

def nonVerifiedContracts():
    nvc = []
    nvc.append('0xAa12d6c9d680EAfA48D8c1ECba3FCF1753940A12')
    nvc.append('0xD1D5A4c0eA98971894772Dcd6D2f1dc71083C44E')
    nvc.append('0x153Fe8894a76f14bC8c8B02Dd81eFBB6d24E909f')
    print('Non-Verified Contracts:')
    for contract in nvc:
        print(contract)
    return nvc

def main():
    from ...sql.mssqlserver.utils import (conn, cursor, execStoredProcNoInput)

    def sqlGetHarvestEventList():
        querylist = cursor.execute("""
                SELECT * from yfi.[prepTxListForHarvestScraping]
                --where /* why are there any nulls? */ strategy_address is not null and block_number >= 12156757 and strategy_address not in ( '0x406813fF2143d178d1Ebccd2357C20A424208912','0x39AFF7827B9D0de80D86De295FE62F7818320b76')
                where strategy_address != '0x0000000000000000000000000000000000000000'
                --and vault_dbid in (2, 48, 45)
                order by strategy_abi, want_token_address desc """)
        return tqdm(querylist.fetchall())

    def sqlInsertHarvestV1(calcMethod, block_number, transaction_index, strategy_dbid, wantEarned,strategistReward,performanceFee,depositor_share,vault_dbid,performanceFeePercent, strategistRewardPercent, FEE_DENOMINATOR):
        cursor.execute("""
            Insert into sucks.vault_harvests(block_number,transaction_index,strategy_dbid,                              wantEarned,strategist_fee,                                      governance_fee,depositor_share,                                 vault_dbid,version,             governance_fee_percent,             strategist_fee_percent,             depositor_percent, calcMethod)
            VALUES (""" + str(block_number) + """,""" + str(transaction_index) + """,""" + str(strategy_dbid) + """,""" + str(wantEarned) + """,""" + str(strategistReward) + """,""" + str(performanceFee) + """,""" + str(depositor_share) + """,""" + str(vault_dbid) + """,1,""" + str(performanceFeePercent) + """,""" + str(strategistRewardPercent) + """,""" + str(round(depositor_share/wantEarned*FEE_DENOMINATOR,20)) + """,'""" + calcMethod + """')
            """)
        conn.commit()

    def brownieDownloadContractObjects(brownieReceipt):
        for thing in brownieReceipt.events:
            #if thing.address != '0xD1D5A4c0eA98971894772Dcd6D2f1dc71083C44E':
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
                    except Exception as e:
                        print(str(e))
                        print('hello test')
                        time.sleep(60)

    def V1_GetPerformanceFeePercent(strategycontract,block_number):
        go = 0
        while go == 0:
            try:
                performanceFeePercent = strategycontract.performanceFee(block_identifier = block_number)
                go = 1
            except Exception as e:
                if str(e) == '429 Client Error: Too Many Requests for url: https://api.archivenode.io/XXXXXXXXXXXXXXX':
                    print('slow down!!')
                    time.sleep(30)
                else:
                    proceed = 0
                    while proceed == 0:
                        try:
                            performanceFeePercent = strategycontract.treasuryFee(block_identifier = block_number)
                            proceed = 1
                            go = 1
                        except Exception as e:
                            if str(e) == '429 Client Error: Too Many Requests for url: https://api.archivenode.io/XXXXXXXXXXXXXXX':
                                print('slow down!!')
                                time.sleep(30)
                            else:
                                print(str(e))
                                print(strategy_address)
        return performanceFeePercent
      
    def V1_GetStrategistRewardPercent(strategycontract,block_number):
        go = 0
        while go == 0:
            try:
                strategistRewardPercent = strategycontract.strategistReward(block_identifier = block_number)
                go = 1
            except Exception as e:
                if str(e) in ["""Contract 'StrategyDForceUSDT' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyDForceDAI' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyDForceUSDC' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCurveBUSDVoterProxy' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCurveYBUSDVoterProxy' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCurveYVoterProxy' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCurveBTCVoterProxy' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCurveSBTC' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCurveYCRV' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCurveYBUSD' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCurveYCRVVoter' object has no attribute 'strategistReward'""",
                                """Contract 'StrategyCreamYFI' object has no attribute 'strategistReward'"""]:
                    strategistRewardPercent = 0
                    go = 1
                elif str(e) == "429 Client Error: Too Many Requests for url: https://api.archivenode.io/XXXXXXXXXXXXXXX":
                    print(str(e))
                    time.sleep(30)
                else:
                    print(str(e))
                    print(strategycontract.name)
                    strategistRewardPercent = 999999999999999
                    go = 1
        return strategistRewardPercent

    def V1_GetFeeDenominator(strategycontract,block_number):
        try:
            FEE_DENOMINATOR = strategycontract.FEE_DENOMINATOR(block_identifier = block_number)
        except:
            try:
                FEE_DENOMINATOR = strategycontract.performanceMax(block_identifier = block_number)
            except Exception as e:
                try:
                    FEE_DENOMINATOR = strategycontract.c_base(block_identifier = block_number)
                except:
                    FEE_DENOMINATOR = strategycontract.DENOMINATOR(block_identifier = block_number)
        return FEE_DENOMINATOR

    def V1_SetVariables(strategycontract, block_number):
        performanceFeePercent = V1_GetPerformanceFeePercent(strategycontract,block_number)
        strategistRewardPercent = V1_GetStrategistRewardPercent(strategycontract,block_number)
        FEE_DENOMINATOR = V1_GetFeeDenominator(strategycontract,block_number)
        return performanceFeePercent, strategistRewardPercent, FEE_DENOMINATOR

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

    def GetHarvestLogs(_txHash):
        brownieReceipt = GetBrownieReceipt(_txHash)
        go = 0
        while go == 0:
            try:
                logs = brownieReceipt.events['Harvested']
                go = 1
            except Exception as e:
                if str(e) == "Event 'Harvested' did not fire.":
                    print(txHash + ' Harvested event did not fire.')
                    logs = []
                    go = 1
                else:
                    print(str(e))
                    time.sleep(15)
        return logs

    def GetHarvestEventsV1(_txHash):
        brownieReceipt = GetBrownieReceipt(_txHash)
        if brownieReceipt.fn_name in ['withdraw','withdrawAll','withdrawETH','withdrawAllETH','ZapOut','migrateAll']:
            cursor.execute("""
            insert into yfi.ignoreNotHarvests
            select blockheight,transaction_index
            from eth.transactions
            where transactionhash = '""" + _txHash + """'  
            """)
            conn.commit()
        elif brownieReceipt.fn_name in ['harvest','workForTokens','forceWork','work','forceHarvest']:
            pass
        else:
            print(brownieReceipt.fn_name)
            #print(brownieReceipt.info())
            brownieDownloadContractObjects(brownieReceipt)
        logs = GetHarvestLogs(_txHash)
        return logs

    def GetHarvestEventsV2(_txHash):
        brownieReceipt = GetBrownieReceipt(_txHash)
        if brownieReceipt.fn_name in ['butthead']: #'withdraw','withdrawAll','withdrawETH','withdrawAllETH']:
            cursor.execute("""
            insert into yfi.ignoreNotHarvests
            select blockheight,transaction_index
            from eth.transactions
            where transactionhash = '""" + _txHash + """'  
            """)
            conn.commit()
        elif brownieReceipt.fn_name in ['harvest','workForTokens','forceWork','work']:
            pass
        else:
            print(brownieReceipt.fn_name)
            brownieDownloadContractObjects(brownieReceipt)
        logs = GetHarvestLogs(_txHash)

        return logs

    #conn, cursor = dbConnSetup()
    worklist = sqlGetHarvestEventList()
    for HarvestEvent in worklist:
        version = HarvestEvent[0]
        vault_dbid = HarvestEvent[1]
        vault_address_dbid = HarvestEvent[2]
        strategy_dbid = HarvestEvent[3]
        strategy_address_dbid = HarvestEvent[4]
        vault_token_dbid = HarvestEvent[5]
        vault_token = HarvestEvent[6]
        reserve_token_dbid = HarvestEvent[7]
        block_number = HarvestEvent[8]
        transaction_index = HarvestEvent[9]
        txHash = HarvestEvent[10]
        token_dbid = HarvestEvent[11]
        gov_recd_value = HarvestEvent[12]
        strategy_address = convert.to_address(HarvestEvent[14])
            
        print(version)

        if version == "V1":
            strategycontract = Contract(strategy_address)
            print("Strategy: " + strategy_address)
            print(txHash)
            harvestLogs = GetHarvestEventsV1(txHash)
            print(harvestLogs)
            if len(harvestLogs) > 0: # First, try to get harvest event(s)
                for log in harvestLogs:
                    print('log address: ' + log.address)
                    if log.address == strategy_address:
                        wantEarned = log['wantEarned']
                        performanceFeePercent, strategistRewardPercent, FEE_DENOMINATOR = V1_SetVariables(strategycontract,block_number)

                        performanceFee = wantEarned * performanceFeePercent / FEE_DENOMINATOR
                        strategistReward = wantEarned * strategistRewardPercent / FEE_DENOMINATOR
                        depositor_share = wantEarned - performanceFee - strategistReward

                        print('performance fee: ' + str(performanceFee))
                        print('strategist reward: ' + str(strategistReward))
                        print('depositor share: ' + str(depositor_share))

                        sqlInsertHarvestV1('event',block_number, transaction_index, strategy_dbid, wantEarned,strategistReward,performanceFee,depositor_share,vault_dbid,performanceFeePercent, strategistRewardPercent, FEE_DENOMINATOR)
                        print('used Harvest event data')
                    else:
                        pass
            elif strategy_address in eventlessStrategies(): # If you can't, reverse calculate from amount sent to Treasury
                try:
                    # This checks to see if vault tokens were burnt, if they were, this is a withdrawal not a harvest
                    checkIfBurningVaultToken = cursor.execute("""
                        SELECT token_dbid,
                            from_address_dbid,
                            to_address_dbid,
                            [value]
                        FROM eth.token_transfers
                        WHERE block_number = """ + str(block_number) + """
                            AND transaction_index = """ + str(transaction_index) + """
                            AND to_address_dbid = 2592
                            AND token_dbid = """ + str(vault_token_dbid)
                            ).fetchall()
                    # If we found txs, that means this tokens are burnt which means a user is a user withdrawing funds
                    if len(checkIfBurningVaultToken) > 0: 
                        pass #print('uhh') #Fill this later to capture withdrawal fees
                    # If we did not find any burnt vault tokens, this was not a withdrawal. Therefore, it must be a harvest
                    if len(checkIfBurningVaultToken) == 0:
                        howManyNonGovtFeeXfers = cursor.execute("""
                                SELECT token_dbid,
                                    from_address_dbid,
                                    to_address_dbid,
                                    [value]
                                FROM eth.token_transfers
                                WHERE block_number = """ + str(block_number) + """
                                    AND transaction_index = """ + str(transaction_index) + """
                                    AND token_dbid = """ + str(reserve_token_dbid) + """
                                    and from_address_dbid = """ + str(strategy_address_dbid) + """
                                    /* Ignore Treasury Fee, We already have that value */
                                    AND to_address_dbid != 17609756
                        """).fetchall()
                        print('do we need this?' + str(howManyNonGovtFeeXfers))
                        performanceFeePercent, strategistRewardPercent, FEE_DENOMINATOR = V1_SetVariables(strategycontract,block_number)
                        
                        # This is the actual math from contract: #
                        # gov_recd_value = wantEarned * performanceFee / FEE_DENOMINATOR
                        print('strategy dbid: ' + str(strategy_dbid))
                        print('gov: ' + str(gov_recd_value))
                        
                        wantEarned = math.floor(gov_recd_value * FEE_DENOMINATOR / performanceFeePercent)
                        strategistReward = math.floor(wantEarned * strategistRewardPercent / FEE_DENOMINATOR)
                        depositor_share = wantEarned - gov_recd_value - strategistReward

                        print(txHash)
                        print('wantEarned: ' + str(wantEarned))
                        print('strategist: ' + str(strategistReward))
                        print('depositor: ' + str(depositor_share))

                        sqlInsertHarvestV1('revCalc',block_number, transaction_index, strategy_dbid, wantEarned,strategistReward,gov_recd_value,depositor_share,vault_dbid,performanceFeePercent, strategistRewardPercent, FEE_DENOMINATOR)
                        print('used reverse-calculation')
                except Exception as e:
                    print(strategy_address)
                    print(str(e))
            else: # What's going on here?
                print("What's going on here?")
                performanceFee = None
        if version == "V2": 
            print('vault_dbid: ' + str(vault_dbid))
            logs = GetHarvestEventsV2(txHash)
            print(logs)
            for log in logs:
                print('strategy_address: ' + strategy_address)
                print('log.address: ' + log.address)
                if log.address == strategy_address:
                    print(log.address)
                    profit = log['profit']
                    print("Profit: " + str(profit))
                    loss = log['loss']
                    print("Loss: " + str(loss))
                    debtPayment = log['debtPayment']
                    print("DebtPayment: " + str(debtPayment))
                    debtOutstanding = log['debtOutstanding']
                    print("DebtOutstanding: " + str(debtOutstanding))

                    query = ("""
                        MERGE sucks.vault_strategies a using (select address_dbid from eth.addresses where address = '""" + log.address + """') b on a.strategy_address_dbid = b.address_dbid
                        WHEN NOT MATCHED THEN insert (strategy_address_dbid) values (b.address_dbid);
                    """)
                    print(query)
                    cursor.execute(query)
                    conn.commit()

                    strategy_dbid = cursor.execute("""
                        SELECT strategy_dbid 
                        FROM eth.addresses a left join sucks.vault_strategies b on a.address_dbid = b.strategy_address_dbid
                        where address = '""" + log.address + """'
                    """).fetchone()[0]
                    
                    #print(str(strategy_dbid))
                    gain = profit
                    vaultcontract = Contract(convert.to_address(HarvestEvent[13]))
                    timestamp = chain[block_number].timestamp
                    totalDebt = vaultcontract.totalDebt(block_identifier=block_number-1)
                    lastReport = vaultcontract.lastReport(block_identifier=block_number-1)
                    managementFeePercent = vaultcontract.managementFee(block_identifier=block_number)
                    performanceFeePercent = vaultcontract.strategies(strategy_address, block_identifier=block_number)[0]

                    strategist_fee = 0
                    governance_fee = totalDebt * (timestamp - lastReport) * managementFeePercent / 10000 / 31556952
                    
                    if gain > 0:
                        strategist_fee = gain * performanceFeePercent / 10000
                        governance_fee += (gain * performanceFeePercent / 10000)

                    print(strategist_fee)
                    print(governance_fee)

                    total_fee = governance_fee + strategist_fee
                    depositor_share = gain - total_fee 
                    
                    if gain == 0:
                        try:
                            cursor.execute("""
                            Insert into sucks.vault_harvests(block_number,transaction_index,vault_dbid,strategy_dbid,profit,loss,debtPayment,debtOutstanding,strategist_fee,governance_fee,depositor_share,version,strategist_fee_percent,governance_fee_percent,depositor_percent)
                            VALUES (""" + str(block_number) + """,""" + str(transaction_index) + """,""" + str(vault_dbid) + """,""" + str(strategy_dbid) + """,""" + str(profit) + """,""" + str(loss) + """,""" + str(debtPayment) + """,""" + str(debtOutstanding) + """,""" + str(strategist_fee) + """,""" + str(governance_fee) + """,""" + str(depositor_share) + """,2,""" + str(-1) + """,""" + str(-1) + """,""" + str(-1) + """)
                            """)
                        except Exception as e:
                            print("FAILED:::" + str(e))
                    else:
                        try:
                            print("managementFeePercent: " + str(managementFeePercent))
                            print("performanceFeePercent: " + str(performanceFeePercent))
                            print("governance fee: " + str(governance_fee/profit*10000))
                            print("strategist fee: " + str(strategist_fee/profit*10000))
                            print("depositor share: " + str(depositor_share/profit*10000))
                            cursor.execute("""
                            Insert into sucks.vault_harvests(block_number,transaction_index,vault_dbid,strategy_dbid,profit,loss,debtPayment,debtOutstanding,strategist_fee,governance_fee,depositor_share,version,strategist_fee_percent,governance_fee_percent,depositor_percent)
                            VALUES (""" + str(block_number) + """,""" + str(transaction_index) + """,""" + str(vault_dbid) + """,""" + str(strategy_dbid) + """,""" + str(profit) + """,""" + str(loss) + """,""" + str(debtPayment) + """,""" + str(debtOutstanding) + """,""" + str(strategist_fee) + """,""" + str(governance_fee) + """,""" + str(depositor_share) + """,2,""" + str(strategist_fee/profit*10000) + """,""" + str(governance_fee/profit*10000) + """,""" + str(depositor_share/profit*10000) + """)
                            """)
                            conn.commit()
                        except Exception as e:
                            print("FAILED:::" + str(e))
                    conn.commit()

    execStoredProcNoInput('yfi.ManualModificationsToHarvestReport')
    print("Done mothafucka!")

    unresolved = sqlGetHarvestEventList()
    lenUnresolved = len(unresolved)
    print(str(lenUnresolved) + " left over")
    for HarvestEvent in worklist:
        txHash = HarvestEvent[10]
        print(txHash)
            
    conn.commit()


