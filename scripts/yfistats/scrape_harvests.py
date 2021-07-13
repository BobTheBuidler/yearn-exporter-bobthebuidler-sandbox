import math
import time
import logging
from tqdm import tqdm
from joblib.parallel import Parallel, delayed
from brownie import Contract, chain, convert
from brownie.network.event import EventLookupError
from pprint import pprint
import csv

# NOTE: For reconciliation
# NOTE: Value '0.01' here would mean reconciliations will pass if < 1% discrepancy
THRESHOLD = 0.002

def eventlessStrategies():
    def new(address):
        eventlessStrats.append(address)
    eventlessStrats = []
    new('0x07DB4B9b3951094B9E278D336aDf46a036295DE7')
    new('0x112570655b32A8c747845E0215ad139661e66E7F')
    new('0x134c08fAeE4F902999a616e31e0B7e42114aE320')
    new('0x2be5D998C95DE70D9A38b3d78e49751F10F9E88b')
    new('0x2De055fec2b826ed4A7478CeDDBefF82C1EdFA70')
    new('0x2EE856843bB65c244F527ad302d6d2853921727e')
    new('0x2F3236a341a1f64E16F5C3c5a020F5282a63921b')
    new('0x372039dD01953f731c8493fa11B2A10a9a93B4cF')
    new('0x382185F3ea9268E65Bb16f81de6b4e725134ED72')
    new('0x39AFF7827B9D0de80D86De295FE62F7818320b76')
    new('0x40BD98e3ccE4F34c087a73DD3d05558733549afB')
    new('0x4FEeaecED575239b46d70b50E13532ECB62e4ea8')
    new('0x594a198048501A304267E63B3bAd0f0638da7628')
    new('0x6D6c1AD13A5000148Aa087E7CbFb53D402c81341')
    new('0x787C771035bDE631391ced5C083db424A4A64bD8')
    new('0x8816B2Fb982281c36E6c535B9e56B7a4417e68cF')
    new('0x8C6698dC64f69231E3dC509CD7Ad72164D2389F7')
    new('0x8fcB1C3F68ef7abE7B25457F35e88658086dc1ad')
    new('0x932fc4fd0eEe66F22f1E23fBA74D7058391c0b15')
    new('0xa069E33994DcC24928D99f4BBEDa83AAeF00B5f3')
    new('0xA30d1D98C502378ad61Fe71BcDc3a808CF60b897')
    new('0xAa12d6c9d680EAfA48D8c1ECba3FCF1753940A12')
    new('0xb15Ee8e74dac2d77F9d1080B32B0F3562954aeE9')
    new('0xBE197E668D13746BB92E675dEa2868FF14dA0b73')
    new('0xc999fb87AcA383A63D804A575396F65A55aa5aC8')
    new('0xd643cf07344428770b84973e049A1c18B5d47edE')
    return eventlessStrats

def nonVerifiedContracts():
    nvc = []
    #nvc.append('0x153Fe8894a76f14bC8c8B02Dd81eFBB6d24E909f')
    #nvc.append('0xAa12d6c9d680EAfA48D8c1ECba3FCF1753940A12')
    nvc.append('0xbd5fDda17bC27bB90E37Df7A838b1bFC0dC997F5')
    #nvc.append('0xD1D5A4c0eA98971894772Dcd6D2f1dc71083C44E')
    nvclist = []
    for contract in nvc:
        nvclist.append('Source for ' + contract + ' has not been verified')

    return nvclist

def main():
    from ...sql.mssqlserver.utils import (conn, cursor, execStoredProcNoInput)

    def sqlGetHarvestEventList():
        results = cursor.execute("""
            SELECT * from yfi.[prepTxListForHarvestScraping]
            --where /* why are there any nulls? */ strategy_address is not null and block_number >= 12156757 and strategy_address not in ( '0x406813fF2143d178d1Ebccd2357C20A424208912','0x39AFF7827B9D0de80D86De295FE62F7818320b76')
            where strategy_address != '0x0000000000000000000000000000000000000000' 
            --and transactionHash in ('0x0b3d5353a67f08a80dc1082c8852b6bbc04ea44a82d9b5963e7cd7ab968b2746')
            --and vault_dbid not in (193)
            and vault_dbid = 151
            order by vault_dbid desc, transactionHash, strategy_abi, want_token_address desc 
            """).fetchall()
        vaults = {}
        return tqdm(results)

    def brownieDownloadContractObjects(brownieReceipt):
        nonVerified = nonVerifiedContracts()
        go = 0
        while go == 0:
            try:
                addresses = set([event.address for event in brownieReceipt.events])
                for address in addresses:
                    proceed = 0
                    while proceed == 0:
                        logging.info(address)
                        try:
                            newcachedcontract = Contract(address)
                            proceed = 1
                        except Exception as e:
                            try:
                                newcachedcontract = Contract.from_explorer(address)
                                proceed = 1
                            except ValueError:
                                proceed = 1
                            except Exception as e:
                                #print(str(e))
                                if str(e) in nonVerified:
                                    proceed = 1
                                else:
                                    time.sleep(60)
                go = 1
            except Exception as e:
                logging.info(str(e))
                time.sleep(5)             

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
                print(_txHash)
                events = brownieReceipt.events
                logs = events['Harvested']
                transfers = events['Transfer']
                transfers = [transfer for transfer in transfers if transfer.address == _vaultcontract.address]
                go = 1
            except AttributeError as e:
                logging.critical(f"could not pull events for {_txHash}, {str(e)}")
                logging.critical("why can't brownie pull events?")
                logs = []
                go = 1
            except Exception as e:
                if str(e) == "Event 'Harvested' did not fire.":
                    print(_txHash + ' Harvested event did not fire.')
                    logs = []
                    go = 1
                else:
                    print(_txHash)
                    print(str(e))
                    time.sleep(15)
                    
        return logs, transfers

    def nonHarvestFunctions():
        nhf = ['withdraw','withdrawAll','withdrawETH','withdrawAllETH','ZapOut','migrateAll']
        return nhf

    def GetFunctionName(_txHash):
        brownieReceipt = GetBrownieReceipt(_txHash)
        logging.info(brownieReceipt.fn_name)
        if brownieReceipt.fn_name in nonHarvestFunctions():
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
            brownieDownloadContractObjects(brownieReceipt)

    def DeleteBadTxs():
        badTxs = open("reports/badTxs.csv")
        rows = list(csv.reader(badTxs))
        badhashes = set([_hash[0] for _hash in rows])
        print(f"{len(badhashes)} more to investigate")
        for _hash in badhashes:
            print(_hash)
            cursor.execute(f"""
                declare @hash varchar(100)='{_hash}'
                declare @block int = (select blockheight from eth.transactions where transactionHash = @hash)
                declare @ix int = (select transaction_index from eth.transactions where transactionHash = @hash)

                delete from sucks.vault_harvests where block_number = @block and transaction_index = @ix
            """)
            conn.commit()
        details = {}
        apiversions = set([_row[2] for _row in rows])
        for _version in apiversions:
            details[_version] = {}
        dbids = set([(row[2],row[1]) for row in rows])
        for row in dbids:
            _version, _dbid = row[0], row[1]
            details[_version][_dbid] = {}
        for row in rows:
            _hash, _vault_dbid, _apiversion, _ratio = row
            try:
                details[_apiversion][_vault_dbid][_hash].append(_ratio)
                details[_apiversion][_vault_dbid][_hash].sort()
            except:
                details[_apiversion][_vault_dbid][_hash] = []
                details[_apiversion][_vault_dbid][_hash].append(_ratio)
            #except:
            #    details[_apiversion] = {}
            #    details[_apiversion] += _vault_dbid
        from pprint import pprint
        pprint(details)
        badTxs.close()
    
    DeleteBadTxs()
    badTxs = open("reports/badTxs.csv",'w')
    _hash = 'wut'
    _donect = -1
    _logct = 0
    _vault = 0
    _logs = None
    _vaultcontract = None
    _vaulttokentransfers = None

    def process(HarvestEvent):

        def processV1():
            
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
                                    try:
                                        performanceFeePercent = strategycontract.fee(block_identifier = block_number)
                                        proceed = 1
                                        go = 1
                                    except:
                                        try: # this is for yvLINK only
                                            performanceFeePercent = strategycontract.split(block_identifier = block_number)
                                            proceed = 1
                                            go = 1
                                        except:
                                            if str(e) == '429 Client Error: Too Many Requests for url: https://api.archivenode.io/XXXXXXXXXXXXXXX':
                                                print('slow down!!')
                                                time.sleep(30)
                                            else:
                                                print(str(e))
                                                print(strategycontract)
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
                                        """Contract 'StrategyCreamYFI' object has no attribute 'strategistReward'""",
                                        """Contract 'StrategyYfii' object has no attribute 'strategistReward'""",
                                        """Contract 'StrategyYffi' object has no attribute 'strategistReward'""",
                                        """Contract 'StrategyControllerV2' object has no attribute 'strategistReward'"""]:
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
                            try:
                                FEE_DENOMINATOR = strategycontract.DENOMINATOR(block_identifier = block_number)
                            except:
                                FEE_DENOMINATOR = strategycontract.max(block_identifier = block_number)
                return FEE_DENOMINATOR

            def V1_SetVariables(strategycontract, block_number):
                performanceFeePercent = V1_GetPerformanceFeePercent(strategycontract,block_number)
                strategistRewardPercent = V1_GetStrategistRewardPercent(strategycontract,block_number)
                FEE_DENOMINATOR = V1_GetFeeDenominator(strategycontract,block_number)
                return performanceFeePercent, strategistRewardPercent, FEE_DENOMINATOR

            def GetHarvestEventsV1(_txHash):
                brownieReceipt = GetBrownieReceipt(_txHash)
                logs = GetHarvestLogs(_txHash)
                if brownieReceipt.fn_name in ['withdraw','withdrawAll','withdrawETH','withdrawAllETH','ZapOut','migrateAll','earlyWithdrawal']:
                    cursor.execute("""
                    insert into yfi.ignoreNotHarvests
                    select blockheight,transaction_index
                    from eth.transactions
                    where transactionhash = '""" + _txHash + """'  
                    """)
                    conn.commit()
                elif brownieReceipt.fn_name in ['harvest','workForTokens','forceWork','work','forceHarvest','delegatedHarvest']:
                    pass
                else:
                    print(brownieReceipt.fn_name)
                    #print(brownieReceipt.info())
                    brownieDownloadContractObjects(brownieReceipt)
                    logs = GetHarvestLogs(_txHash)
                        
                return logs

            def sqlInsertHarvestV1(calcMethod, block_number, transaction_index, strategy_dbid, wantEarned,strategistReward,performanceFee,depositor_share,vault_dbid,performanceFeePercent, strategistRewardPercent, FEE_DENOMINATOR):
                cursor.execute("""
                    Insert into sucks.vault_harvests(block_number,transaction_index,strategy_dbid,                              wantEarned,strategist_fee,                                      governance_fee,depositor_share,                                 vault_dbid,version,             governance_fee_percent,             strategist_fee_percent,             depositor_percent, calcMethod)
                    VALUES (""" + str(block_number) + """,""" + str(transaction_index) + """,""" + str(strategy_dbid) + """,""" + str(wantEarned) + """,""" + str(strategistReward) + """,""" + str(performanceFee) + """,""" + str(depositor_share) + """,""" + str(vault_dbid) + """,1,""" + str(performanceFeePercent) + """,""" + str(strategistRewardPercent) + """,""" + str(round(depositor_share/wantEarned*FEE_DENOMINATOR,20)) + """,'""" + calcMethod + """')
                    """)
                conn.commit()

            logging.info(version)
            strategycontract = Contract(strategy_address)
            logging.info("Strategy: " + strategy_address)
            
            harvestLogs = GetHarvestEventsV1(txHash)
            logging.info(harvestLogs)
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
            elif strategy_address in eventlessStrategies(): #and GetFunctionName(txHash) not in nonHarvestFunctions(): # If you can't, reverse calculate from amount sent to Treasury
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
                    print(vault_token_dbid)
                    print(checkIfBurningVaultToken)
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

        def processV2():
            
            def special_handling():
                if txHash == '0xec36298301fd794370e41baf1d47798c01e59048d4e044efdb1948edce0335a3': #We aren't able to calculate because we need totalDebt mid-block
                    nonlocal governance_fee
                    governance_fee = 173040317209291237 / vaultcontract.pricePerShare(block_identifier = block_number) * 10 ** vaultcontract.decimals(block_identifier = block_number)

            def GetHarvestEventsV2(_txHash):
                brownieReceipt = GetBrownieReceipt(_txHash)
                if brownieReceipt.fn_name in ['none yet, prob none ever']:
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
                logs, transfers = GetHarvestLogs(_txHash)

                return logs, transfers

            def GetCredit():
                vault_totalAssets = vaultcontract.totalAssets(block_identifier = block_number - 1)
                vault_debtLimit = vaultcontract.debtRatio(block_identifier = block_number - 1) * vault_totalAssets / 10000
                vault_totalDebt = vaultcontract.totalDebt(block_identifier = block_number - 1)
                strategy_debtLimit = vaultcontract.strategies(strategy_address,block_identifier = block_number - 1)['debtRatio'] * vault_totalAssets / 10000
                strategy_totalDebt = vaultcontract.strategies(strategy_address,block_identifier = block_number - 1)['totalDebt']
                strategy_minDebtPerHarvest = vaultcontract.strategies(strategy_address,block_identifier = block_number - 1)['minDebtPerHarvest']
                strategy_maxDebtPerHarvest = vaultcontract.strategies(strategy_address,block_identifier = block_number - 1)['maxDebtPerHarvest']

                if strategy_debtLimit <= strategy_totalDebt or vault_debtLimit <= vault_totalDebt:
                    return 0

                available = strategy_debtLimit - strategy_totalDebt
                available = min(available, vault_debtLimit, vault_totalDebt)
                available = min(available, Contract(vaultcontract.token(block_identifier=block_number-1)).balanceOf(vaultcontract, block_identifier = block_number - 1))

                if available < strategy_minDebtPerHarvest:
                    return 0
                else:
                    return min(available, strategy_maxDebtPerHarvest)
                
            def GetTotalDebt(vaultcontract, strategy_address, apiversion, block): 
                if apiversion in ['0.2.2','0.3.0']:
                    totalDebt = vaultcontract.totalAssets(block_identifier = block)
                if apiversion in ['0.3.1','0.3.2','0.3.3','0.3.4']:
                    totalDebt = vaultcontract.totalDebt(block_identifier = block) - loss
                if apiversion in ['0.3.5','0.4.0','0.4.1','0.4.2']:
                    totalDebt = vaultcontract.strategies(strategy_address, block_identifier = block)['totalDebt']
                    if totalDebt == 0:
                        try:
                            if len(GetBrownieReceipt(txHash).events['StrategyMigrated']) == 1: # NOTE: This happens if a strategy is migrated during the same call as the harvest
                                logging.info('total debt is 0 and strategy was migrated, checking again...')
                                totalDebt = vaultcontract.strategies(strategy_address, block_identifier = block + 1)['totalDebt']
                                logging.info(f"next block total debt: {totalDebt}")
                                totalDebt += debtPayment
                        except EventLookupError: 
                            pass
                print('total debt:   ' + str(totalDebt))
                return totalDebt

            def GetLastReport(vaultcontract, strategy_address, apiversion, block):
                if apiversion in ['0.2.2','0.3.0','0.3.1','0.3.2','0.3.3','0.3.4']:
                    lastReport = vaultcontract.lastReport(block_identifier = block)
                if apiversion in ['0.3.5','0.4.0','0.4.1','0.4.2']:
                    logging.info(vaultcontract)
                    lastReport = vaultcontract.strategies(strategy_address, block_identifier = block)['lastReport']
                    if lastReport == 0: # NOTE: This only happens during strategy migration
                        logging.info('lastReport is 0, checking again...')
                        lastReport = vaultcontract.strategies(isMigrationFrom(), block_identifier = block)['lastReport']
                logging.info(f"block number: {block_number}")
                logging.info('lastReport:   ' + str(lastReport))
                logging.info('timestamp:    ' + str(timestamp))
                return lastReport

            def GetGovFee(block):
                if apiversion in ['0.2.2','0.3.0','0.3.1','0.3.2','0.3.3']:
                    governance_fee = totalDebt * (timestamp - lastReport) * managementFeePercent / 10000 / 31556952
                if apiversion in ['0.3.2']:
                    governance_fee = (totalDebt - loss) * (timestamp - lastReport) * managementFeePercent / 10000 / 31556952
                    logging.info(f"delegatedAssets() does not exist for this api version")
                if apiversion == '0.3.4':
                    delegatedAssets = vaultcontract.delegatedAssets(block_identifier = block)
                    logging.info(f"delegated assets: {delegatedAssets}")
                    governance_fee = (totalDebt - delegatedAssets) * (timestamp - lastReport) * managementFeePercent / 10000 / 31556952
                if apiversion in ['0.3.5','0.4.2']:
                    delegatedAssets = Contract(strategy_address).delegatedAssets(block_identifier = block)
                    logging.info(f"delegated assets: {delegatedAssets}")
                    governance_fee = (totalDebt - delegatedAssets) * (timestamp - lastReport) * managementFeePercent / 10000 / 31556952
                return governance_fee

            def isMigrationFrom():
                nonlocal txHash
                nonlocal vaultcontract
                migrations = [event for event in GetBrownieReceipt(txHash).events['StrategyMigrated'] if event.address == vaultcontract and event['newVersion'] == strategycontract]
                if len(migrations) == 1:
                    migration = migrations[0]
                return migration['oldVersion']

            def isMigrationTo():
                nonlocal txHash
                nonlocal vaultcontract
                migration = [event for event in GetBrownieReceipt(txHash).events['StrategyMigrated'] if event.address == vaultcontract and event['oldVersion'] == strategycontract]
                return migration['newVersion']

            def GetValueFromTransferEvent(transfer):
                print(transfer)
                if len(transfer) == 0:
                    return 0
                elif len(transfer) == 1:
                    return transfer[0]['value']
                elif len(transfer) > 1:
                    logging.critical('Unexpected behavior, figure this out')
                    return None

            def Reconcile(calculatedFromHarvest, calculatedFromTransfers):
                adjusted_from_transfers = calculatedFromTransfers * vaultcontract.pricePerShare(block_identifier = block_number-1) / 10 ** vaultcontract.decimals(block_identifier = block_number-1)
                if calculatedFromHarvest == 0 and adjusted_from_transfers == 0:
                    return 1, adjusted_from_transfers
                elif adjusted_from_transfers != 0:
                    return calculatedFromHarvest/adjusted_from_transfers, adjusted_from_transfers
                else:
                    return None, adjusted_from_transfers

            nonlocal _logs, _logct, _vaultcontract, _vaulttokentransfers
            if _logs == None:
                _vaultcontract = Contract(convert.to_address(HarvestEvent[13]))
                _logs, _vaulttokentransfers = GetHarvestEventsV2(txHash)
                _logct = len(_logs)
                logging.info('vault_dbid: ' + str(vault_dbid))
                logging.info(txHash)
                logging.info(version)
                logging.info(_logs)
            logs = _logs
            vaultcontract = _vaultcontract
            for log in logs:
                if log.address == strategy_address:
                    logging.info(f"log.address: {log.address}    strategy_address: {strategy_address}")
                    apiversion = vaultcontract.apiVersion(block_identifier = block_number)
                    logging.info(f"api version: {apiversion}")
                    if apiversion in ['0.2.2','0.3.0','0.3.1','0.3.2','0.3.3','0.3.4','0.3.5','0.4.2']:
                        sameTxHarvestsForSameVault = [log.pos[0] for log in logs if Contract(log.address).vault(block_identifier = block_number) == vaultcontract]
                        sameTxHarvestsForSameStrat = [log.pos[0] for log in logs if log.address == strategy_address]
                        logging.info(F"{sameTxHarvestsForSameVault}  {str((log.pos[0] == min(sameTxHarvestsForSameVault)))}")
                        logging.info(f"{sameTxHarvestsForSameStrat}  {str(log.pos[0] == min(sameTxHarvestsForSameStrat))}")
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
                        cursor.execute(query)
                        conn.commit()

                        strategy_dbid = cursor.execute("""
                            SELECT strategy_dbid 
                            FROM eth.addresses a left join sucks.vault_strategies b on a.address_dbid = b.strategy_address_dbid
                            where address = '""" + log.address + """'
                        """).fetchone()[0]
                        logging.info('strategy_dbid: ' + str(strategy_dbid))

                        gain = profit
                        
                        timestamp = chain[block_number].timestamp

                        managementFeePercent = vaultcontract.managementFee(block_identifier=block_number)
                        performanceFeePercent = vaultcontract.strategies(strategy_address, block_identifier=block_number)[0]
                        strategistRewardPercent = performanceFeePercent
                        if apiversion in ['0.2.0','0.2.1','0.2.2','0.3.0','0.3.1','0.3.2','0.3.3','0.3.4','0.3.5','0.4.0','0.4.1','0.4.2']: # NOTE: In newer vault versions, perf fee pct doesn't always = strategist fee pct
                            performanceFeePercent = vaultcontract.performanceFee(block_identifier=block_number)
                        
                        strategycontract = Contract(strategy_address)
                        if gain == 0 and apiversion in ['0.4.0','0.4.1','0.4.2']:
                            csv.writer(badTxs).writerow((txHash, vault_dbid, apiversion, None))
                            return
                        
                        if log.pos[0] == min(sameTxHarvestsForSameVault) or (log.pos[0] == min(sameTxHarvestsForSameStrat) and apiversion in ['0.3.2','0.3.5','0.4.0','0.4.1','0.4.2']):
                            lastReport = GetLastReport(vaultcontract, strategy_address, apiversion, block_number-1)
                            credit = GetCredit()
                            logging.info(f"credit: {credit}")
                            if debtPayment == 0 and credit == 0 and loss == 0:
                                totalDebt = GetTotalDebt(vaultcontract, strategy_address, apiversion, block_number)
                            elif debtPayment == 0 and loss != 0: #and credit == 0 
                                totalDebt = GetTotalDebt(vaultcontract, strategy_address, apiversion, block_number-1) - loss
                            else:
                                totalDebt = GetTotalDebt(vaultcontract, strategy_address, apiversion, block_number-1)
                            governance_fee = GetGovFee(block_number-1)
                        else: #These will be close, but not fully accurate
                            logging.info("(else:) is running")
                            lastReport = GetLastReport(vaultcontract, strategy_address, apiversion, block_number)
                            totalDebt = GetTotalDebt(vaultcontract, strategy_address, apiversion, block_number)
                            governance_fee = GetGovFee(block_number)
                        
                        strategist_fee = 0
                        
                        if managementFeePercent == 0:
                            logging.warning('Management fee percent is zero? ' + vault_token)

                        print('mgmt fee pct: ' + str(managementFeePercent))
                        print('mgmt fee:     ' + str(governance_fee))
                        print(f"perf fee pct: {performanceFeePercent}")
                        print(f"strat fee pct: {strategistRewardPercent}")

                        if gain > 0:
                            strategist_fee = gain * strategistRewardPercent / 10000
                            governance_fee += (gain * performanceFeePercent / 10000)

                        special_handling()

                        print('strategist: ' + str(strategist_fee))
                        print('total gov:  ' + str(governance_fee))

                        total_fee = governance_fee + strategist_fee
                        logging.info(f"total fee: {total_fee}")
                        if total_fee > gain and apiversion in ['0.3.5','0.4.0','0.4.1','0.4.2','0.4.3']:
                            total_fee = gain
                            logging.info(f"new total fee: {total_fee}")
                            if strategist_fee > 0:
                                governance_fee = total_fee - strategist_fee
                            else:
                                governance_fee = total_fee
                        
                        depositor_share = gain - total_fee
                        print(f"depositor: {depositor_share}")

                        token_mints = [(transfer.pos[0], transfer) for transfer in _vaulttokentransfers if transfer.pos[0] <= log.pos[0] and transfer[0]['sender'] == '0x0000000000000000000000000000000000000000']
                        
                        logging.info(f"log.pos: {log.pos[0]}")
                        logging.info(f"token mints: {token_mints}")
                        # We already filtered out mints with log_ix greater than harvest log_ix,
                        #   now we filter out mints from earlier harvests
                        try:
                            token_mint_pos = max([token_mint[0] for token_mint in token_mints])
                        except ValueError as e:
                            logging.critical(f"{str(e)}")
                            csv.writer(badTxs).writerow((txHash, vault_dbid, apiversion, None))
                            return
                        if len(token_mints) > 1:
                            token_mint = [token_mint[1] for token_mint in token_mints if token_mint[0] == token_mint_pos][0]
                        elif len(token_mints) == 1:
                            token_mint = token_mints[0][1]
                        else:
                            csv.writer(badTxs).writerow((txHash, vault_dbid, apiversion, None))
                            return
                        fee_transfers = [transfer for transfer in _vaulttokentransfers if transfer.pos[0] < log.pos[0] and transfer.pos[0] > token_mint_pos and transfer[0]['sender'] == vaultcontract.address]
                        governance_transfer = GetValueFromTransferEvent([transfer for transfer in fee_transfers if transfer[0]['receiver'] == vaultcontract.rewards(block_identifier = block_number)])
                        strategist_transfer = GetValueFromTransferEvent([transfer for transfer in fee_transfers if transfer[0]['receiver'] == strategy_address])
                        print(f"governance transfer amount: {governance_transfer}")
                        print(f"strategist transfer amount: {strategist_transfer}")
                        if governance_transfer == None or strategist_transfer == None:
                            csv.writer(badTxs).writerow((txHash, vault_dbid, apiversion, None))
                            return

                        ratio, adjusted_gov_xfer = Reconcile(governance_fee,governance_transfer)
                        if ratio < 1-THRESHOLD or ratio > 1+THRESHOLD or ratio == None:
                            logging.critical('Gov Fees do not reconcile, must investigate')
                            logging.critical(f"calced gov fee: {governance_fee}    adjusted transfer value: {adjusted_gov_xfer}    ratio: {ratio}")
                            csv.writer(badTxs).writerow((txHash, vault_dbid, apiversion, ratio))
                            return
                        ratio, adjusted_strat_xfer = Reconcile(strategist_fee, strategist_transfer)
                        if ratio < 1-THRESHOLD or ratio > 1+THRESHOLD or ratio == None:
                            logging.critical('Strat Fees do not reconcile, must investigate')
                            logging.critical(f"calced strat fee: {strategist_fee}    adjusted transfer value: {adjusted_strat_xfer}    ratio: {ratio}")
                            csv.writer(badTxs).writerow((txHash, vault_dbid, apiversion, ratio))
                            return

                        nonlocal _donect
                        if gain == 0:
                            try:
                                cursor.execute("""
                                Insert into sucks.vault_harvests(block_number,transaction_index,vault_dbid,strategy_dbid,profit,loss,debtPayment,debtOutstanding,strategist_fee,governance_fee,depositor_share,version,strategist_fee_percent,governance_fee_percent,depositor_percent,calcMethod)
                                VALUES (""" + str(block_number) + """,""" + str(transaction_index) + """,""" + str(vault_dbid) + """,""" + str(strategy_dbid) + """,""" + str(profit) + """,""" + str(loss) + """,""" + str(debtPayment) + """,""" + str(debtOutstanding) + """,""" + str(strategist_fee) + """,""" + str(governance_fee) + """,""" + str(depositor_share) + """,2,""" + str(-1) + """,""" + str(-1) + """,""" + str(-1) + """,'event')
                                """)
                                conn.commit()
                                if _donect == -1:
                                    _donect = 1
                                else:
                                    _donect += 1
                            except Exception as e:
                                logging.critical("FAILED:::" + str(e))
                                csv.writer(badTxs).writerow((txHash, vault_dbid, apiversion, ratio))
                        else:
                            try:
                                logging.info("managementFeePercent: " + str(managementFeePercent))
                                logging.info("performanceFeePercent: " + str(performanceFeePercent))
                                print("governance fee: " + str(governance_fee/profit*10000))
                                print("strategist fee: " + str(strategist_fee/profit*10000))
                                print("depositor share: " + str(depositor_share/profit*10000))
                                cursor.execute("""
                                Insert into sucks.vault_harvests(block_number,transaction_index,vault_dbid,strategy_dbid,profit,loss,debtPayment,debtOutstanding,strategist_fee,governance_fee,depositor_share,version,strategist_fee_percent,governance_fee_percent,depositor_percent,calcMethod)
                                VALUES (""" + str(block_number) + """,""" + str(transaction_index) + """,""" + str(vault_dbid) + """,""" + str(strategy_dbid) + """,""" + str(profit) + """,""" + str(loss) + """,""" + str(debtPayment) + """,""" + str(debtOutstanding) + """,""" + str(strategist_fee) + """,""" + str(governance_fee) + """,""" + str(depositor_share) + """,2,""" + str(strategist_fee/profit*10000) + """,""" + str(governance_fee/profit*10000) + """,""" + str(depositor_share/profit*10000) + """,'event')
                                """)
                                conn.commit()
                                conn.commit()
                                if _donect == -1:
                                    _donect = 1
                                else:
                                    _donect += 1
                            except Exception as e:
                                logging.critical("FAILED:::" + str(e))
                                csv.writer(badTxs).writerow((txHash, vault_dbid, apiversion, ratio))
                        conn.commit()
        
        

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
            
        nonlocal _hash, _donect, _vault, _logct, _logs, _vaultcontract, _vaulttokentransfers
        if _vault != vault_dbid or _hash != txHash:
            _vault = vault_dbid
            _hash = txHash
            _donect = -1
            _logct = 0 
            _logs, _vaulttokentransfers = None, None
            _vaultcontract = None

        if version == "V1":
            processV1()
        if version == "V2" and _donect < _logct: 
            processV2()
    
    logging.basicConfig(level=logging.INFO)
    Parallel(1,'threading')(delayed(process)(HarvestEvent) for HarvestEvent in sqlGetHarvestEventList())
    conn.commit()
    badTxs.close()
    DeleteBadTxs()


