from brownie import Contract
from pprint import pprint

def main():
    from ...sql.mssqlserver.utils import conn, cursor

    provider = Contract("0x0000000022D53366457F9d5E68Ec105046FC4383")
    registry = Contract(provider.get_registry())

    pools = []
    ct = registry.pool_count()
    print(str(ct) + " Pools")
    for ix in range(ct):
        pool = registry.pool_list(ix)
        token = registry.get_lp_token(pool)
        query = cursor.execute(f"""
            SELECT * from yfi.curveContracts where tokenContract = '{token}'
        """)
        response = query.fetchall()
        if response == []:
            tokenContract = Contract(token)
            name = tokenContract.name()
            abi_dbid = cursor.execute(f"""
                SELECT contract_abi_dbid from eth.contracts_join where address = '{pool}'
            """).fetchone()[0]
            pools.append((pool,token,name,abi_dbid))

    ct = len(pools)
    print(str(ct) + " Pools")
    pprint(pools)
    for item in pools:
        pool = item[0]
        token = item[1]
        name = item[2]
        abi_dbid = item[3]

        cursor.execute(f"""
            INSERT INTO yfi.curveContracts (contractAddress, startBlock, [name-shorthand-donotuse], abi, tokenContract, platform)
            VALUES ('{pool}', 0, '{name}', 'CurvePoolABI1','{token}', 'Curve')
        """)
        conn.commit()
