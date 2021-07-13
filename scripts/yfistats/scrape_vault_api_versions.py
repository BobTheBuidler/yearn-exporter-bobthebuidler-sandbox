from ...sql.mssqlserver.utils import conn, cursor
from brownie import Contract
from tqdm import tqdm
import logging

def main():
    vaults = cursor.execute("""
        select vault_dbid, address
        from sucks.vaults a
            left join eth.addresses b on a.vault_address_dbid = b.address_dbid
        where apiversion is null
        """).fetchall()
    for vault in tqdm(vaults):
        try:
            apiversion = Contract(vault[1]).apiVersion()
            cursor.execute(f"""
                update sucks.vaults set apiversion = '{apiversion}' where vault_dbid = {vault[0]}
            """)
            conn.commit()
        except AttributeError:
            cursor.execute(f"""
                update sucks.vaults set apiversion = 'None' where vault_dbid = {vault[0]}
            """)
            conn.commit()
        except ValueError as e:
            logging.critical(f"{str(e)} for vault {vault[1]}")