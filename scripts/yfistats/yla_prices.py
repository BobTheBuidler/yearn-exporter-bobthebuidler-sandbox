import os
import collections
from datetime import datetime
from ...sql.mssqlserver.utils import (conn, cursor, execStoredProcNoInput)
from yearn.prices.magic import get_price
from tqdm import tqdm

def sqlGetAndSetBlockList(token_dbid):
    cursor = conn.cursor()
    # Get List of Blocks we need data for
    shortenedblocklist = collections.deque(maxlen=10000)
    response = cursor.execute("""
            DECLARE @token_dbid BIGINT= """ + str(token_dbid) + """;

            with TokenDbids as(
                SELECT @token_dbid AS LPTokenDbid
                UNION
                SELECT a.LPTokenDbid
                FROM pricedata.lptokencomposition AS a
                WHERE reservetokendbid = @token_dbid
                    AND a.LPTokenDbid IS NOT NULL
                UNION
                SELECT a.LPTokenDbid
                FROM pricedata.lptokencomposition AS a
                LEFT JOIN pricedata.lptokencomposition AS b ON a.reservetokendbid = b.lptokendbid
                WHERE b.reservetokendbid = @token_dbid
                    AND a.LPTokenDbid IS NOT NULL
                    AND b.LPTokenDbid IS NOT NULL
                UNION
                SELECT a.LPTokenDbid
                FROM pricedata.lptokencomposition AS a
                LEFT JOIN pricedata.lptokencomposition AS b ON a.reservetokendbid = b.lptokendbid
                LEFT JOIN pricedata.lptokencomposition AS c ON b.reservetokendbid = c.lptokendbid
                WHERE c.reservetokendbid = @token_dbid
                    AND a.LPTokenDbid IS NOT NULL
                    AND b.LPTokenDbid IS NOT NULL
                    AND c.LPTokenDbid IS NOT NULL
                UNION
                SELECT a.LPTokenDbid
                FROM pricedata.lptokencomposition AS a
                LEFT JOIN pricedata.lptokencomposition AS b ON a.reservetokendbid = b.lptokendbid
                LEFT JOIN pricedata.lptokencomposition AS c ON b.reservetokendbid = c.lptokendbid
                LEFT JOIN pricedata.lptokencomposition AS d ON c.reservetokendbid = d.lptokendbid
                WHERE d.reservetokendbid = @token_dbid
                    AND a.LPTokenDbid IS NOT NULL
                    AND b.LPTokenDbid IS NOT NULL
                    AND c.LPTokenDbid IS NOT NULL
                    AND d.LPTokenDbid IS NOT NULL
            )

            SELECT distinct a.blockheight
            FROM
            (
                SELECT blockHeight
                FROM
                (
                SELECT DISTINCT
                        c.BlockHeight
                FROM --eth.token_transfers AS a
                z_ethTools.TrackedTxsUnion a
                    LEFT JOIN eth.blockmetrics AS b ON a.blockHeight = b.blockheight
                    LEFT JOIN eth.blockMetrics AS c ON b.timestamp = c.timestamp
                    INNER JOIN TokenDbids AS d ON a.token_dbid = d.lptokendbid
                ) AS a
                UNION
                SELECT blockheight
                FROM
                (
                SELECT MAX(timestamp) AS timestamp
                FROM eth.blockmetrics
                WHERE timestamp >=
                (
                    SELECT timestamp
                    FROM eth.blockMetrics
                    WHERE BlockHeight =
                    (
                        SELECT MIN(block_number)
                        FROM eth.token_transfers
                        WHERE token_dbid = @token_dbid
                    )
                )
                and date < (select max(date) from eth.blockmetrics)
                GROUP BY date
                ) AS a
                INNER JOIN eth.blockmetrics AS b ON a.timestamp = b.timestamp
            ) AS a
            LEFT JOIN eth.blockMetrics b on a.blockHeight = b.blockHeight
            LEFT JOIN pricedata.historicalPricesInUSD c on b.timestamp = c.timestamp and c.coin = 'YLA'
            WHERE c.price is null"""
    )
    numset = set()
    for row in response:
        numset.add(row[0])
    for blk in numset:
        shortenedblocklist.append(blk)
    shortenedblocklist = list(shortenedblocklist)
    print("numset " + str(len(numset)))
    print("shortened block list " + str(len(shortenedblocklist)))
    print(datetime.now())
    return shortenedblocklist

def main():
    blocks = sorted(sqlGetAndSetBlockList(135217))
    print(blocks)
    for block in tqdm(blocks):
        price = get_price('0x9ba60ba98413a60db4c651d4afe5c937bbd8044b',block)
        print('block: ' + str(block))
        print('price: ' + str(price))
        print('--------------------')
        cursor.execute("""
        drop table if exists #temp
            declare @block bigint = """ + str(block) + """
            declare @timestamp smalldatetime = (select timestamp from eth.blockMetrics where blockHeight = @block)
            select 'YLA' as coin, @timestamp as timestamp, """ + str(price) + """ as price
            into #temp

            merge pricedata.historicalpricesinusd a using #temp b on a.coin = b.coin and a.timestamp = b.timestamp
            when not matched then
                insert (coin, timestamp, price) values (b.coin, b.timestamp, b.price)
            when matched then
                update set a.price = b.price
            ;
        """)
        conn.commit()
