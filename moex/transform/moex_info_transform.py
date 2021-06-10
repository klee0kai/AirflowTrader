import asyncio

from moex import *

logging.basicConfig(level=logging.DEBUG)


async def sortSecurities():
    markets = [  # (engine , market )
        ('stock', 'shares'),
        ('currency', 'selt'),
        ('currency', 'otc'),
    ]

    dfSec = None
    for engine, market in markets:
        df = loadDataFrame(f"{COMMON_MOEX_PATH}/securities_{engine}_{market}")
        if not df is None:
            df.columns = df.columns.map(lambda x: x.lower())
        if dfSec is None:
            dfSec = df
        else:
            dfSec = dfSec.append(df)

    del df

    dfAggr = loadDataFrame(f"{COMMON_MOEX_PATH}/aggregates")
    dfAggr = dfAggr.loc[dfAggr['volume'] != 0]

    # look at https://iss.moex.com/iss/engines/stock/markets/shares/securities/columns.json
    dfSec = dfSec[
        ['secid', 'sectype', 'shortname', 'prevprice', 'lotsize', 'facevalue', 'secname', 'marketcode', 'instrid',
         'minstep', 'prevwaprice', 'faceunit', 'prevdate', 'issuesize', 'isin', 'latname', 'regnumber',
         'prevlegalcloseprice', 'prevadmittedquote', 'currencyid', 'listlevel', 'settledate']]
    dfAggr = dfAggr[['secid', 'engine', 'market_name', 'volume', 'value']]
    dfAggr.columns = dfAggr.columns.map(lambda x: 'aggregate' if x == 'value' else x)

    dfAll = dfSec.merge(dfAggr)
    dfAll = dfAll.sort_values(by='volume', ascending=False)
    dfAll = dfAll.drop_duplicates('secid')

    dfAll_stock_shares = dfAll.loc[dfAll['engine'] == 'stock'].loc[dfAll['market_name'] == 'shares']
    dfAll_stock_shares_common = dfAll_stock_shares.loc[dfAll_stock_shares['sectype'] == '1']

    dfAll_stock_shares_list1 = dfAll_stock_shares_common.loc[dfAll_stock_shares_common['listlevel'] == 1]
    dfAll_stock_shares_list2 = dfAll_stock_shares_common.loc[dfAll_stock_shares_common['listlevel'] == 2]
    dfAll_stock_shares_list3 = dfAll_stock_shares_common.loc[dfAll_stock_shares_common['listlevel'] == 3]

    saveDataFrame(dfAll_stock_shares, f"{TRANSFORM_MOEX_PATH}/stock_shares")
    saveDataFrame(dfAll_stock_shares_common, f"{TRANSFORM_MOEX_PATH}/stock_shares_common")
    saveDataFrame(dfAll_stock_shares_list1, f"{TRANSFORM_MOEX_PATH}/stock_shares_common_list1")
    saveDataFrame(dfAll_stock_shares_list2, f"{TRANSFORM_MOEX_PATH}/stock_shares_common_list2")
    saveDataFrame(dfAll_stock_shares_list3, f"{TRANSFORM_MOEX_PATH}/stock_shares_common_list3")
    print()


def transformMoexCommon():
    os.makedirs(TRANSFORM_MOEX_PATH, exist_ok=True)
    asyncio.run(sortSecurities())
    chmodForAll(MOEX_PATH, 0x777, 0o666)


if __name__ == "__main__":
    transformMoexCommon()
