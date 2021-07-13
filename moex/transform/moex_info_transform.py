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
        if dfSec is None:
            dfSec = df
        else:
            dfSec = dfSec.append(df)

    del df

    dfSecTQBR = dfSec.loc[dfSec['boardid'] == 'TQBR']
    dfSecTQBR = dfSecTQBR.sort_values(by=['issuesize'])
    dfSecTQBRBig = dfSecTQBR.loc[dfSecTQBR['issuesize'] > 200_000_000]
    dfSecUnicTQBR = dfSecTQBRBig.drop_duplicates('secid')
    dfAll_stock_shares_common = dfSecTQBRBig.loc[dfSecTQBR['sectype'] == '1']

    dfAll_stock_shares_list1 = dfAll_stock_shares_common.loc[dfAll_stock_shares_common['listlevel'] == 1]
    dfAll_stock_shares_list2 = dfAll_stock_shares_common.loc[dfAll_stock_shares_common['listlevel'] == 2]
    dfAll_stock_shares_list3 = dfAll_stock_shares_common.loc[dfAll_stock_shares_common['listlevel'] == 3]

    saveDataFrame(dfAll_stock_shares_common.drop_duplicates('secid'), f"{TRANSFORM_MOEX_PATH}/stock_shares_common")
    saveDataFrame(dfAll_stock_shares_list1.drop_duplicates('secid'), f"{TRANSFORM_MOEX_PATH}/stock_shares_common_list1")
    saveDataFrame(dfAll_stock_shares_list2.drop_duplicates('secid'), f"{TRANSFORM_MOEX_PATH}/stock_shares_common_list2")
    saveDataFrame(dfAll_stock_shares_list3.drop_duplicates('secid'), f"{TRANSFORM_MOEX_PATH}/stock_shares_common_list3")


def transformMoexCommon():
    os.makedirs(TRANSFORM_MOEX_PATH, exist_ok=True)
    asyncio.run(sortSecurities())
    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    transformMoexCommon()
