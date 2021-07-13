import asyncio
import os.path

import pandas as pd

from moex import *
import glob


async def extractHistAsync(engine, market, security):
    s_today = datetime.utcnow().strftime("%Y-%m-%d")
    fileName = f"{HIST_MOEX_PATH}/{engine}_{market}_{security}"
    dfAll = loadDataFrame(fileName)
    st_day = None
    if not dfAll is None:
        start_day = datetime.strptime(dfAll['tradedate'].values[-1], "%Y-%m-%d")
        st_day = datetime.strftime(start_day - timedelta(days=3), "%Y-%m-%d")
    async with AiohttpMoexClientSession() as session:
        iteration = 0
        while (st_day is None or st_day != s_today) and iteration < MAX_ITERATION:
            iteration += 1
            req_url = f"{MOEX_ISS_URL}/iss/history/engines/{engine}/markets/{market}/securities/{security}.json"
            if not st_day is None:
                req_url += f"?from={st_day}"
            data = await aiomoex.ISSClient(session, req_url).get()
            df = pd.DataFrame(data['history'])
            df.columns = df.columns.map(lambda x: x.lower())
            if not dfAll is None:
                dfAll = dfAll.loc[[not v in df['tradedate'].values for v in dfAll['tradedate'].values]]
                dfAll = dfAll.append(df)
            else:
                dfAll = df

            dfAll = dfAll.sort_values(by=['tradedate', 'secid'])
            st_day = dfAll['tradedate'].values[-1]
            if len(df) < 10:
                break

        saveDataFrame(dfAll, fileName)


def extractHists():
    os.makedirs(HIST_MOEX_PATH, exist_ok=True)
    df = loadDataFrame(f'{TRANSFORM_MOEX_PATH}/stock_shares_common')

    for sec in df['secid']:
        asyncio.run(extractHistAsync('stock', 'shares', sec))

    allSec = list(df['secid'])
    for f in glob.glob(f"{HIST_MOEX_PATH}/stock_shares_*.csv"):
        # удалим истории которых нет в загрузке
        sec = f[len(f"{HIST_MOEX_PATH}/stock_shares_"):-len(".csv")]
        if not sec in allSec:
            rmDataFrame(f"{HIST_MOEX_PATH}/stock_shares_{sec}")

    chmodForAll(MOEX_PATH, 0x777, 0o666)


if __name__ == "__main__":
    # todo нет таких акций как sber
    extractHists()
    pass
