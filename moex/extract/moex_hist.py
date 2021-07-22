import asyncio
import os.path

import aiomoex
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

        if isMoexWorkTime():
            req_url = f"{MOEX_ISS_URL}/iss/engines/{engine}/markets/{market}/securities/{security}/candles.json?from={s_today}&interval=60"
            data = await aiomoex.ISSClient(session, req_url).get()
            candles_df = pd.DataFrame(data['candles'])
            candles_df.columns = candles_df.columns.map(lambda x: x.lower())
            if len(candles_df) > 0:
                ap_df = pd.DataFrame(data={
                    'secid': security,
                    'tradedate': s_today,
                    'boardid': 'TQBR',  ## по TQBR производится фильтр данных в moex_hist_transform_1.py
                    'open': [float(candles_df.head(1)['open'])],
                    'close': [float(candles_df.tail(1)['open'])],
                    'high': [candles_df['high'].max()],
                    'low': [candles_df['low'].min()],
                })

                if not dfAll is None:
                    dfAll = dfAll.loc[[not v in ap_df['tradedate'].values for v in dfAll['tradedate'].values]]
                    dfAll = dfAll.append(ap_df)
                else:
                    dfAll = ap_df

                dfAll = dfAll.sort_values(by=['tradedate', 'secid'])

        saveDataFrame(dfAll, fileName)


def extractHists():
    os.makedirs(HIST_MOEX_PATH, exist_ok=True)
    df = loadDataFrame(f'{TRANSFORM_MOEX_PATH}/stock_shares_common')

    for sec in df['secid']:
        asyncio.run(extractHistAsync('stock', 'shares', sec))

    # удалим истории которых нет в загрузке
    allSec = list(df['secid'])
    for f in glob.glob(f"{HIST_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_MOEX_PATH}/stock_shares_"):-len(".csv")]
        if not sec in allSec:
            rmDataFrame(f"{HIST_MOEX_PATH}/stock_shares_{sec}")

    chmodForAll(MOEX_PATH, 0x777, 0o666)


if __name__ == "__main__":
    asyncio.run(extractHistAsync('stock', 'shares', 'TRCN'))
    # extractHists()
    pass
