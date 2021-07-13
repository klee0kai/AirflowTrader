import asyncio
import glob
from cmath import nan

import numpy as np
import pandas as pd

import tel_bot.telegram_bot
from moex import *

IS_AIRFLOW = False


# Задачи:
#  избавиться от NaN значений - добавляется столбец dummy - отображающий, что данные не настоящие (подсовываются усредненные)
#  добавить промежуточные данные в выходные и праздники (также на основе средних значений), удалить дубли по датам, отмечаются в столбце dummy
#  фильтруем по Режиму TQBR -  Т+: Акции и ДР - безадрес.
# todo обратить внимание, что некоторые акции сначала торговались через EQNE


async def transformHistAsync(sec):
    df_origin = loadDataFrame(f"{HIST_MOEX_PATH}/stock_shares_{sec}")
    dfOut = df_origin.copy()
    dfOut = dfOut.loc[dfOut['boardid'] == 'TQBR']
    dfOut = dfOut.loc[dfOut['secid'] == sec]  # фильтруем, если вдруг пришли не те данные
    dfOut['dummy'] = False

    # df.isnull().sum() - кол-во нулов
    st_date = datetime.strptime(dfOut['tradedate'].values[0], '%Y-%m-%d')
    l_date = datetime.strptime(dfOut['tradedate'].values[-1], '%Y-%m-%d')
    dfOut = dfOut.loc[~dfOut.duplicated('tradedate')]
    dfOut = dfOut.merge(how='right', right=pd.DataFrame(
        data={
            'tradedate': [datetime.strftime(st_date + timedelta(days=i), '%Y-%m-%d') for i in range((l_date - st_date).days + 1)],
            'boardid': dfOut['boardid'].values[0],  # применяется фильтр по boardid == TQBR
            'shortname': dfOut['shortname'].values[0],
            'secid': dfOut['secid'].values[0],
        }))

    dfOut = dfOut.sort_values(by=['tradedate', 'secid'])
    dfOut = dfOut.reset_index(drop=True)
    dfOut['dummy'] = dfOut.isnull().sum(1) > 0
    dfOut['dummycount'] = dfOut.isnull().sum(1)
    dfOut[['open', 'close', 'low', 'high']] = dfOut[['open', 'close', 'low', 'high']].interpolate()
    dfOut.fillna(method='bfill', inplace=True)

    dummyCheckdf = dfOut.tail(500)
    dummyCount = len(dummyCheckdf.loc[dummyCheckdf['dummycount'] >= 4])
    allLen = len(dummyCheckdf)
    dummyRatio = float(dummyCount) / float(allLen)
    if dummyRatio > 0.5:
        print(f"Некорректная история {sec}. Акциями никто не торгует.")
        rmDataFrame(f"{HIST_MOEX_PATH}/stock_shares_{sec}")  # удаляем историю
        rmDataFrame(f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_{sec}")  # удаляем нормализацию
    else:
        # df_check = dfOut.tail(300)[['tradedate', 'open', 'close', 'low', 'high']]
        saveDataFrame(dfOut, f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_{sec}")


def transfromHist1():
    os.makedirs(HIST_TRANSFORM1_MOEX_PATH, exist_ok=True)

    for f in glob.glob(f"{HIST_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_MOEX_PATH}/stock_shares_"):-len(".csv")]
        asyncio.run(transformHistAsync(sec))

    chmodForAll(MOEX_PATH, 0x777, 0o666)


if __name__ == "__main__":
    transfromHist1()
