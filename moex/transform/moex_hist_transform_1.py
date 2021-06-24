import asyncio
import glob
from cmath import nan

from moex import *

IS_AIRFLOW = False


# Задачи:
#  избавиться от NaN значений - добавляется столбец dummy - отображающий, что данные не настоящие (подсовываются усредненные)
#  добавить промежуточные данные в выходные и праздники (также на основе средних значений), отмечаются в столбце dummy


async def transformHistAsync(sec):
    df = loadDataFrame(f"{HIST_MOEX_PATH}/stock_shares_{sec}")
    dfOut = df.copy()
    dfOut.columns.values.put(-1, 'dummy')

    # df.isnull().sum() - кол-во нулов

    dfOut['dummy'] = df.isnull().sum(1) > 0
    dfOut['dummycount'] = df.isnull().sum(1)
    dfOut = dfOut.interpolate()
    dfOut.fillna(method='bfill', inplace=True)
    saveDataFrame(dfOut, f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_{sec}")


def transfromHist1():
    os.makedirs(HIST_TRANSFORM1_MOEX_PATH, exist_ok=True)
    for f in glob.glob(f"{HIST_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_MOEX_PATH}/stock_shares_"):-len(".csv")]
        asyncio.run(transformHistAsync(sec))

    chmodForAll(MOEX_PATH, 0x777, 0o666)


if __name__ == "__main__":
    transfromHist1()
