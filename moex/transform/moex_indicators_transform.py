import asyncio
import glob

import pandas as pd

from moex import *

IS_AIRFLOW = False


# Задачи - добавить индикаторы
#  macd - Moving Average Convergence/Divergence
#  sma5 ,sma10 , sma15 - простые скользящие средние
#  wma - взвешенные скользящие средние
#  ema9 , ema12, ema26 - экспонециальные скользящие средние


async def loadIndicatorsAsync(sec):
    df = loadDataFrame(f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_{sec}")

    df['move'] = df['close'] - df['open']
    df['diff'] = df['high'] - df['low']
    df['topshadow'] = df['high'] - df[['open', 'close']].max(1)  # верхняя тень свечи =  high - max(open, close)
    df['bottomshadow'] = df[['close', 'open']].min(1) - df['low']  # нижняя тень свечи = min(сlose, open) - low

    # todo candlecode кодирование свечи

    # sma - простая скользящая среднияя (simple Moving Average)
    df['sma5'] = df['close'].rolling(window=5).mean()
    df['sma10'] = df['close'].rolling(window=10).mean()
    df['sma15'] = df['close'].rolling(window=15).mean()
    df['sma30'] = df['close'].rolling(window=30).mean()
    df['sma60'] = df['close'].rolling(window=60).mean()
    df['sma120'] = df['close'].rolling(window=120).mean()
    df['sma240'] = df['close'].rolling(window=240).mean()
    df['sma480'] = df['close'].rolling(window=480).mean()

    if not IS_AIRFLOW:
        sma_df = df[['close', 'sma5', 'sma10', 'sma30', 'sma120', 'sma480']]
        fig = sma_df.plot(figsize=(100, 10))
        plt.show()


    def wma(x_df):
        x_df = x_df.reset_index(drop=True)
        len_df = len(x_df)
        weights = pd.Series([abs(n - len_df) for n in range(len_df, 0, -1)])
        denominator = sum(weights)
        wma = sum(weights * x_df) / denominator
        return wma

    # wma - взвешенная скользящая средняя
    df['wma5'] = df['close'].rolling(window=5).apply(lambda x: wma(x))
    df['wma10'] = df['close'].rolling(window=10).apply(lambda x: wma(x))
    df['wma30'] = df['close'].rolling(window=30).apply(lambda x: wma(x))
    df['wma60'] = df['close'].rolling(window=60).apply(lambda x: wma(x))
    df['wma120'] = df['close'].rolling(window=120).apply(lambda x: wma(x))
    df['wma240'] = df['close'].rolling(window=240).apply(lambda x: wma(x))
    df['wma480'] = df['close'].rolling(window=480).apply(lambda x: wma(x))

    if not IS_AIRFLOW:
        wma_df = df[['close', 'wma5', 'wma10', 'wma30', 'wma60', 'wma120', 'wma240', 'wma480']]
        fig = wma_df.plot(figsize=(100, 10))
        plt.show()

    # ema - экспоненциальная скользящая среднияя (simple Moving Average)
    def ema(df, buffsize):
        a = 2. / (buffsize + 1)
        ema.oldEma = float(df) * a + (1 - a) * ema.oldEma
        return ema.oldEma

    ema.oldEma = 0
    df['ema9'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 9))
    ema.oldEma = 0
    df['ema12'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 12))
    ema.oldEma = 0
    df['ema26'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 26))
    ema.oldEma = 0
    df['ema52'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 52))

    if not IS_AIRFLOW:
        ema_df = df[['close', 'ema9', 'ema12', 'ema26', 'ema52']]
        fig = ema_df.plot(figsize=(100, 10))
        plt.show()

    # дисперсия
    df['var9'] = df['close'].rolling(window=9).var()
    df['var12'] = df['close'].rolling(window=12).var()
    df['var26'] = df['close'].rolling(window=26).var()
    df['var60'] = df['close'].rolling(window=60).var()

    # Среднеквадратичное (стандартное) отклонение
    df['std9'] = df['close'].rolling(window=9).std()
    df['std12'] = df['close'].rolling(window=12).std()
    df['std26'] = df['close'].rolling(window=26).std()
    df['std60'] = df['close'].rolling(window=60).std()

    if not IS_AIRFLOW:
        ema_df = df[['close', 'var9', 'var12', 'var26', 'var60', 'std9', 'std12', 'std26', 'std60']]
        fig = ema_df.plot(figsize=(100, 10))
        plt.show()

    # Оценка волатильности
    #  - стандартная формула
    #  - метод Германа - Класса
    #  - Метод Роджерса-Сатчела
    #  - Метод Янг-Жанга

    pass


def loadAllIndicators():
    os.makedirs(HIST_INDICATORS_MOEX_PATH, exist_ok=True)
    for f in glob.glob(f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_"):-len(".csv")]
        asyncio.run(loadIndicatorsAsync(sec))

    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    loadAllIndicators()
