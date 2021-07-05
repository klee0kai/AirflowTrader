import asyncio
import glob
import math

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
    already_out_df = loadDataFrame(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_{sec}")
    filled = 0
    if not already_out_df is None and len(df) >= len(already_out_df):
        cond = list(df.iloc[:len(already_out_df)]['close'] == already_out_df['close'])
        filled = cond.index(False) if False in cond else len(already_out_df)
    append_count = len(df) - filled
    if append_count < 0:
        raise Exception("append_count < 0")
    if append_count == 0:
        print(f"loadIndicatorsAsync {sec} already loaded")
        return

    print(f"loadIndicatorsAsync {sec} append update {append_count}")

    # максимальное скользящее окно 480
    df = df.tail(min(append_count + 600, len(df)))
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
    df['ema8'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 8))
    ema.oldEma = 0
    df['ema9'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 9))
    ema.oldEma = 0
    df['ema12'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 12))
    ema.oldEma = 0
    df['ema17'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 17))
    ema.oldEma = 0
    df['ema26'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 26))
    ema.oldEma = 0
    df['ema52'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 52))

    # macd индикатор
    # MACD = ЕМАs(P) − EMAl(P)
    # от быстрой ema вычитается медленная
    df['macd_12_26'] = df['ema12'] - df['ema26']
    ema.oldEma = 0
    df['macd_12_26_signal9'] = df['macd_12_26'].rolling(window=1).apply(lambda x: ema(x, 9))

    df['macd_8_17'] = df['ema8'] - df['ema17']
    ema.oldEma = 0
    df['macd_8_17_signal9'] = df['macd_8_17'].rolling(window=1).apply(lambda x: ema(x, 9))

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
    df['std360'] = df['close'].rolling(window=360).std()

    if not IS_AIRFLOW:
        ema_df = df[['close', 'var9', 'var12', 'var26', 'var60', 'std9', 'std12', 'std26', 'std60']]
        fig = ema_df.plot(figsize=(100, 10))
        plt.show()

    # Оценка волатильности
    #  - стандартная формула
    #  - метод Германа - Класса
    #  - Метод Роджерса-Сатчела
    #  - Метод Янг-Жанга
    # https://ru.wikipedia.org/wiki/%D0%92%D0%BE%D0%BB%D0%B0%D1%82%D0%B8%D0%BB%D1%8C%D0%BD%D0%BE%D1%81%D1%82%D1%8C
    df['volatility_std12'] = df['std12'] * math.sqrt(1 / 12.)
    df['volatility_std60'] = df['std60'] * math.sqrt(1 / 60.)
    df['volatility_std360'] = df['std360'] * math.sqrt(1 / 360.)

    # old_df = df.copy()
    # already_out_copy_df = already_out_df.copy()
    df = df.iloc[-append_count-5:]
    already_out_df = already_out_df.iloc[[not t in list(df['tradedate']) for t in list(already_out_df['tradedate'])]]
    df = already_out_df.append(df)
    df = df.loc[~df.duplicated('tradedate')]

    # check1= df[['tradedate','close','volatility_std800']].tail(300)
    # check0= old_df[['tradedate','close','volatility_std800']].tail(300)
    # check2= already_out_copy_df[['tradedate','close']].tail(300)

    if not IS_AIRFLOW:
        ema_df = df[['close', 'volatility_std12', 'volatility_std60', 'volatility_std360']]
        fig = ema_df.plot(figsize=(100, 10))
        plt.show()

    saveDataFrame(df, f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_{sec}")


def loadAllIndicators():
    os.makedirs(HIST_INDICATORS_MOEX_PATH, exist_ok=True)
    for f in glob.glob(f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_"):-len(".csv")]
        asyncio.run(loadIndicatorsAsync(sec))

    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    loadAllIndicators()
