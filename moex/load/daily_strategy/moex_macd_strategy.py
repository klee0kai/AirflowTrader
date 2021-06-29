import asyncio
import glob
from cmath import nan

from moex import *

IS_AIRFLOW = False


# Стратегия MACD:
# https://bcs-express.ru/novosti-i-analitika/indikator-macd-skol-ziashchie-srednie-v-udobnoi-upakovke
# Для продаж: 12 — быстрая, 26 — медленная, 9 — сигнальная
# Для покупок: 8 — быстрая, 17 — медленная, 9 — сигнальная
#
# todo тестирование стратегии
# todo предсказание стратегии на завтра


async def loadDailyMacdDivergenceStrategyAsync(sec):
    df = loadDataFrame(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_{sec}")

    pass


async def loadDailyMacdSimpleStrategyAsync(sec):
    df = loadDataFrame(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_{sec}")
    orig_df = loadDataFrame(f"{HIST_MOEX_PATH}/stock_shares_{sec}")
    df = df[['tradedate', 'close', 'dummycount', 'macd_12_26', 'macd_12_26_signal9', 'macd_8_17', 'macd_8_17_signal9']]

    df['macd_l_signal'] = (df['macd_12_26'] - df['macd_12_26_signal9'])
    df['macd_s_signal'] = (df['macd_8_17'] - df['macd_8_17_signal9'])

    def signFromMacdPair(macd_l, macd_s):
        return 'keep' if (macd_l < 0) == (macd_s > 0) else ('cell' if macd_l < 0 else ('buy' if macd_s > 0 else 'keep'))

    df['sign'] = df['macd_l_signal'].combine(df['macd_l_signal'], signFromMacdPair)
    df['sign_1'] = df['macd_l_signal'].combine(df['macd_s_signal'], signFromMacdPair)

    df['sign'] = df['sign'].combine(df['sign'].eq(df['sign'].shift()),lambda x,eq: x if not eq else 'keep')
    df['sign_1'] = df['sign_1'].combine(df['sign_1'].eq(df['sign_1'].shift()),lambda x,eq: x if not eq else 'keep')

    df = df.tail(200)
    orig_df = orig_df.tail(200)
    df_check = df[['tradedate', 'close', 'sign', 'sign_1', 'macd_12_26', 'macd_12_26_signal9', 'macd_l_signal', 'dummycount']]
    pass


def loadDailyMacdStrategy():
    os.makedirs(DAILY_STRATEGY_MOEX_PATH, exist_ok=True)
    for f in glob.glob(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_"):-len(".csv")]
        asyncio.run(loadDailyMacdSimpleStrategyAsync(sec))

    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    loadDailyMacdStrategy()
