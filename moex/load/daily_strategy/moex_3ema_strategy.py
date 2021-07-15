import asyncio
import glob
from cmath import nan

from moex import *

IS_AIRFLOW = False


# Стратегия 3ema:
#
# https://binium.ru/strategiya-3-skolzyashhie-srednie/
# с периодами: 5, 10 и 15.
#
# todo тестирование стратегии
# todo предсказание стратегии на завтра

async def loadDail3EmaStrategyAsync(sec):
    fileName = f"{DAILY_STRATEGY_MOEX_PATH}/3ema/3ema_{sec}"
    df = loadDataFrame(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_{sec}")

    # подсчитываем уже загруженные данные
    already_out_df = loadDataFrame(fileName)
    filled = 0
    if not already_out_df is None and len(df) >= len(already_out_df):
        cond = list(df.iloc[:len(already_out_df)]['close'] == already_out_df['close'])
        filled = cond.index(False) if False in cond else len(already_out_df)
    append_count = len(df) - filled
    if append_count < 0:
        raise Exception("append_count < 0")
    if append_count == 0:
        print(f"loadDail3EmaStrategyAsync {sec} already loaded")
        return
    # максимальное скользящее окно 20 + запас на 50 и всякипогрешности
    df = df.tail(min(append_count + 70, len(df)))

    # ema - экспоненциальная скользящая среднияя (simple Moving Average)
    def ema(df, buffsize):
        a = 2. / (buffsize + 1)
        ema.oldEma = float(df) * a + (1 - a) * ema.oldEma
        return ema.oldEma

    ema.oldEma = 0
    df['ema5'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 5))
    ema.oldEma = 0
    df['ema10'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 10))
    ema.oldEma = 0
    df['ema15'] = df['close'].rolling(window=1).apply(lambda x: ema(x, 15))

    # # данные для стратегии направление движения точка, входа, цель (доп движение к цели в процентах), обнаружен разворот
    sec_strategy_df1 = pd.DataFrame()
    for data, df_wind in df.fillna(0).iterrows():
        s = df_wind[['tradedate', 'close', 'ema5', 'ema10', 'ema15']]
        s['entry'] = s['close']
        s['direction'] = 'null'
        s['is_reversal'] = False
        s['is_strategy'] = False  # стратегия что то накопала
        s['targets'] = ''
        s['targets_percent'] = ''
        s['description'] = ''

        if s['ema5'] < s['ema10'] < s['ema15']:
            s['is_strategy'] = True
            s['direction'] = 'down'
            s['description'] += f"Стабильное движение вниз при цене {s['entry']:.3f}."
        # дивергенция цена обновила минимумы, однако macd не смог обновить максимумы
        # слишком древние маркеры пропускаем
        elif s['ema5'] > s['ema10'] > s['ema15']:
            s['is_strategy'] = True
            s['direction'] = 'up'
            s['description'] += f"Стабильное движение вверх при цене {s['entry']:.3f}."

        sec_strategy_df1 = sec_strategy_df1.append(s, ignore_index=True)

    sec_strategy_df1 = sec_strategy_df1.iloc[-append_count - 5:]

    tradedatelist = list(sec_strategy_df1['tradedate'])
    if not already_out_df is None:
        already_out_df = already_out_df.iloc[[not t in tradedatelist for t in list(already_out_df['tradedate'])]]
        sec_strategy_df1 = already_out_df.append(sec_strategy_df1)
    sec_strategy_df1 = sec_strategy_df1.loc[~sec_strategy_df1.duplicated('tradedate')]

    saveDataFrame(sec_strategy_df1, fileName)


def loadDaily3emaStrategy(airflow=False):
    os.makedirs(DAILY_STRATEGY_MOEX_PATH, exist_ok=True)
    os.makedirs(f"{DAILY_STRATEGY_MOEX_PATH}/3ema", exist_ok=True)
    if not airflow:
        asyncio.run(loadDail3EmaStrategyAsync('SBER'))
    else:
        for f in glob.glob(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_*.csv"):
            sec = f[len(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_"):-len(".csv")]
            asyncio.run(loadDail3EmaStrategyAsync(sec))

    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    loadDaily3emaStrategy()
