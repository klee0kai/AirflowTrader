import asyncio
import glob
from cmath import nan

import pandas as pd

import tel_bot.telegram_bot
from moex import *


# Стратегия пробития минимальных и максимальных значений стоимости акции

async def loadDailyMaxMinStrategyAsync(sec):
    df = loadDataFrame(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_{sec}")
    # orig_df = loadDataFrame(f"{HIST_MOEX_PATH}/stock_shares_{sec}")

    # подсчитываем уже загруженные данные
    already_out_df = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}")
    filled = 0
    if not already_out_df is None and len(df) >= len(already_out_df):
        cond = list(df.iloc[:len(already_out_df)]['close'] == already_out_df['close'])
        filled = cond.index(False) if False in cond else len(already_out_df)
    append_count = len(df) - filled
    if append_count < 0:
        raise Exception("append_count < 0")
    if append_count == 0:
        print(f"loadDailyMaxMinStrategyAsync {sec} already loaded")
        return
    # максимальное скользящее окно 360 + запас на 5 и всякипогрешности
    df = df.tail(min(append_count + 370, len(df)))

    def min_percent(x_df):
        return abs((x_df['low'].min() - x_df['low'].iloc[-1]) / x_df['close'].iloc[-1])

    def max_percent(x_df):
        return abs((x_df['high'].max() - x_df['high'].iloc[-1]) / x_df['close'].iloc[-1])

    # *_p - значит percent, тоесть процентное значение от стоимости
    df['min_30_p'] = pd.DataFrame([min_percent(df_wind) for df_wind in df.rolling(30)])
    df['max_30_p'] = pd.DataFrame([max_percent(df_wind) for df_wind in df.rolling(30)])
    df['min_60_p'] = pd.DataFrame([min_percent(df_wind) for df_wind in df.rolling(60)])
    df['max_60_p'] = pd.DataFrame([max_percent(df_wind) for df_wind in df.rolling(60)])
    df['min_150_p'] = pd.DataFrame([min_percent(df_wind) for df_wind in df.rolling(150)])
    df['max_150_p'] = pd.DataFrame([max_percent(df_wind) for df_wind in df.rolling(150)])
    df['min_360_p'] = pd.DataFrame([min_percent(df_wind) for df_wind in df.rolling(360)])
    df['max_360_p'] = pd.DataFrame([max_percent(df_wind) for df_wind in df.rolling(360)])

    # данные для стратегии направление движения точка, входа, цель (доп движение к цели в процентах), обнаружен разворот
    minmax_strategy_df1 = pd.DataFrame()
    for data, df_wind in df.fillna(0).iterrows():
        s = df_wind[['tradedate', 'close', 'low', 'high', 'min_30_p', 'max_30_p', 'min_60_p', 'max_60_p', 'min_150_p', 'max_150_p', 'min_360_p', 'max_360_p']]
        s['entry'] = s['close']
        s['direction'] = 'null'
        s['is_reversal'] = False
        s['targets'] = ''
        s['targets_percent'] = ''
        s['description'] = ''

        def calcWmaTargetsUp(df_wind):
            targets = df_wind[['wma10', 'wma30', 'wma60', 'wma120', 'wma480']]
            targets = [t for t in targets.iteritems() if t[1] > s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s_targets = ','.join([f"{t[1]:.3f}" for t in targets])
            s_targets_percent = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            return s_targets, s_targets_percent, s_target_desc

        def calcWmaTargetsDown(df_wind):
            targets = df_wind[['wma10', 'wma30', 'wma60', 'wma120', 'wma480']]
            targets = [t for t in targets.iteritems() if t[1] < s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s_targets = ','.join([f"{t[1]:.3f}" for t in targets])
            s_targets_percent = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            return s_targets, s_targets_percent, s_target_desc

        # пробитие 360 дневного максимума
        if float(df_wind['max_360_p']) < 0.01:
            s['direction'] = 'down'
            s['targets'], s['targets_percent'], targetDesc = calcWmaTargetsDown(df_wind)
            s['description'] += f"Обнаружено пробитие 360 дневного максимума. {targetDesc}"
        elif float(df_wind['min_360_p']) < 0.01:
            s['direction'] = 'up'
            s['targets'], s['targets_percent'], targetDesc = calcWmaTargetsUp(df_wind)
            s['description'] += f"Обнаружено пробитие 360 дневного минимума. {targetDesc}"
        elif float(df_wind['max_150_p']) < 0.01:
            s['direction'] = 'down'
            s['targets'], s['targets_percent'], targetDesc = calcWmaTargetsDown(df_wind)
            s['description'] += f"Обнаружено пробитие 150 дневного максимума. {targetDesc}"
        elif float(df_wind['min_150_p']) < 0.01:
            s['direction'] = 'up'
            s['targets'], s['targets_percent'], targetDesc = calcWmaTargetsUp(df_wind)
            s['description'] += f"Обнаружено пробитие 150 дневного минимума. {targetDesc}"
        elif float(df_wind['max_60_p']) < 0.01:
            s['direction'] = 'down'
            s['targets'], s['targets_percent'], targetDesc = calcWmaTargetsDown(df_wind)
            s['description'] += f"Обнаружено пробитие 60 дневного максимума. {targetDesc}"
        elif float(df_wind['min_60_p']) < 0.01:
            s['direction'] = 'up'
            s['targets'], s['targets_percent'], targetDesc = calcWmaTargetsUp(df_wind)
            s['description'] += f"Обнаружено пробитие 60 дневного минимума. {targetDesc}"
        elif float(df_wind['max_30_p']) < 0.01:
            s['direction'] = 'down'
            s['targets'], s['targets_percent'], targetDesc = calcWmaTargetsDown(df_wind)
            s['description'] += f"Обнаружено пробитие 30 дневного максимума. {targetDesc}"
        elif float(df_wind['min_30_p']) < 0.01:
            s['direction'] = 'up'
            s['targets'], s['targets_percent'], targetDesc = calcWmaTargetsUp(df_wind)
            s['description'] += f"Обнаружено пробитие 30 дневного минимума. {targetDesc}"

        minmax_strategy_df1 = minmax_strategy_df1.append(s, ignore_index=True)

    minmax_strategy_df1 = minmax_strategy_df1.iloc[-append_count - 5:]
    tradedatelist = list(minmax_strategy_df1['tradedate'])
    if not already_out_df is None:
        already_out_df = already_out_df.iloc[[not t in tradedatelist for t in list(already_out_df['tradedate'])]]
        minmax_strategy_df1 = already_out_df.append(minmax_strategy_df1)
    minmax_strategy_df1 = minmax_strategy_df1.loc[~minmax_strategy_df1.duplicated('tradedate')]

    minmax_strategy_df1_check = minmax_strategy_df1.tail(300)

    saveDataFrame(minmax_strategy_df1, f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}")


def loadDailyMaxMinStrategy(airflow=False):
    os.makedirs(DAILY_STRATEGY_MOEX_PATH, exist_ok=True)
    os.makedirs(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin", exist_ok=True)
    for f in glob.glob(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_*.csv")[:3]:
        sec = f[len(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_"):-len(".csv")]
        asyncio.run(loadDailyMaxMinStrategyAsync(sec))

    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    loadDailyMaxMinStrategy()
