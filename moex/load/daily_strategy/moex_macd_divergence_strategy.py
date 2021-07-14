import asyncio
import glob
from cmath import nan

import numpy as np
import pandas as pd

import tel_bot.telegram_bot
from moex import *

IS_AIRFLOW = False


# Стратегия дивенгерция MACD:
#  https://www.youtube.com/watch?v=1ahPo9Mlaik
#   Дивенгерция - цена обновляет максимумы, однако macd histogram не смог обновить свои максимумы (или максимумы macd hist < 0)
#   Ковенгерция - цена обновляет минимумы, при этом macd histogram не смог обновить свои минимумы (или минимумы macd hist >0 )
#   при этом в зоне дивергенции и ковергенции индикатор macd должен менять знак (отображение отката)
# todo тестирование стратегии
# todo предсказание стратегии на завтра


async def loadDailyMacdDivergenceStrategyAsync(sec):
    fileName = f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence/macd_divergence_{sec}"
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
        print(f"loadDailyMacdDivergenceStrategyAsync {sec} already loaded")
        return
    # максимальное скользящее окно 200 + запас на 10 и всякипогрешности
    df = df.tail(min(append_count + 210, len(df)))

    df['macd_histogram'] = (df['macd_12_26'] - df['macd_12_26_signal9'])
    df['move_close_p'] = df['close'].rolling(2).apply(lambda x_df: (x_df.iloc[-1] - x_df.iloc[0]) / x_df.iloc[0] * 100.)
    df['high_max_extremum_20'] = df['high'].rolling(20, center=True, min_periods=11).apply(lambda x_df: x_df.max() == x_df.iloc[10])
    df['low_min_extremum_20'] = df['low'].rolling(20, center=True, min_periods=11).apply(lambda x_df: x_df.min() == x_df.iloc[10])
    # df['macd_r'] = df['macd_histogram'].rolling(10, center=True, min_periods=2).apply(lambda x_df: len(x_df))

    # # данные для стратегии направление движения точка, входа, цель (доп движение к цели в процентах), обнаружен разворот
    macd_strategy_df1 = pd.DataFrame()
    for df_wind in df.fillna(0).rolling(200):
        s = df_wind.iloc[-1][['tradedate', 'close', 'move_close_p', 'macd_12_26', 'macd_12_26_signal9', 'macd_histogram',
                              'high_max_extremum_20', 'low_min_extremum_20']]
        s['entry'] = s['close']
        s['direction'] = 'null'
        s['is_reversal'] = False
        s['targets'] = ''
        s['targets_percent'] = ''
        s['description'] = ''

        topPriceExtremumIndeces = [i for i, e in enumerate(list(df_wind['high_max_extremum_20'])) if e == True]
        bottomPriceExtremumIndeces = [i for i, e in enumerate(list(df_wind['low_min_extremum_20'])) if e == True]

        # если экстремумы не найдены, или слишкоом большие дневные движения (больше 4%) пропускаем
        if len(df_wind) < 2 or len(topPriceExtremumIndeces) < 2 or len(bottomPriceExtremumIndeces) < 2:
            macd_strategy_df1 = macd_strategy_df1.append(s, ignore_index=True)
            continue

        topPriceExtremumIndeces = topPriceExtremumIndeces[-2:]
        bottomPriceExtremumIndeces = bottomPriceExtremumIndeces[-2:]

        topPriceExtremum = [df_wind.iloc[topPriceExtremumIndeces[-2]]['high'], df_wind.iloc[topPriceExtremumIndeces[-1]]['high']]
        topMacdExtremum = [df_wind.iloc[topPriceExtremumIndeces[-2] - 3:topPriceExtremumIndeces[-2] + 3]['macd_histogram'].max(),
                           df_wind.iloc[topPriceExtremumIndeces[-1] - 3:topPriceExtremumIndeces[-1] + 3]['macd_histogram'].max()]

        bottomPriceExtremum = [df_wind.iloc[bottomPriceExtremumIndeces[-2]]['low'], df_wind.iloc[bottomPriceExtremumIndeces[-1]]['low']]
        bottomMacdExtremum = [df_wind.iloc[bottomPriceExtremumIndeces[-2] - 3:bottomPriceExtremumIndeces[-2] + 3]['macd_histogram'].min(),
                              df_wind.iloc[bottomPriceExtremumIndeces[-1] - 3:bottomPriceExtremumIndeces[-1] + 3]['macd_histogram'].min()]

        def calcSmaTargetsUp(df_wind):
            targets = df_wind[['sma10', 'sma30', 'sma60', 'sma120', 'sma480']]
            targets = [t for t in targets.iteritems() if t[1] > s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s_targets = ','.join([f"{t[1]:.3f}" for t in targets])
            s_targets_percent = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            return s_targets, s_targets_percent, s_target_desc

        def calcSmaTargetsDown(df_wind):
            targets = df_wind[['sma10', 'sma30', 'sma60', 'sma120', 'sma480']]
            targets = [t for t in targets.iteritems() if t[1] < s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s_targets = ','.join([f"{t[1]:.3f}" for t in targets])
            s_targets_percent = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            return s_targets, s_targets_percent, s_target_desc

            # разворот происходит, если macd много больше нуля и обгоняет стремясь к нулю сигнальную macd

        def isGrowing(values):
            return len(values) > 0 and np.all(np.diff(values) > 0)

        def isFalling(values):
            return len(values) > 0 and np.all(np.diff(values) < 0)

        # ковергенция цена обновила максимумы, однако macd не смог обновить максимумы
        if isGrowing(topPriceExtremum) and isFalling(topMacdExtremum) \
                and (df_wind.iloc[topPriceExtremumIndeces[-2]:topPriceExtremumIndeces[-1]]['macd_histogram'] < 0).any():
            s['direction'] = 'down'
            s['is_reversal'] = True
            s['entry'] = topPriceExtremum[-1]
            s['targets'], s['targets_percent'], targetDesc = calcSmaTargetsDown(df_wind.iloc[-1])
            s['description'] += f"Обнаружена конвергенция вниз на цене {s['entry']:.3f}. {targetDesc}. "
        # дивергенция цена обновила минимумы, однако macd не смог обновить максимумы
        elif isFalling(bottomPriceExtremum) and isGrowing(bottomMacdExtremum) \
                and (df_wind.iloc[bottomPriceExtremumIndeces[-2]:bottomPriceExtremumIndeces[-1]]['macd_histogram'] > 0).any():
            s['direction'] = 'up'
            s['is_reversal'] = True
            s['entry'] = bottomPriceExtremum[-1]

            s['targets'], s['targets_percent'], targetDesc = calcSmaTargetsUp(df_wind.iloc[-1])
            s['description'] += f"Обнаружен дивенгерция вверх на цене {s['entry']:.3f}. {targetDesc}. "

        macd_strategy_df1 = macd_strategy_df1.append(s, ignore_index=True)

    macd_strategy_df1 = macd_strategy_df1.iloc[-append_count - 5:]

    tradedatelist = list(macd_strategy_df1['tradedate'])
    if not already_out_df is None:
        already_out_df = already_out_df.iloc[[not t in tradedatelist for t in list(already_out_df['tradedate'])]]
        macd_strategy_df1 = already_out_df.append(macd_strategy_df1)
    macd_strategy_df1 = macd_strategy_df1.loc[~macd_strategy_df1.duplicated('tradedate')]

    saveDataFrame(macd_strategy_df1, fileName)


def loadDailyMacdDivergenceStrategy(airflow=False):
    os.makedirs(DAILY_STRATEGY_MOEX_PATH, exist_ok=True)
    os.makedirs(f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence", exist_ok=True)
    if not airflow:
        asyncio.run(loadDailyMacdDivergenceStrategyAsync('SBER'))
    else:
        for f in glob.glob(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_*.csv"):
            sec = f[len(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_"):-len(".csv")]
            asyncio.run(loadDailyMacdDivergenceStrategyAsync(sec))

    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    loadDailyMacdDivergenceStrategy()
