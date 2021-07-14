import asyncio
import glob
from cmath import nan

import pandas as pd

import tel_bot.telegram_bot
from moex import *

IS_AIRFLOW = False


# Классическая стратегия MACD:
#
# https://www.youtube.com/watch?v=BduBdQFyp-Y
# https://bcs-express.ru/novosti-i-analitika/indikator-macd-skol-ziashchie-srednie-v-udobnoi-upakovke
# Для продаж: 12 — быстрая, 26 — медленная, 9 — сигнальная
# Для покупок: 8 — быстрая, 17 — медленная, 9 — сигнальная
#
# todo тестирование стратегии
# todo предсказание стратегии на завтра


async def loadDailyMacdSignalStrategyAsync(sec):
    df = loadDataFrame(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_{sec}")

    # подсчитываем уже загруженные данные
    already_out_df = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_{sec}")
    filled = 0
    if not already_out_df is None and len(df) >= len(already_out_df):
        cond = list(df.iloc[:len(already_out_df)]['close'] == already_out_df['close'])
        filled = cond.index(False) if False in cond else len(already_out_df)
    append_count = len(df) - filled
    if append_count < 0:
        raise Exception("append_count < 0")
    if append_count == 0:
        print(f"loadDailyMacdSignalStrategyAsync {sec} already loaded")
        return
    # максимальное скользящее окно 10 + запас на 5 и всякипогрешности
    df = df.tail(min(append_count + 20, len(df)))

    df['macd_histogram'] = (df['macd_12_26'] - df['macd_12_26_signal9'])
    df['move_close_p'] = df['close'].rolling(2).apply(lambda x_df: (x_df.iloc[-1] - x_df.iloc[0]) / x_df.iloc[0] * 100.)

    # данные для стратегии направление движения точка, входа, цель (доп движение к цели в процентах), обнаружен разворот
    macd_strategy_df1 = pd.DataFrame()
    for df_wind in df.fillna(0).rolling(10):
        if len(df_wind)<2:
            continue
        s = df_wind.iloc[-1][['tradedate', 'close', 'move_close_p', 'macd_12_26', 'macd_12_26_signal9', 'macd_histogram']]
        s['entry'] = s['close']
        s['direction'] = 'null'
        s['is_reversal'] = False
        s['targets'] = ''
        s['targets_percent'] = ''
        s['description'] = ''
        if float(df_wind['move_close_p'].abs().max()) > 4.0:
            continue

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
        if df_wind.iloc[-2]['macd_histogram'] < 0. < df_wind.iloc[-1]['macd_histogram'] and df_wind['macd_12_26'].max() < 0 and df_wind['macd_12_26_signal9'].max() < 0:
            s['is_reversal'] = True
            s['direction'] = 'up'
            s['targets'], s['targets_percent'], targetDesc = calcSmaTargetsUp(df_wind.iloc[-1])
            s['description'] += f"Обнаружен разворот наверх на цене {s['entry']:.3f}. {targetDesc}"
        elif df_wind.iloc[-2]['macd_histogram'] > 0. > df_wind.iloc[-1]['macd_histogram'] and df_wind['macd_12_26'].min() > 0 and df_wind['macd_12_26_signal9'].min() > 0:
            s['is_reversal'] = True
            s['direction'] = 'down'
            s['targets'], s['targets_percent'], targetDesc = calcSmaTargetsDown(df_wind.iloc[-1])
            s['description'] += f"Обнаружен разворот вниз на цене {s['entry']:.3f}. {targetDesc}"

        macd_strategy_df1 = macd_strategy_df1.append(s, ignore_index=True)

    macd_strategy_df1 = macd_strategy_df1.iloc[-append_count - 5:]
    tradedatelist = list(macd_strategy_df1['tradedate'])
    if not already_out_df is None:
        already_out_df = already_out_df.iloc[[not t in tradedatelist for t in list(already_out_df['tradedate'])]]
        macd_strategy_df1 = already_out_df.append(macd_strategy_df1)
    macd_strategy_df1 = macd_strategy_df1.loc[~macd_strategy_df1.duplicated('tradedate')]

    saveDataFrame(macd_strategy_df1, f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_{sec}")


def loadDailyMacdSignalStrategy(airflow=False):
    os.makedirs(DAILY_STRATEGY_MOEX_PATH, exist_ok=True)
    os.makedirs(f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal", exist_ok=True)
    for f in glob.glob(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_"):-len(".csv")]
        asyncio.run(loadDailyMacdSignalStrategyAsync(sec))

    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    loadDailyMacdSignalStrategy()
