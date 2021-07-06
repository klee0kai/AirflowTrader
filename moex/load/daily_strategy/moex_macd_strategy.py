import asyncio
import glob
from cmath import nan

import pandas as pd

import tel_bot.telegram_bot
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
    # orig_df = loadDataFrame(f"{HIST_MOEX_PATH}/stock_shares_{sec}")

    # подсчитываем уже загруженные данные
    already_out_df = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_{sec}")
    filled = 0
    if not already_out_df is None and len(df) >= len(already_out_df):
        cond = list(df.iloc[:len(already_out_df)]['close'] == already_out_df['close'])
        filled = cond.index(False) if False in cond else len(already_out_df)
    append_count = len(df) - filled
    append_count = 3
    if append_count < 0:
        raise Exception("append_count < 0")
    if append_count == 0:
        print(f"loadDailyMacdSimpleStrategyAsync {sec} already loaded")
        return
    # максимальное скользящее окно 5 + запас на 5 и всякипогрешности
    df = df.tail(min(append_count + 20, len(df)))

    # *_p - значит percent, тоесть процентное значение от стоимости
    df['macd_12_26_p'] = df['macd_12_26'] / df['close'] * 100.
    df['macd_8_17_p'] = df['macd_8_17'] / df['close'] * 100.
    df['macd_12_26_signal9_p'] = df['macd_12_26_signal9'] / df['close'] * 100.
    df['macd_8_17_signal9_p'] = df['macd_8_17_signal9'] / df['close'] * 100.

    df['macd_l_signal_p'] = (df['macd_12_26_p'] - df['macd_12_26_signal9_p'])
    df['macd_s_signal_p'] = (df['macd_8_17_p'] - df['macd_8_17_signal9_p'])

    df['move_close_p'] = df['close'].rolling(2).apply(lambda x_df: (x_df.iloc[-1] - x_df.iloc[0]) / x_df.iloc[0] * 100.)
    # прекидываем macd катализиронный на ход вперед, за счет динамики изменения
    df['macd_8_17_catalyzed_p'] = df['macd_8_17_p'].rolling(3).apply(lambda x_df: x_df.iloc[-1] + (x_df.iloc[-1] - x_df.iloc[0]) / len(x_df))
    df['macd_12_26_catalyzed_p'] = df['macd_12_26_p'].rolling(3).apply(lambda x_df: x_df.iloc[-1] + (x_df.iloc[-1] - x_df.iloc[0]) / len(x_df))

    # macd_strategy_df1 = df[['tradedate', 'close', 'move_close_p', 'macd_12_26_p', 'macd_12_26_catalyzed_p']]
    # данные для стратегии направление движения точка, входа, цель (доп движение к цели в процентах), обнаружен разворот
    macd_strategy_df1 = pd.DataFrame()
    for df_wind in df.fillna(0).rolling(5):
        s = df_wind.iloc[-1][['tradedate', 'close', 'move_close_p', 'macd_12_26_p', 'macd_12_26_catalyzed_p']]
        s['entry'] = s['close']
        s['direction'] = 'null'
        s['is_reversal'] = False
        s['targets'] = ''
        s['targets_percent'] = ''
        s['description'] = ''
        # разворот происходит если дневное движение цены меньше 4%, macd меняет знак на более 0.1% (чтобы избежать погршностей )
        if s['macd_12_26_catalyzed_p'] > 0.1 and df_wind['macd_12_26_p'].mean() < 0 and abs(s['move_close_p'] < 4):
            s['is_reversal'] = True
            s['direction'] = 'up'
            targets = df_wind.iloc[-1][['wma10', 'wma30', 'wma60', 'wma120', 'wma480']]
            targets = [t for t in targets.iteritems() if t[1] > s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s['targets'] = ','.join([f"{t[1]:.3f}" for t in targets])
            s['targets_percent'] = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            s['description'] += f"Обнаружен разворот наверх на цене {s['entry']:.3f}. {s_target_desc}"
        elif s['macd_12_26_catalyzed_p'] < 0.1 and df_wind['macd_12_26_p'].mean() > 0 and abs(s['move_close_p'] < 4):
            s['is_reversal'] = True
            s['direction'] = 'down'

            targets = df_wind.iloc[-1][['wma10', 'wma30', 'wma60', 'wma120', 'wma480']]
            targets = [t for t in targets.iteritems() if t[1] < s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s['targets'] = ','.join([f"{t[1]:.3f}" for t in targets])
            s['targets_percent'] = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            s['description'] += f"Обнаружен разворот вниз на цене {s['entry']:.3f}. {s_target_desc}"

        elif s['move_close_p'] <= -4 and df_wind['macd_12_26_p'].mean() > 0:
            s['is_reversal'] = True
            s['direction'] = 'up'

            targets = df_wind.iloc[-1][['wma10', 'wma30', 'wma60', 'wma120', 'wma480']]
            targets = [t for t in targets.iteritems() if t[1] > s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s['targets'] = ','.join([f"{t[1]:.3f}" for t in targets])
            s['targets_percent'] = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            s['description'] += f"Обнаружен разворот наверх на цене {s['entry']:.3f} с дневным движением цены {s['move_close_p']:.3f} {s_target_desc}" \
                                f"Будьте внимательны, рынок может продолжить движение наверх, либо большой участник может развернуть движение вниз.  (При затянувшемся тренде) "

        elif s['move_close_p'] >= 4 and df_wind['macd_12_26_p'].mean() < 0:
            s['is_reversal'] = True
            s['direction'] = 'down'

            targets = df_wind.iloc[-1][['wma10', 'wma30', 'wma60', 'wma120', 'wma480']]
            targets = [t for t in targets.iteritems() if t[1] < s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s['targets'] = ','.join([f"{t[1]:.3f}" for t in targets])
            s['targets_percent'] = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            s['description'] += f"Обнаружен разворот вниз на цене {s['entry']:.3f}. {s_target_desc}" \
                                f"Будьте внимательны, рынок может продолжить движение вниз, либо большой участник может развернуть движение вверх. (При затянувшемся тренде) "
        elif s['move_close_p'] <= -4 and df_wind['macd_12_26_p'].mean() < 0:
            s['description'] += f"Ация потеряла за день {s['move_close_p']:.3f} при движении вниз. Возможен откат на 2-3% вверх с продолжением движения или разворотом."

        elif s['move_close_p'] >= 4 and df_wind['macd_12_26_p'].mean() > 0:
            s['description'] += f"Ация набрала за день {s['move_close_p']:.3f} при движении вверх. Возможен откат на 2-3% вниз с продолжением движения или разворотом."
        elif s['move_close_p'] >= abs(4):
            s['description'] += f"Ация набрала за день {s['move_close_p']:.3f}. Возможен откат на 2-3%."

        elif s['macd_12_26_catalyzed_p'] > 0.2 and df_wind['macd_12_26_p'].mean() > 0.2:
            s['is_reversal'] = False
            s['direction'] = 'up'

            targets = df_wind.iloc[-1][['wma10', 'wma30', 'wma60', 'wma120', 'wma480']]
            targets = [t for t in targets.iteritems() if t[1] > s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s['targets'] = ','.join([f"{t[1]:.3f}" for t in targets])
            s['targets_percent'] = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            s['description'] += f"Уверенное движение вверх. {s_target_desc}"

        elif s['macd_12_26_catalyzed_p'] < 0.2 and df_wind['macd_12_26_p'].mean() < 0.2:
            s['is_reversal'] = False
            s['direction'] = 'down'

            targets = df_wind.iloc[-1][['wma10', 'wma30', 'wma60', 'wma120', 'wma480']]
            targets = [t for t in targets.iteritems() if t[1] < s['close']]
            targets = [list(t) + [(t[1] - s['close']) * 100. / s['close']] for t in targets]
            s['targets'] = ','.join([f"{t[1]:.3f}" for t in targets])
            s['targets_percent'] = ','.join([f"{t[2]:.3f}" for t in targets])
            s_target_desc = 'Цели не обнаружены' if len(targets) <= 0 else ('цели: ' + ' , '.join([f"{t[0]} : {t[1]:.3f} ({t[2]:.2f}%)" for t in targets]))
            s['description'] += f"Уверенное движение вниз. {s_target_desc}"

        macd_strategy_df1 = macd_strategy_df1.append(s, ignore_index=True)

    macd_strategy_df1 = macd_strategy_df1.iloc[-append_count - 5:]
    tradedatelist = list(macd_strategy_df1['tradedate'])
    already_out_df = already_out_df.iloc[[not t in tradedatelist for t in list(already_out_df['tradedate'])]]
    macd_strategy_df1 = already_out_df.append(macd_strategy_df1)
    macd_strategy_df1 = macd_strategy_df1.loc[~macd_strategy_df1.duplicated('tradedate')]

    macd_strategy_df1_check = macd_strategy_df1.tail(300)
    df_check2 = df[['tradedate', 'close', 'move_close_p',
                    'macd_12_26_p', 'macd_12_26_catalyzed_p', 'macd_12_26_signal9_p', 'macd_l_signal_p',
                    'macd_8_17_p', 'macd_8_17_catalyzed_p', 'macd_8_17_signal9_p', 'macd_s_signal_p'
                    ]]
    saveDataFrame(macd_strategy_df1, f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_{sec}")


def loadDailyMacdStrategy(airflow=False):
    os.makedirs(DAILY_STRATEGY_MOEX_PATH, exist_ok=True)
    os.makedirs(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple", exist_ok=True)
    for f in glob.glob(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_"):-len(".csv")]
        asyncio.run(loadDailyMacdSimpleStrategyAsync(sec))

    chmodForAll(MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    loadDailyMacdStrategy()
