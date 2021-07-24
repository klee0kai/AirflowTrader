import asyncio
import glob
from cmath import nan

import pandas as pd

from moex import *

IS_AIRFLOW = False

# игнорируем стартовые предсказания, так как данные еще не сформировались
START_OFFSET = 300


# оценка стратегий по следующим оценкам
#  профит - потеря в % за 5 дней.
#  профит - потеря в % за 10 дней.
#  профит - потеря в % за 20 дней.
#  профит - потеря в % за 30 дней.
#  профит - потеря в % за 50 дней.
#  разница с целями за 10,20,30 дней
#  кол-во дней в профите
#  вход указан правильно

def dailyPredictTest(sec):
    sec = sec.upper()

    strategy_file_names = [
        (f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence/macd_divergence_{sec}", f"{DAILY_STRATEGY__MOEX_TEST_PATH}/macd_divergence_/macd_divergence_{sec}"),
        (f"{DAILY_STRATEGY_MOEX_PATH}/3ema/3ema_{sec}", f"{DAILY_STRATEGY__MOEX_TEST_PATH}/3ema/3ema_{sec}"),
        (f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_{sec}", f"{DAILY_STRATEGY__MOEX_TEST_PATH}/macd_signal/macd_signal_{sec}"),
        (f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}", f"{DAILY_STRATEGY__MOEX_TEST_PATH}/maxmin/maxmin_{sec}")
    ]
    hist_df = loadDataFrame(f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_{sec}")

    for strategy_file_name, out_marks_filename in strategy_file_names:
        strategy_df = loadDataFrame(strategy_file_name).iloc[START_OFFSET:].reset_index(drop=True)
        hist_offset = hist_df.index[hist_df['tradedate'] == strategy_df['tradedate'].iloc[0]].tolist()[0]
        test_hist_df = hist_df.iloc[hist_offset:].reset_index(drop=True)
        marks_df = pd.DataFrame()
        for _idx1, s_event in strategy_df.loc[strategy_df['is_strategy'] == True].iloc[:-50].iterrows():
            s_mark = pd.Series()
            entry = s_event['entry']
            s_tradedate = s_mark['tradedate'] = s_event['tradedate']
            direction = s_mark['direction'] = s_event['direction']
            g_st_index = strategy_df.index[strategy_df['tradedate'] == s_tradedate].tolist()[0]

            if direction == 'up':
                g_entry_index = test_hist_df.index[test_hist_df['low'] < entry].tolist()
            elif direction == 'down':
                g_entry_index = test_hist_df.index[test_hist_df['high'] > entry].tolist()
            else:
                raise Exception("todo")

            g_entry_index = [i for i in g_entry_index if i >= g_st_index]
            g_entry_index = g_entry_index[0] if len(g_entry_index) > 0 else nan
            if g_entry_index < g_st_index:
                raise Exception("g_entry_index < g_index")

            if g_entry_index < g_st_index + 10:
                days_passed = 0
                days_profit = 0
                high = low = hist_df['close'].iloc[g_entry_index]
                for _idx2, hist_ser in hist_df.iloc[g_entry_index:].iterrows():
                    low = hist_ser['close'] if low > hist_ser['close'] else low
                    high = hist_ser['close'] if high < hist_ser['close'] else high

                    if direction == 'up':
                        days_profit += 1 if hist_ser['close'] > entry else -1
                    elif direction == 'down':
                        days_profit += 1 if hist_ser['close'] < entry else -1

                    if days_passed in [5, 10, 20, 30, 50] and direction == 'up':
                        low_percent = (entry - low) * 100. / entry
                        high_percent = (high - entry) * 100. / entry
                        s_mark[f"stop_loss{days_passed}"] = -low_percent
                        s_mark[f"profit{days_passed}"] = high_percent
                        s_mark[f"move{days_passed}"] = high_percent - low_percent
                        s_mark[f"days_profit{days_passed}"] = days_profit

                    elif days_passed in [5, 10, 20, 30, 50] and direction == 'down':
                        low_percent = (entry - low) * 100. / entry
                        high_percent = (high - entry) * 100. / entry
                        s_mark[f"stop_loss{days_passed}"] = -high_percent
                        s_mark[f"profit{days_passed}"] = low_percent
                        s_mark[f"move{days_passed}"] = low_percent - high_percent
                        s_mark[f"days_profit{days_passed}"] = days_profit

                    days_passed += 1
                    pass

            marks_df = marks_df.append(s_mark, ignore_index=True)
            pass

        marks_df = marks_df.fillna(0)
        saveDataFrame(marks_df, out_marks_filename)


if __name__ == "__main__":
    dailyPredictTest('SBER')
    pass
