import asyncio
import glob
from cmath import nan

import pandas as pd

from moex import *

IS_AIRFLOW = False

# игнорируем стартовые предсказания, так как данные еще не сформировались
START_OFFSET = 300


# оценка стратегий по следующим оценкам
#  профит - потеря в % за 30 дней.
#  кол-во дней в профите (выше 0%, 1% профита)
#  вход указан правильно

def dailyPredictTest(sec):
    sec = sec.upper()

    strategy_file_names = [
        (f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence/macd_divergence_{sec}", f"{DAILY_STRATEGY__MOEX_TEST_PATH}/macd_divergence/macd_divergence_{sec}"),
        (f"{DAILY_STRATEGY_MOEX_PATH}/3ema/3ema_{sec}", f"{DAILY_STRATEGY__MOEX_TEST_PATH}/3ema/3ema_{sec}"),
        (f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_{sec}", f"{DAILY_STRATEGY__MOEX_TEST_PATH}/macd_signal/macd_signal_{sec}"),
        (f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}", f"{DAILY_STRATEGY__MOEX_TEST_PATH}/maxmin/maxmin_{sec}")
    ]
    hist_df = loadDataFrame(f"{HIST_TRANSFORM1_MOEX_PATH}/stock_shares_{sec}")

    for strategy_file_name, out_marks_filename in strategy_file_names:
        strategy_df = loadDataFrame(strategy_file_name).iloc[START_OFFSET:].reset_index(drop=True)
        if len(strategy_df) <= 0:
            continue
        hist_offset = hist_df.index[hist_df['tradedate'] == strategy_df['tradedate'].iloc[0]].tolist()[0]
        test_hist_df = hist_df.iloc[hist_offset:].reset_index(drop=True)
        marks_df = pd.DataFrame()
        for _idx1, s_event in strategy_df.loc[strategy_df['is_strategy'] == True].iloc[:-50].iterrows():
            s_mark = pd.Series(dtype=pd.StringDtype())
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

            g_entry_index = [i for i in g_entry_index if i > g_st_index]
            g_entry_index = g_entry_index[0] if len(g_entry_index) > 0 else nan
            if g_entry_index < g_st_index:
                raise Exception("g_entry_index < g_index")

            if g_entry_index < g_st_index + 10:
                days_passed = 0
                days_profit = 0
                high = low = hist_df['close'].iloc[g_entry_index]
                s_mark[f"delay"] = g_entry_index - g_st_index
                for _idx2, hist_ser in hist_df.iloc[g_entry_index:].iterrows():
                    low = hist_ser['close'] if low > hist_ser['close'] else low
                    high = hist_ser['close'] if high < hist_ser['close'] else high

                    if direction == 'up':
                        days_profit += 1 if hist_ser['close'] > entry else -1
                    elif direction == 'down':
                        days_profit += 1 if hist_ser['close'] < entry else -1

                    if days_passed in [30] and direction == 'up':
                        low_percent = (entry - low) * 100. / entry
                        high_percent = (high - entry) * 100. / entry
                        s_mark[f"stop_loss{days_passed}"] = -low_percent
                        s_mark[f"profit{days_passed}"] = high_percent
                        s_mark[f"move{days_passed}"] = high_percent - low_percent
                        s_mark[f"days_profit{days_passed}"] = days_profit

                    elif days_passed in [30] and direction == 'down':
                        low_percent = (entry - low) * 100. / entry
                        high_percent = (high - entry) * 100. / entry
                        s_mark[f"stop_loss{days_passed}"] = -high_percent
                        s_mark[f"profit{days_passed}"] = low_percent
                        s_mark[f"move{days_passed}"] = low_percent - high_percent
                        s_mark[f"days_profit{days_passed}"] = days_profit

                    if days_passed > 50:
                        break
                    else:
                        days_passed += 1

            marks_df = marks_df.append(s_mark, ignore_index=True)
            pass

        marks_df = marks_df.fillna(0)
        saveDataFrame(marks_df, out_marks_filename)


def commonSecResults():
    common_marks_df = pd.DataFrame()
    for f in glob.glob(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_"):-len(".csv")].upper()
        strategy_file_names = [
            (f"{DAILY_STRATEGY__MOEX_TEST_PATH}/macd_divergence/macd_divergence_{sec}", "macd_divergence"),
            (f"{DAILY_STRATEGY__MOEX_TEST_PATH}/3ema/3ema_{sec}", "3ema"),
            (f"{DAILY_STRATEGY__MOEX_TEST_PATH}/macd_signal/macd_signal_{sec}", "macd_signal"),
            (f"{DAILY_STRATEGY__MOEX_TEST_PATH}/maxmin/maxmin_{sec}", "maxmin")
        ]

        for marks_filename, strategy in strategy_file_names:
            try:
                print(f"open {marks_filename}")
                marks_df = loadDataFrame(marks_filename)
                s_mark = pd.Series(dtype=pd.StringDtype())
                s_mark['sec'] = sec
                s_mark['strategy'] = strategy
                s_mark['days_profit30'] = marks_df['days_profit30'].mean()
                s_mark['days_profit30_std'] = marks_df['days_profit30'].std()

                s_mark['delay'] = marks_df['delay'].mean()
                s_mark['delay_std'] = marks_df['delay'].std()

                s_mark['move30'] = marks_df['move30'].mean()
                s_mark['move30_std'] = marks_df['move30'].std()

                s_mark['profit30'] = marks_df['profit30'].mean()
                s_mark['profit30_std'] = marks_df['profit30'].std()

                s_mark['stop_loss30'] = marks_df['stop_loss30'].mean()
                s_mark['stop_loss30_std'] = marks_df['stop_loss30'].std()
                s_mark['count'] = len(marks_df)

                common_marks_df = common_marks_df.append(s_mark, ignore_index=True)
            except Exception as e:
                print(f" err {e}")
    common_marks_df = common_marks_df.sort_values(by=['days_profit30', 'move30'], ascending=False)
    saveDataFrame(common_marks_df, f"{DAILY_STRATEGY__MOEX_TEST_PATH}/common_result")


if __name__ == "__main__":
    for f in glob.glob(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_*.csv"):
        sec = f[len(f"{HIST_INDICATORS_MOEX_PATH}/stock_shares_"):-len(".csv")]
        dailyPredictTest(sec)

    commonSecResults()
    pass
