import asyncio
import glob
from cmath import nan
import math

import pandas as pd
import utils.threads_utils as threads_utils
import tel_bot.telegram_bot
from moex import *

today_str = datetime.now().strftime("%Y-%m-%d")
yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# Выбор наилучших стратегий по дневным предсказаниям

def __loadSecLastPredict(sec):
    macd_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_{sec}")
    securities_df = loadDataFrame(f"{COMMON_MOEX_PATH}/securities")
    secinfo = securities_df.loc[securities_df['secid'] == sec]
    shortname = secinfo['shortname'].iloc[0] if len(secinfo) > 0 else ""

    macd_strategy_df1['sec'] = sec
    macd_strategy_df1['shortname'] = shortname
    macd_strategy_df1 = macd_strategy_df1.sort_values(['tradedate'])
    toleranceDays = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(0, 4)]
    if macd_strategy_df1.iloc[-1]['tradedate'] in toleranceDays:
        return macd_strategy_df1.iloc[-1]


def postLoadBestPredicts(airflow=False):
    if airflow and datetime.now().isoweekday() in (6, 7):
        print("today is weekend")
        return

    tel_bot.telegram_bot.initBot(configs.TELEGRAM_BOT_TOKEN_RELEASE if airflow else configs.TELEGRAM_BOT_TOKEN_DEBUG)
    sec_predicts_df = pd.DataFrame()
    for f in glob.glob(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_*.csv"):
        sec = f[len(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_"):-len(".csv")]
        s = __loadSecLastPredict(sec)
        if s is None:
            continue
        sec_predicts_df = sec_predicts_df.append(s)

    if (len(sec_predicts_df) <= 0):
        print("no predicts")
        return

    sec_predicts_df['mean_targents'] = [np.array(str(d.targets_percent).split(',')).astype(float).mean() for d in sec_predicts_df.itertuples()]
    sec_predicts_df = sec_predicts_df.sort_values(['mean_targents', 'macd_12_26_catalyzed_p'], ascending=False)
    sec_predicts_df = sec_predicts_df.loc[sec_predicts_df['direction'] == 'up']
    best_predict_df = sec_predicts_df.iloc[:3]
    best_reversal_df = sec_predicts_df.loc[sec_predicts_df['is_reversal'] == True].iloc[:3]
    sec_predicts_df = sec_predicts_df.sort_values(['macd_12_26_catalyzed_p', 'mean_targents'], ascending=False)
    more_fast = sec_predicts_df.iloc[:3]
    without_targets = sec_predicts_df.loc[np.isnan(sec_predicts_df['mean_targents'])].iloc[:3]

    report = f"Дневная аналитика по инструментам:\n "

    report += "- Движение с целями:\n"
    for d in best_predict_df.itertuples():
        report += f"{d.sec} - {d.shortname} (на {d.tradedate} с ценой {d.close:.3f}): {d.description}\n"

    report += "- На разороте:\n"
    for d in best_reversal_df.itertuples():
        report += f"{d.sec} - {d.shortname} (на {d.tradedate} с ценой {d.close:.3f}): {d.description}\n"

    report += "- Наиболее быстрые:\n"
    for d in more_fast.itertuples():
        report += f"{d.sec} - {d.shortname} (на {d.tradedate} с ценой {d.close:.3f}): {d.description}\n"

    report += "- Без цели:\n"
    for d in without_targets.itertuples():
        report += f"{d.sec} - {d.shortname} (на {d.tradedate} с ценой {d.close:.3f}): {d.description}\n"

    tel_bot.telegram_bot.sendMessage(tel_bot.telegram_rep.ROLE_BEST_PREDICTS, report)

    threads_utils.join_all_threads()


if __name__ == '__main__':
    postLoadBestPredicts()
