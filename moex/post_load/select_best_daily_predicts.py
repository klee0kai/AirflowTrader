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
    maxmin_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}")
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
    if airflow and not isMoexWorkTime():
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
    best_predict_df = sec_predicts_df.iloc[:2]
    best_reversal_df = sec_predicts_df.loc[sec_predicts_df['is_reversal'] == True].iloc[:2]
    sec_predicts_df = sec_predicts_df.sort_values(['macd_12_26_catalyzed_p', 'mean_targents'], ascending=False)
    # todo не наиболее быстрые а наиболее набирающие скорость
    more_fast = sec_predicts_df.iloc[:2]
    without_targets = sec_predicts_df.loc[np.isnan(sec_predicts_df['mean_targents'])].iloc[:2]

    report = ""
    if len(best_predict_df) > 0:
        report += "<i>Движение с целями:</i>\n"
        for d in best_predict_df.itertuples():
            report += f"<b>{d.sec}</b> - {d.shortname} (на {d.tradedate} с ценой {d.close:.3f}): {d.description}\n"

    if len(best_reversal_df) > 0:
        report += "<i>На разороте:</i>\n"
        for d in best_reversal_df.itertuples():
            report += f"<b>{d.sec}</b> {d.shortname} (на {d.tradedate} с ценой {d.close:.3f}): {d.description}\n"

    if len(more_fast) > 0:
        report += "<i>Наиболее быстрые:</i>\n"
        for d in more_fast.itertuples():
            report += f"<b>{d.sec}</b> {d.shortname} (на {d.tradedate} с ценой {d.close:.3f}): {d.description}\n"

    if len(without_targets) > 0:
        report += "<i>Без цели:</i>\n"
        for d in without_targets.itertuples():
            report += f"<b>{d.sec}</b> {d.shortname} (на {d.tradedate} с ценой {d.close:.3f}): {d.description}\n"

    if len(report) > 0:
        report = f"Дневная аналитика по инструментам:\n" + report
        tel_bot.telegram_bot.sendMessage(tel_bot.telegram_rep.ROLE_BEST_PREDICTS, report)

    threads_utils.join_all_threads()


if __name__ == '__main__':
    postLoadBestPredicts()
