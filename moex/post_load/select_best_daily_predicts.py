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

securities_df = None
securities_shares_df = None


# Выбор наилучших стратегий по дневным предсказаниям
# оцениваем качество стратегий по баловой системе
#    [1-10] - давность предсказания
#    [1-10] - удаленность до цели (большой ход)
#    [1-3]  - issuesize > 1_000_000_000
#    [1-3] - на развороте
#    [1-..] - результаты тестирования стратегии
# Общий результат по инструменту суммируется для однонаправленных стратегий

def secInfo(sec):
    global securities_df, securities_shares_df
    if securities_df is None:
        securities_df = loadDataFrame(f"{COMMON_MOEX_PATH}/securities")
    if securities_shares_df is None:
        securities_shares_df = loadDataFrame(f"{COMMON_MOEX_PATH}/securities_stock_shares")
    secinfo = securities_df.loc[securities_df['secid'] == sec]
    sec_shares_info = securities_shares_df.loc[securities_shares_df['secid'] == sec]
    shortname = secinfo['shortname'].iloc[0] if len(secinfo) > 0 else ""
    issuesize = sec_shares_info['issuesize'].iloc[0] if len(sec_shares_info) > 0 else 0

    return sec, shortname, issuesize


def targetsEvaluation(targets_percent):
    return 0


def issuzeSizeEvaluation(issuesize):
    return (3 if issuesize > 1_000_000_000 else 0)


def loadMaxMinStrategResults():
    predicts_df = pd.DataFrame()
    for f in glob.glob(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_*.csv"):
        sec = f[len(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_"):-len(".csv")]
        sec_strategy_df = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}")
        sec_strategy_df = sec_strategy_df.loc[sec_strategy_df['direction'] != 'null']
        if len(sec_strategy_df) <= 0:
            continue
        s = sec_strategy_df.iloc[-1]
        s['sec'], s['shortname'], s['issuesize'] = secInfo(sec)
        s['strategy'] = "maxmin"
        s['strategy_name'] = "максимумов-минимумов"
        tradedate = datetime.strptime(s['tradedate'], "%Y-%m-%d")
        s['evaluation'] = 0  # нет разворота
        s['evaluation'] = s['evaluation'] + 10 - min((datetime.now() - tradedate).days, 10)
        s['evaluation'] = s['evaluation'] + targetsEvaluation(s['targets_percent'])
        s['evaluation'] = s['evaluation'] + issuzeSizeEvaluation(s['issuesize'])
        predicts_df = predicts_df.append(s)

    return predicts_df


def loadMacdSignalStrategyResults():
    predicts_df = pd.DataFrame()
    for f in glob.glob(f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_*.csv"):
        sec = f[len(f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_"):-len(".csv")]
        sec_strategy_df = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_{sec}")
        sec_strategy_df = sec_strategy_df.loc[sec_strategy_df['is_reversal'] == True]
        if len(sec_strategy_df) <= 0:
            continue
        s = sec_strategy_df.iloc[-1]
        s['sec'], s['shortname'], s['issuesize'] = secInfo(sec)
        s['strategy'] = "macd_signal"
        s['strategy_name'] = "Macd (Signal)"
        tradedate = datetime.strptime(s['tradedate'], "%Y-%m-%d")
        s['evaluation'] = 3
        s['evaluation'] = s['evaluation'] + 10 - min((datetime.now() - tradedate).days, 10)
        s['evaluation'] = s['evaluation'] + targetsEvaluation(s['targets_percent'])
        s['evaluation'] = s['evaluation'] + issuzeSizeEvaluation(s['issuesize'])
        predicts_df = predicts_df.append(s)

    return predicts_df


def loadMacdDivergenceStrategyResults():
    predicts_df = pd.DataFrame()
    for f in glob.glob(f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence/macd_divergence_*.csv"):
        sec = f[len(f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence/macd_divergence_"):-len(".csv")]
        strategy_df = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence/macd_divergence_{sec}")
        strategy_df = strategy_df.loc[strategy_df['is_reversal'] == True]
        if len(strategy_df) <= 0:
            continue
        s = strategy_df.iloc[-1]
        s['sec'], s['shortname'], s['issuesize'] = secInfo(sec)
        s['strategy'] = "macd_divergence"
        s['strategy_name'] = "дивергенция Macd"
        tradedate = datetime.strptime(s['tradedate'], "%Y-%m-%d")
        s['evaluation'] = 3
        s['evaluation'] = s['evaluation'] + 10 - min((datetime.now() - tradedate).days, 10)
        s['evaluation'] = s['evaluation'] + targetsEvaluation(s['targets_percent'])
        s['evaluation'] = s['evaluation'] + issuzeSizeEvaluation(s['issuesize'])
        predicts_df = predicts_df.append(s)

    return predicts_df


def postLoadBestPredicts(airflow=False):
    global securities_df
    if airflow and not isMoexWorkTime():
        print("today is weekend")
        return

    tel_bot.telegram_bot.initBot(configs.TELEGRAM_BOT_TOKEN_RELEASE if airflow else configs.TELEGRAM_BOT_TOKEN_DEBUG)
    maxMinPredicts = loadMaxMinStrategResults()
    macdSignalPredicts = loadMacdSignalStrategyResults()
    macdDivergencePredicts = loadMacdDivergenceStrategyResults()

    predicts_df = maxMinPredicts.append(macdSignalPredicts).append(macdDivergencePredicts)
    predicts_df = predicts_df.loc[predicts_df['direction'] == 'up']
    predicts_df = predicts_df.sort_values(by=['evaluation', 'sec'])
    bestSecs = predicts_df[['sec', 'evaluation']].groupby(['sec']).sum()
    bestSecs = bestSecs.sort_values(by=['evaluation', 'sec'], ascending=False)
    bestSecs_list = list(bestSecs.index[:3])

    if (len(predicts_df) <= 0):
        print("no predicts")
        return

    report = []
    if len(bestSecs_list) > 0:
        report += ["<i>Лучшие:</i>"]
        for sec in bestSecs_list:
            sec_predict = predicts_df.loc[predicts_df['sec'] == sec]
            if len(sec_predict) <= 0:
                continue
            r = f"<b>{sec_predict.iloc[0]['sec']}</b> {sec_predict.iloc[0]['shortname']}, оценка - {int(sec_predict['evaluation'].sum())}\n"
            for data, s in sec_predict.iterrows():
                r += f"<i>Стратегия {s['strategy_name']}:</i> (на {s['tradedate']} с ценой {s['close']:.3f}): {s['description']}\n"
            report += [r]

    bestMacdDivergence_df = predicts_df.loc[predicts_df['strategy'] == 'macd_divergence']
    bestMacdDivergence_df = bestMacdDivergence_df.sort_values(by=['evaluation', 'sec'], ascending=False)
    if len(bestMacdDivergence_df) > 0:
        r = "<i>Лучшие macd дивергенции:</i>\n"
        for data, s in bestMacdDivergence_df.iloc[:2].iterrows():
            r += f"<b>{s['sec']}</b> {s['shortname']}, оценка - {int(s['evaluation'])} на {s['tradedate']} с ценой {s['close']:.3f}:" \
                 f" {s['description']}\n"
        report += [r]

    bestMacdSignal_df = predicts_df.loc[predicts_df['strategy'] == 'macd_divergence']
    bestMacdSignal_df = bestMacdSignal_df.sort_values(by=['evaluation', 'sec'], ascending=False)
    if len(bestMacdSignal_df) > 0:
        r = "<i>Лучшие macd (signal):</i>\n"
        for data, s in bestMacdSignal_df.iloc[:2].iterrows():
            r += f"<b>{s['sec']}</b> {s['shortname']}, оценка - {int(s['evaluation'])} на {s['tradedate']} с ценой {s['close']:.3f}:" \
                 f" {s['description']}\n"
        report += [r]

    if len(report) > 0:
        report = [f"Дневная аналитика по инструментам:\n"] + report
        for r in report:
            tel_bot.telegram_bot.sendMessage(tel_bot.telegram_rep.ROLE_BEST_PREDICTS, r)

    threads_utils.join_all_threads()


if __name__ == '__main__':
    postLoadBestPredicts()
