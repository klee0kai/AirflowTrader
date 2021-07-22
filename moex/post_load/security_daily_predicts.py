import asyncio
import glob
from cmath import nan

import pandas as pd

import tel_bot.telegram_bot
import tel_bot.telegram_rep as userrep
from moex import *

today_str = datetime.now().strftime("%Y-%m-%d")
yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

securities_df = None


# Общий вывод по дневным движениям

def secDailyPredict(sec, airflow=False):
    global securities_df
    tel_bot.telegram_bot.initBot(configs.TELEGRAM_BOT_TOKEN_RELEASE if airflow else configs.TELEGRAM_BOT_TOKEN_DEBUG)
    if securities_df is None:
        securities_df = loadDataFrame(f"{COMMON_MOEX_PATH}/securities")

    sec = sec.upper()
    ema3_strategy_df = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/3ema/3ema_{sec}")
    macd_divergence_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence/macd_divergence_{sec}")
    macd_signal_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_{sec}")
    maxmin_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}")
    secinfo = securities_df.loc[securities_df['secid'] == sec]
    shortname = secinfo['shortname'].iloc[0] if len(secinfo) > 0 else ""
    str_analysis = ""
    if not ema3_strategy_df is None:
        s = ema3_strategy_df.loc[ema3_strategy_df['is_strategy'] == True].iloc[-1]
        tradedate = datetime.strptime(s['tradedate'], "%Y-%m-%d")
        if (datetime.now() - tradedate).days < 10:
            str_analysis += f"<i>Стратегия 3Ema:</i> Цена {s['close']:.3f}, дата {s['tradedate']}. {s['description']}\n"
    if not maxmin_strategy_df1 is None:
        s = maxmin_strategy_df1.loc[maxmin_strategy_df1['is_strategy'] == True].iloc[-1]
        tradedate = datetime.strptime(s['tradedate'], "%Y-%m-%d")
        if (datetime.now() - tradedate).days < 10:
            str_analysis += f"<i>Стратегия MaxMin:</i> Цена {s['close']:.3f}, дата {s['tradedate']}. {s['description']}\n"
    if not macd_signal_strategy_df1 is None:
        s = macd_signal_strategy_df1.loc[macd_signal_strategy_df1['is_strategy'] == True].iloc[-1]
        tradedate = datetime.strptime(s['tradedate'], "%Y-%m-%d")
        if (datetime.now() - tradedate).days < 10:
            str_analysis += f"<i>Стратегия Macd (Signal):</i> Цена {s['close']:.3f}, дата {s['tradedate']}. {s['description']}\n"
    if not macd_divergence_strategy_df1 is None:
        s = macd_divergence_strategy_df1.loc[macd_divergence_strategy_df1['is_strategy'] == True].iloc[-1]
        tradedate = datetime.strptime(s['tradedate'], "%Y-%m-%d")
        if (datetime.now() - tradedate).days < 10:
            str_analysis += f"<i>Стратегия дивергенция по MACD:</i> Цена {s['close']:.3f}, дата {s['tradedate']}. {s['description']}\n"

    if len(str_analysis) > 0:
        str_analysis = f"Ежедневный анализ для <b>{sec}</b> - {shortname} на {today_str}:\n" + str_analysis
        tel_bot.telegram_bot.sendSecPredictInfo(sec, str_analysis)


def postLoadDailyPredicts(airflow=False):
    global securities_df
    if airflow and not isMoexWorkTime():
        print("today is weekend")
        return

    for sec in set(userrep.getFolowingSecList()):
        secDailyPredict(sec, airflow=airflow)
    pass


if __name__ == '__main__':
    postLoadDailyPredicts()
