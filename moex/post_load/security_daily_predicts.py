import asyncio
import glob
from cmath import nan

import pandas as pd

import tel_bot.telegram_bot
import tel_bot.telegram_rep as userrep
from moex import *

today_str = datetime.now().strftime("%Y-%m-%d")
yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

securities_df = pd.DataFrame()


# Общий вывод по дневным движениям

def __postLoadSecurityPredict(sec):
    sec = sec.upper()
    macd_divergence_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_divergence/macd_divergence_{sec}")
    macd_signal_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_signal/macd_signal_{sec}")
    maxmin_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}")
    secinfo = securities_df.loc[securities_df['secid'] == sec]
    shortname = secinfo['shortname'].iloc[0] if len(secinfo) > 0 else ""
    str_analysis = ""
    if not maxmin_strategy_df1 is None:
        s = maxmin_strategy_df1.loc[maxmin_strategy_df1['direction'] != 'null'].iloc[-1]
        str_analysis += f"<i>Стратегия MaxMin:</i> Цена {s['close']:.3f}, дата {s['tradedate']}. {s['description']}\n"
    if not macd_signal_strategy_df1 is None:
        s = macd_signal_strategy_df1.loc[macd_signal_strategy_df1['is_reversal'] == True].iloc[-1]
        str_analysis += f"<i>Стратегия Macd (Signal):</i> Цена {s['close']:.3f}, дата {s['tradedate']}. {s['description']}\n"
    if not macd_divergence_strategy_df1 is None:
        s = macd_divergence_strategy_df1.loc[macd_divergence_strategy_df1['is_reversal'] == True].iloc[-1]
        str_analysis += f"<i>Стратегия дивергенция по MACD:</i> Цена {s['close']:.3f}, дата {s['tradedate']}. {s['description']}\n"

    if len(str_analysis) > 0:
        str_analysis = f"Ежедневный анализ для <b>{sec}</b> - {shortname} на {today_str}:\n" + str_analysis
        tel_bot.telegram_bot.sendSecPredictInfo(sec, str_analysis)


def postLoadSecPredicts(airflow=False):
    global securities_df
    if airflow and not isMoexWorkTime():
        print("today is weekend")
        return

    tel_bot.telegram_bot.initBot(configs.TELEGRAM_BOT_TOKEN_RELEASE if airflow else configs.TELEGRAM_BOT_TOKEN_DEBUG)
    securities_df = loadDataFrame(f"{COMMON_MOEX_PATH}/securities")
    for sec in set(userrep.getFolowingSecList()):
        __postLoadSecurityPredict(sec)
    pass


if __name__ == '__main__':
    postLoadSecPredicts()
