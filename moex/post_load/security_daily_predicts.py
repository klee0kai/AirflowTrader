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
    macd_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_{sec.upper()}")
    maxmin_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/maxmin/maxmin_{sec}")
    sec = sec.upper()
    secinfo = securities_df.loc[securities_df['secid'] == sec]
    shortname = secinfo['shortname'].iloc[0] if len(secinfo) > 0 else ""
    macd_strategy_df1.tail(200)
    str_analysis = ""
    if macd_strategy_df1.iloc[-1]['tradedate'] in (today_str, yesterday_str):
        str_analysis += f"<i>Стратегия MACD (средние с ускорением, без Macd signal):</i> Цена {macd_strategy_df1.iloc[-1]['close']}. {macd_strategy_df1.iloc[-1]['description']}\n"
    if maxmin_strategy_df1.iloc[-1]['tradedate'] in (today_str, yesterday_str):
        str_analysis += f"<i>Стратегия MaxMin:</i> Цена {maxmin_strategy_df1.iloc[-1]['close']}. {maxmin_strategy_df1.iloc[-1]['description']}\n"


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
