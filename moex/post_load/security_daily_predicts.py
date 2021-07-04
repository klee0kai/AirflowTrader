import asyncio
import glob
from cmath import nan

import pandas as pd

import tel_bot.telegram_bot
from moex import *


today_str = datetime.now().strftime("%Y-%m-%d")
yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# Общий вывод по дневным движениям

def __postLoadSecurityPredict(sec):
    macd_strategy_df1 = loadDataFrame(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_{sec}")

    macd_strategy_df1.tail(200)

    str_analysis = f"Ежедневный анализ для {sec} на {today_str}:\n"
    if macd_strategy_df1.iloc[-1]['tradedate'] in (today_str, yesterday_str):
        str_analysis += f"Стратегия MACD (средние с ускорением, без Macd signal): {macd_strategy_df1.iloc[-1]['description']}\n"

    tel_bot.telegram_bot.sendSecPredictInfo(sec, str_analysis)
    pass


def postLoadSecPredicts(airflow=False):
    tel_bot.telegram_bot.initBot(configs.TELEGRAM_BOT_TOKEN_RELEASE if airflow else configs.TELEGRAM_BOT_TOKEN_DEBUG)
    for f in glob.glob(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_*.csv"):
        sec = f[len(f"{DAILY_STRATEGY_MOEX_PATH}/macd_simple/macd_simple1_"):-len(".csv")]
        __postLoadSecurityPredict(sec)
    pass


if __name__ == '__main__':
    postLoadSecPredicts()
