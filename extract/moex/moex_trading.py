import asyncio
import logging
import os, sys
import shutil
from copy import copy

import configs
import os.path

import aiohttp
import aiomoex
import numpy as np
import pandas as pd
import configparser
import json
from datetime import datetime, timedelta
from utils import *
import requests
from extract.moex import *

TRADING_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/trading")


async def last_day_turnovers(engine):
    async with aiohttp.ClientSession() as session:
        df_old = None
        fileName = f"{TRADING_PATH}/turnovers"
        if os.path.exists(f"{fileName}.csv"):
            with open(f"{fileName}.csv", "r") as f:
                try:
                    df_old = pd.read_csv(f, index_col=0)
                    df = df_old
                except Exception as e:
                    pass

        f_csv = open(f"{TRADING_PATH}/turnovers.csv", "w+")
        f_txt = open(f"{TRADING_PATH}/turnovers.txt", "w+")
        request_url = f"{MOEX_ISS_URL}/iss/turnovers.json?date=today&land=ru"
        iss = aiomoex.ISSClient(session, request_url)
        data = await iss.get()
        df = pd.DataFrame(data['turnovers'])
        df = df[['NAME', 'ID', 'VALTODAY', 'VALTODAY_USD', 'NUMTRADES', 'UPDATETIME', 'TITLE']]
        if not df_old is None:
            df_old = df_old.loc[[not v[:10] in (d[:10] for d in df['UPDATETIME'].values) for v in df_old['UPDATETIME'].values]]
            df = df_old.append(df)



        f_csv.write(df.to_csv())
        f_txt.write(df.to_string())
        f_csv.close()
        f_txt.close()


async def last_day_aggregates(security, day='today'):
    async with aiohttp.ClientSession() as session:
        df_old = None
        fileName = f"{TRADING_PATH}/aggregates_{security}"
        if os.path.exists(f"{fileName}.csv"):
            with open(f"{fileName}.csv", "r") as f:
                try:
                    df_old = pd.read_csv(f, index_col=0)
                    df_old = df_old[['market_name', 'market_title', 'engine', 'tradedate', 'secid', 'value', 'volume', 'numtrades', 'updated_at']]
                except:
                    pass

        f_csv = open(f"{fileName}.csv", "w+")
        f_txt = open(f"{fileName}.txt", "w+")
        request_url = f"{MOEX_ISS_URL}/iss/securities/{security}/aggregates.json?date={day}&land=ru"
        iss = aiomoex.ISSClient(session, request_url)
        data = await iss.get()
        df = pd.DataFrame(data['aggregates'])
        df = df[['market_name', 'market_title', 'engine', 'tradedate', 'secid', 'value', 'volume', 'numtrades', 'updated_at']]

        if not df_old is None:
            df_old = df_old.loc[[not v in df['tradedate'].values for v in df_old['tradedate'].values]]
            df = df_old.append(df)

        f_csv.write(df.to_csv())
        f_txt.write(df.to_string())
        f_csv.close()
        f_txt.close()


def extractDayResults():
    os.makedirs(TRADING_PATH, exist_ok=True)

    pass


if __name__ == "__main__":
    asyncio.run(last_day_aggregates(engine="stock", market="shares", security="SBER"))

    pass
