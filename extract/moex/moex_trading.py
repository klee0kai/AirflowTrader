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


async def last_day_turnovers(startdate=datetime.now()):
    async with AiohttpClientSession() as session:
        dfAll = None
        fileName = f"{TRADING_PATH}/turnovers"
        if os.path.exists(f"{fileName}.csv"):
            with open(f"{fileName}.csv", "r") as f:
                try:
                    dfAll = pd.read_csv(f, index_col=0)
                    # dfAll = dfAll[['NAME', 'ID', 'VALTODAY', 'VALTODAY_USD', 'NUMTRADES', 'UPDATETIME', 'TITLE']]
                except:
                    pass

        iis_gets_async = []
        for i in range((datetime.now() - startdate).days):
            date = startdate + timedelta(days=i)
            s_date = datetime.strftime(date, "%Y-%m-%d")
            if not dfAll is None and (datetime.now() - date).days > 1:
                if len(dfAll.loc[[not v[:10] == s_date for v in dfAll['UPDATETIME'].values]]) > 0:
                    continue
            request_url = f"{MOEX_ISS_URL}/iss/turnovers.json?date={s_date}&land=ru"
            iis_gets_async += [aiomoex.ISSClient(session, request_url).get()]

        for iis_get_async in iis_gets_async:
            data = await iis_get_async
            df = pd.DataFrame(data['turnovers'])
            # df = df[['NAME', 'ID', 'VALTODAY', 'VALTODAY_USD', 'NUMTRADES', 'UPDATETIME', 'TITLE']]

            if not dfAll is None:
                dfAll = dfAll.loc[[not v[:10] in (d[:10] for d in df['UPDATETIME'].values) for v in dfAll['UPDATETIME'].values]]
                dfAll = dfAll.append(df)
            else:
                dfAll = df

        dfAll.sort_values(by=['UPDATETIME', 'NAME'])

        with open(f"{fileName}.csv", "w+") as f:
            f.write(dfAll.to_csv())
        with open(f"{fileName}.txt", "w+") as f:
            f.write(dfAll.to_string())


async def last_day_aggregates(security, startdate=datetime.now()):
    async with AiohttpClientSession() as session:
        dfAll = None
        fileName = f"{TRADING_PATH}/aggregates_{security}"
        if os.path.exists(f"{fileName}.csv"):
            with open(f"{fileName}.csv", "r") as f:
                try:
                    dfAll = pd.read_csv(f, index_col=0)
                    # dfAll = dfAll[['market_name', 'market_title', 'engine', 'tradedate', 'secid', 'value', 'volume', 'numtrades', 'updated_at']]
                except:
                    pass

        iis_gets_async = []
        for i in range((datetime.now() - startdate).days):
            date = startdate + timedelta(days=i)
            s_date = datetime.strftime(date, "%Y-%m-%d")
            if not dfAll is None and (datetime.now() - date).days > 1:
                if len(dfAll.loc[dfAll['tradedate'] == s_date]) > 0:
                    continue

            request_url = f"{MOEX_ISS_URL}/iss/securities/{security}/aggregates.json?date={s_date}&land=ru"
            iis_gets_async += [aiomoex.ISSClient(session, request_url).get()]

        for iis_get_async in iis_gets_async:
            data = await iis_get_async
            df = pd.DataFrame(data['aggregates'])
            # df = df[['market_name', 'market_title', 'engine', 'tradedate', 'secid', 'value', 'volume', 'numtrades', 'updated_at']]

            if not dfAll is None:
                dfAll = dfAll.loc[[not v in df['tradedate'].values for v in dfAll['tradedate'].values]]
                dfAll = dfAll.append(df)
            else:
                dfAll = df

        dfAll.sort_values(by=['tradedate', 'secid', 'market_name'])

        with open(f"{fileName}.csv", "w+") as f:
            f.write(dfAll.to_csv())
        with open(f"{fileName}.txt", "w+") as f:
            f.write(dfAll.to_string())


def extractDayResults(startdate):
    os.makedirs(TRADING_PATH, exist_ok=True)

    asyncio.run(last_day_turnovers(startdate=startdate))

    with open(f"{COMMON_INFO_PATH}/securities.csv", "r") as sec_f:
        dfAll = pd.read_csv(sec_f, index_col=0)
        dfAll = dfAll['secid']
        dfAll = dfAll.drop_duplicates()
        for secid in dfAll.values:
            print(f"last_day_aggregates for {secid}")
            asyncio.run(last_day_aggregates(security=secid, startdate=startdate))


if __name__ == "__main__":
    extractDayResults(datetime.now() - timedelta(days=3))
    os.makedirs(TRADING_PATH, exist_ok=True)
    asyncio.run(last_day_turnovers(startdate=datetime.now() - timedelta(days=3)))
    asyncio.run(last_day_aggregates(security="SBER", startdate=datetime.now() - timedelta(days=3)))

    pass
