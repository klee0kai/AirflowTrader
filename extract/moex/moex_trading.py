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
        columns = ['NAME', 'ID', 'VALTODAY', 'VALTODAY_USD', 'NUMTRADES', 'UPDATETIME', 'TITLE']
        dfAll = loadDataFrame(fileName)
        if not dfAll is None:
            dfAll = dfAll[columns]

        iis_gets_async = []
        s_dates = []
        for i in range((datetime.now() - startdate).days):
            date = startdate + timedelta(days=i)
            s_date = datetime.strftime(date, "%Y-%m-%d")
            if not dfAll is None and (datetime.now() - date).days > 1:
                if len(dfAll.loc[[v[:10] == s_date for v in dfAll['UPDATETIME'].values]]) > 0:
                    continue
            request_url = f"{MOEX_ISS_URL}/iss/turnovers.json?date={s_date}&land=ru"
            iis_gets_async += [aiomoex.ISSClient(session, request_url).get()]
            s_dates += [s_date]

        for i, iis_get_async in enumerate(iis_gets_async):
            data = await iis_get_async
            df = pd.DataFrame(data['turnovers'])
            if df.empty:
                df = pd.DataFrame(data={'NAME': ['null'], 'ID': ['null'], 'VALTODAY': ['null'], 'VALTODAY_USD': ['null'],
                                        'NUMTRADES': ['null'], 'UPDATETIME': [s_dates[i]], 'TITLE': ['null']})

            df = df[columns]
            # print(f"loaded turnovers:  {len(df)}")

            if not dfAll is None:
                dfAll = dfAll.loc[[not v[:10] in (d[:10] for d in df['UPDATETIME'].values) for v in dfAll['UPDATETIME'].values]]
                dfAll = dfAll.append(df)
            else:
                dfAll = df

        dfAll = dfAll.sort_values(by=['UPDATETIME', 'NAME'])

        saveDataFrame(dfAll, fileName)


async def last_day_aggregates(security, startdate=datetime.now()):
    async with AiohttpClientSession() as session:
        dfAll = None
        fileName = f"{TRADING_PATH}/aggregates_{security}"
        columns = ['market_name', 'market_title', 'engine', 'tradedate', 'secid', 'value', 'volume', 'numtrades', 'updated_at']
        dfAll = loadDataFrame(fileName)
        if not dfAll is None:
            dfAll = dfAll[columns]

        iis_gets_async = []
        s_dates = []
        for i in range((datetime.now() - startdate).days):
            date = startdate + timedelta(days=i)
            s_date = datetime.strftime(date, "%Y-%m-%d")
            if not dfAll is None and (datetime.now() - date).days > 1:
                if len(dfAll.loc[dfAll['tradedate'] == s_date]) > 0:
                    continue

            request_url = f"{MOEX_ISS_URL}/iss/securities/{security}/aggregates.json?date={s_date}&land=ru"
            iis_gets_async += [aiomoex.ISSClient(session, request_url).get()]
            s_dates += [s_date]

        for i, iis_get_async in enumerate(iis_gets_async):
            data = await iis_get_async
            df = pd.DataFrame(data['aggregates'])
            if df.empty:
                df = pd.DataFrame(data={'market_name': ['null'], 'market_title': ['null'], 'engine': ['null'], 'tradedate': [s_dates[i]],
                                        'secid': [security], 'value': ['null'], 'volume': ['null'], 'numtrades': ['null'], 'updated_at': [s_dates[i]]})

            # print(f"loaded aggregates {security}: {len(df)}")
            df = df[columns]

            if not dfAll is None:
                dfAll = dfAll.loc[[not v in df['tradedate'].values for v in dfAll['tradedate'].values]]
                dfAll = dfAll.append(df)
            else:
                dfAll = df

        dfAll = dfAll.sort_values(by=['tradedate', 'secid', 'market_name'])

        saveDataFrame(dfAll, fileName)


def extractDayResults(startdate):
    print(f"extractDayResults startdate = {datetime.strftime(startdate, '%Y-%m-%d')} ")
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
    # extractDayResults(datetime.now() - timedelta(days=3))
    os.makedirs(TRADING_PATH, exist_ok=True)
    asyncio.run(last_day_turnovers(startdate=datetime.now() - timedelta(days=8)))
    # asyncio.run(last_day_aggregates(security="SBER", startdate=datetime.now() - timedelta(days=3)))

    pass
