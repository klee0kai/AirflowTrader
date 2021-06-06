import asyncio
import logging
import os, sys
import shutil

import authconfig
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

logging.basicConfig(level=logging.DEBUG)

MOEX_ISS_URL = "https://iss.moex.com"
COMMON_INFO_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/common")
TRADING_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/trading")

sem = asyncio.Semaphore(200)


class AiohttpClientSession(aiohttp.ClientSession):

    def __init__(self, **kwargs):
        super().__init__(auth=authconfig.MOEX_AUTH, **kwargs)

    async def _request(self, method, url, **kwargs):
        async with sem:
            print(f"aiohttp  {method} {url}")
            result = await super()._request(method, url, **kwargs)
            if result.status != 200:
                print(f"aiohttp  {method} {url} status {result.status} {await result.text()}")

            return result


def loadDataFrame(fileName):
    if os.path.exists(f"{fileName}.csv"):
        with open(f"{fileName}.csv", "r") as f:
            try:
                df = pd.read_csv(f, index_col=0)
                return df
            except:
                pass


def saveDataFrame(df, fileName):
    olddf = loadDataFrame(fileName)
    try:
        if not olddf is None and len(olddf) <= len(df) and olddf.compare(df[:len(olddf)]).empty:
            if len(olddf) == len(df):
                # ignore (no changes)
                return

            print(f"append df to {fileName}.csv")
            appenddf = df[len(olddf):]
            with open(f"{fileName}.csv", "a") as f:
                f.write(appenddf.to_csv(header=False))
            with open(f"{fileName}.txt", "a") as f:
                f.write('\n')
                f.write(appenddf.to_string(header=False))
            return
    except:
        pass

    print(f"safe df to {fileName}.csv")
    # else
    with open(f"{fileName}.csv", "w+") as f:
        f.write(df.to_csv())
    with open(f"{fileName}.txt", "w+") as f:
        f.write(df.to_string())
