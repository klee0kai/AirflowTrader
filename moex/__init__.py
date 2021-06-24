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
import utils.inet
import matplotlib
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.DEBUG)

MOEX_ISS_URL = "https://iss.moex.com"

MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex")

API_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/api")
COMMON_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/common")
HIST_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/hits")
TRANSFORM_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/transform")
HIST_TRANSFORM1_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/hits/transform1")
HIST_INDICATORS_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/hits/indicators")

MAX_ITERATION = 1000  # чтоб не использовать while true
sem = asyncio.Semaphore(10)


class AiohttpClientSession(aiohttp.ClientSession):

    async def _request(self, method, url, **kwargs):
        async with sem:
            print(f"aiohttp  {method} {url}")
            result = await super()._request(method, url, **kwargs)
            if result.status != 200:
                print(f"aiohttp  {method} {url} status {result.status} {await result.text()}")

            return result


class AiohttpMoexClientSession(AiohttpClientSession):

    def __init__(self, **kwargs):
        super().__init__(auth=authconfig.MOEX_AUTH, headers=inet.gen_headers(False), **kwargs)


def isDataframeExist(fileName):
    return os.path.exists(f"{fileName}.csv")


def loadDataFrame(fileName):
    if os.path.exists(f"{fileName}.csv"):
        with open(f"{fileName}.csv", "r") as f:
            try:
                df = pd.read_csv(f, index_col=0)
                df = df.reset_index(drop=True)
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
            df.columns = df.columns.map(lambda x: x.lower())
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
