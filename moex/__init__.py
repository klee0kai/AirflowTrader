import asyncio
import logging
import os, sys
import shutil

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
from utils.dateframes import *

logging.basicConfig(level=logging.DEBUG)

MOEX_ISS_URL = "https://iss.moex.com"

MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex")

API_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/api")
COMMON_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/common")
HIST_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/hits")
TRANSFORM_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/transform")
HIST_TRANSFORM1_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/hits/transform1")
HIST_INDICATORS_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/hits/indicators")
DAILY_STRATEGY_MOEX_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/daily")
DAILY_STRATEGY__MOEX_TEST_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/test/daily")

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
        super().__init__(auth=configs.MOEX_AUTH, headers=inet.gen_headers(False), **kwargs)


def isMoexWorkTime():
    return not datetime.now().isoweekday() in (6, 7)
