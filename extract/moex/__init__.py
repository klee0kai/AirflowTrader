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

logging.basicConfig(level=logging.DEBUG)

MOEX_ISS_URL = "https://iss.moex.com"
COMMON_INFO_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/common")
# COMMON_INFO_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "common/moex")
TRADING_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/trading")

sem = asyncio.Semaphore(30)


class AiohttpClientSession(aiohttp.ClientSession):
    async def _request(self, method, url, **kwargs):
        async with sem:
            print(f"aiohttp  {method} {url}")
            result = await super()._request(method, url, **kwargs)
            if result.status != 200:
                print(f"aiohttp  {method} {url} status {result.status} {await result.text()}")

            return result
