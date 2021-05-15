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
logging.basicConfig(level=logging.DEBUG)

# extract common moex info:
#     instruments
#     engines
#     markets

COMMON_INFO_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "common/moex")
MOEX_ISS_URL = "https://iss.moex.com"

async def extractMoexInfoAsync():
    request_url = f"{MOEX_ISS_URL}/iss.json"
    async with aiohttp.ClientSession() as session:
        iss = aiomoex.ISSClient(session, request_url)
        data = await iss.get()
        for key in data.keys():
            df = pd.DataFrame(data[key])
            with open(f"{COMMON_INFO_PATH}/{key}.csv", "w") as f:
                f.write(df.to_csv())
                print(f"created {f.name}")
            with open(f"{COMMON_INFO_PATH}/{key}.txt", "w") as f:
                f.write(df.to_string())
                print(f"created {f.name}")


async def extractMoexSecuritiesAsync():
    start = 0
    async with aiohttp.ClientSession() as session:
        f_csv = open(f"{COMMON_INFO_PATH}/securities.csv", "w")
        f_txt = open(f"{COMMON_INFO_PATH}/securities.txt", "w")

        while True:
            request_url = f"{MOEX_ISS_URL}/iss/securities.json?start={start}&land=ru"
            iss = aiomoex.ISSClient(session, request_url)
            data = await iss.get()
            df = pd.DataFrame(data['securities'])
            if len(df) <= 0:
                break
            start += len(df)
            f_csv.write(df.to_csv())
            f_txt.write(df.to_string())
        print(f"created {f_csv.name} and {f_txt.name}")


def extractMoexAllCommonInfo():
    # shutil.rmtree(COMMON_INFO_PATH, ignore_errors=True)
    os.makedirs(COMMON_INFO_PATH, exist_ok=True)

    print(f"extract moex info to {COMMON_INFO_PATH}")
    asyncio.run(extractMoexInfoAsync())
    asyncio.run(extractMoexSecuritiesAsync())
    pass


if __name__ == "__main__":
    extractMoexAllCommonInfo()
