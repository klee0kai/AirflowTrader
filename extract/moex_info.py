import os, sys
import shutil

import configs
import os.path

# extract common moex info:
#     instruments
#     engines
#     markets

MOEX_INFO_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex_info")


def extractMoexInfo():
    shutil.rmtree(MOEX_INFO_PATH, ignore_errors=True)
    os.makedirs(MOEX_INFO_PATH, exist_ok=True)

    print("extractMoexInfo  ")
    pass


if __name__ == "__main__":
    extractMoexInfo()
