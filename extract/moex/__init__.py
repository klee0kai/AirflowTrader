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

MOEX_ISS_URL = "https://iss.moex.com"
COMMON_INFO_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "moex/common")