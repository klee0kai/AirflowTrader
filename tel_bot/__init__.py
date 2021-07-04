import asyncio
import logging
import os, sys
import shutil
from functools import wraps

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
import threading
import queue
import time
import logging
from cmath import nan

import telegram, telegram.ext, telegram.ext.utils, telegram.ext.utils.types
from telegram import ParseMode, InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackQueryHandler,
    Filters,
    CallbackContext,
    MessageHandler,
    TypeHandler
)

# Enable logging
from telegram.utils import helpers

TELEGRAM_BOT_PATH = os.path.join(configs.AIRFLOW_DATA_PATH, "telegram")

import tel_bot.telegram_rep as rep


def wrap_telegram_handler(func):
    @wraps(func)
    def wrapped(update, context, *args, **kwargs):
        print(f"{update.effective_user.username}: {update.message.text if 'text' in dir(update.message) else ''} ")
        user_id = update.effective_user.id
        user = rep.getUser(user_id)

        welcomeText = f"Привет {update.effective_user.first_name}! Для работы с ботом напиши @andreykuzubov. Веедите /help для помощи."
        if user is None:
            s = pd.Series(data={"id": update.effective_user.id, "login": update.effective_user.username, "username": update.effective_user.first_name + " " + update.effective_user.last_name})
            rep.setUser(s)
            update.message.reply_text(welcomeText)
            return
        if user['roles'] is None or user['roles'] == 'null' and user_id != rep.OWNER_ID:
            update.message.reply_text(welcomeText)
            return

        user['last_message_date'] = datetime.now().strftime("%Y-%m-%d %H:%M")
        rep.setUser(user)
        return func(update, context, *args, **kwargs)

    return wrapped


def wrap_restricted_byrole(role):
    def decorator(func):
        @wraps(func)
        def wrapped(update, context, *args, **kwargs):
            user_id = update.effective_user.id
            if not rep.isUserRole(user_id, role):
                print(f"Unauthorized access denied for {update.effective_user.username}")
                update.message.reply_text("Доступ не предоставлен, напиши @andreykuzubov")
                return
            return func(update, context, *args, **kwargs)

        return wrapped

    return decorator
