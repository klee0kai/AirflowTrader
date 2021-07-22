import asyncio

import telegram

from configs import *
from tel_bot import *
import tel_bot.telegram_rep as rep

bot = None


def initBot(token, force=False):
    global bot
    if force or bot is None:
        bot = telegram.Bot(token)


def sendMessageToUser(userId, message):
    if bot is None:
        return
    bot.send_message(userId, message, parse_mode=telegram.ParseMode.HTML)


def sendSecPredictInfo(sec, message):
    df_users = rep.getUsers()
    if df_users is None:
        return
    df_users = df_users[[sec.lower() in x.following_sec.lower() and (rep.ROLE_TRACK in x.roles or x.id == rep.OWNER_ID) for x in df_users.itertuples()]]
    if len(df_users) <= 0:
        return
    for u in df_users.itertuples():
        bot.send_message(u.id, message, parse_mode=telegram.ParseMode.HTML)


def sendMessage(forRole, message):
    df_users = rep.getUsers()
    if df_users is None:
        return
    df_users = df_users[[forRole in x.roles or x.id == rep.OWNER_ID for x in df_users.itertuples()]]

    if len(df_users) <= 0:
        return
    for u in df_users.itertuples():
        bot.send_message(u.id, message, parse_mode=telegram.ParseMode.HTML)


if __name__ == "__main__":
    initBot(TELEGRAM_BOT_TOKEN_DEBUG)
    sendMessage(rep.ROLE_OWNER, "<b>title</b>\n"
                                "<i>subtitle</i>\n"
                                "test messs\n"
                                '<a href="http://www.example.com">link</a>')
