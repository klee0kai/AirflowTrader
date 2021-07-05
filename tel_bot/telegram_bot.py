import asyncio

import telegram

from configs import *
from tel_bot import *
import utils.threads_utils as threads_utils
import tel_bot.telegram_rep as rep

bot = None


def initBot(token):
    global bot
    bot = telegram.Bot(token)


def sendMessageToUser(userId, message):
    if bot is None:
        return
    bot.send_message(userId, message)


def sendSecPredictInfo(sec, message):
    # todo
    pass


def sendMessage(forRole, message):
    df_users = rep.getUsers()
    if df_users is None:
        return
    df_users = df_users[[forRole in x.roles or x.id == rep.OWNER_ID for x in df_users.itertuples()]]

    if len(df_users) <= 0:
        return
    for u in df_users.itertuples():
        secondThread = threading.Thread(target=sendMessageToUser, args=(u.id, message,))
        secondThread.start()
        threads_utils.collect_thread(secondThread)


if __name__ == "__main__":
    initBot(TELEGRAM_BOT_TOKEN_DEBUG)
    sendMessage(rep.ROLE_OWNER, "test messs")
