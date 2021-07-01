import numpy as np
import pandas as pd

from configs import *
from tel_bot import *
import tel_bot.TelegramRep as rep


def startBotServer(token):
    try:
        os.makedirs(TELEGRAM_BOT_PATH, exist_ok=True)
        chmodForAll(TELEGRAM_BOT_PATH, 0o777, 0o666)
    except:
        pass

    bot = telebot.TeleBot(token)

    print(f"bot starting {bot.get_me().first_name} : {bot.get_me().username}")

    @bot.message_handler(commands=["send"])
    def telegramGetAccountDetails(message):
        bot.send_message(message.from_user.id, "service in work")

    @bot.message_handler(commands=["track"])
    def telegramGetAccountDetails(message):
        bot.send_message(message.from_user.id, "service in work")

    @bot.message_handler(commands=["untrack"])
    def telegramGetAccountDetails(message):
        bot.send_message(message.from_user.id, "service in work")

    @bot.message_handler(commands=["list"])
    def telegramGetAccountDetails(message):
        bot.send_message(message.from_user.id, "service in work")

    @bot.message_handler(commands=["ping"])
    def telegramGetAccountDetails(message):
        bot.send_message(message.from_user.id, "Сервис запущен")

    @bot.message_handler(commands=["start"])
    def telegramGetAccountDetails(message):
        bot.send_message(message.from_user.id, f"Привет {message.from_user.first_name}! Для работы с ботом напиши @andreykuzubov. Веедите /help для помощи.")

    @bot.message_handler(commands=["help"])
    def telegramGetAccountDetails(message):
        markup = types.ReplyKeyboardMarkup()
        for c in rep.commandsFor(message.from_user.id):
            markup.add(c)
        bot.send_message(message.from_user.id, rep.getHelp(), reply_markup=markup)

    @bot.message_handler(content_types=["text"])
    def telegramGetAccountDetails(message):
        user = rep.getUser(message.from_user.id)
        welcomeText = f"Привет {message.from_user.first_name}! Для работы с ботом напиши @andreykuzubov. Веедите /help для помощи."
        if user is None:
            s = pd.Series(data={"id": message.from_user.id, "login": message.from_user.username, "username": message.from_user.first_name + " " + message.from_user.last_name})
            rep.touchUser(s)
            bot.send_message(message.from_user.id, welcomeText)
            return
        if user['roles'] is None or user['roles'] == 'null':
            bot.send_message(message.from_user.id, welcomeText)
            return

        bot.send_message(message.from_user.id, f"{message.from_user.first_name}, используй команды для работы (смотри /help)", )

    bot.infinity_polling()


if __name__ == "__main__":
    startBotServer(TELEGRAM_BOT_TOKEN_DEBUG)
