import numpy as np
import pandas as pd
import telegram.ext

from configs import *
import tel_bot
from tel_bot import *
import moex
import glob
import tel_bot.telegram_bot as bot_sender
import moex.post_load.security_daily_predicts as daily_predicts

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)

CALLBACK_SEND_ALL = 'callback_nd_all'
CALLBACK_SEND_ALL_ANON = 'callback_send_all_anon'
CALLBACK_SEND_ADMIN = 'callback_send_admin'
CALLBACK_CONFIRM_NOTRACK = 'callback_notrack'

ASK_REPLY__SEND_ALL = "Отправить всем:"
ASK_REPLY__SEND_ALL_ANON = "Отправить всем (анонимно):"
ASK_REPLY__SEND_ADMIN = "Отправить админу:"
ASK_REPLY__ADD_SECURITIES = "Отслеживать инструменты:"
ASK_REPLY__REMOVE_SECURITIES = "Не отслеживать инструменты:"
ASK_REPLY__TODAY_SECURITIES = "Сегодняшняя информация по инструментам:"


@wrap_telegram_handler
def start_handler(update: Update, context: CallbackContext):
    update.message.reply_text("Добро пожаловать к торговому боту от @andrekuzubov. Смотрите /help, чтобы узнать, что тут есть.")


@wrap_telegram_handler
@wrap_restricted_byrole(role=rep.ROLE_TRACK)
def track_handler(update: Update, context: CallbackContext):
    if update.message.text == "/track":
        update.message.reply_text(ASK_REPLY__ADD_SECURITIES, reply_markup=telegram.ForceReply())
    if update.message.text == "/untrack":
        update.message.reply_text(ASK_REPLY__REMOVE_SECURITIES, reply_markup=telegram.ForceReply())
    if update.message.text == "/today":
        update.message.reply_text(ASK_REPLY__TODAY_SECURITIES, reply_markup=telegram.ForceReply())
    if update.message.text == "/notrack":
        update.message.reply_text("Вы хотите отказаться от отслеживания инструментов:", reply_markup=telegram.InlineKeyboardMarkup([[
            telegram.InlineKeyboardButton(text="Да, сбросить отслеживаемые инструменты", callback_data=CALLBACK_CONFIRM_NOTRACK),
        ]], one_time_keyboard=True, resize_keyboard=True))


@wrap_telegram_handler
def me_handler(update: Update, context: CallbackContext):
    user = rep.getUser(update.effective_user.id)
    me_info = ""
    if rep.ROLE_TRACK in user['roles']:
        me_info += f"Отслеживаемые инструменты: {user['following_sec']}. "
    if rep.ROLE_BEST_PREDICTS in user['roles']:
        me_info += f"Вам присылается ежедневная аналитика по перспективным иструментам. "

    update.message.reply_text(me_info)


@wrap_telegram_handler
def send_handler(update: Update, context: CallbackContext):
    update.message.reply_text("Кому хотите отправить сообщения:", reply_markup=telegram.InlineKeyboardMarkup([[
        telegram.InlineKeyboardButton(text="Администратору", callback_data=CALLBACK_SEND_ADMIN),
        telegram.InlineKeyboardButton(text="Всем", callback_data=CALLBACK_SEND_ALL),
        telegram.InlineKeyboardButton(text="Всем (анонимно)", callback_data=CALLBACK_SEND_ALL_ANON),
    ]], one_time_keyboard=True, resize_keyboard=True))

    pass


@wrap_telegram_handler
def callback_handler(update: Update, context: CallbackContext):
    if update.callback_query.data == CALLBACK_SEND_ADMIN:
        update.callback_query.message.reply_text(ASK_REPLY__SEND_ADMIN, reply_markup=telegram.ForceReply())
        pass
    elif update.callback_query.data == CALLBACK_SEND_ALL:
        update.callback_query.message.reply_text(ASK_REPLY__SEND_ALL, reply_markup=telegram.ForceReply())
        pass
    elif update.callback_query.data == CALLBACK_SEND_ALL_ANON:
        update.callback_query.message.reply_text(ASK_REPLY__SEND_ALL_ANON, reply_markup=telegram.ForceReply())
        pass

    elif update.callback_query.data == CALLBACK_CONFIRM_NOTRACK:
        u = rep.getUser(update.effective_user.id)
        u['following_sec'] = rep.DF_NULL
        rep.setUser(u)
        update.callback_query.message.reply_text(f"Успешно сброшено слежение за инструментами")


@wrap_telegram_handler
def reply_handler(update: Update, context: CallbackContext):
    if update.message.reply_to_message.text == ASK_REPLY__SEND_ALL:
        send_text = f"Общее сообщение от {update.effective_user.name} : {update.message.text}"
        for u in rep.getUsers().itertuples():
            if not rep.DF_NULL in u.roles and not u.roles is None:
                context.bot.send_message(u.id, send_text)
        pass
    elif update.message.reply_to_message.text == ASK_REPLY__SEND_ALL_ANON:
        send_text = f"Общее сообщение от анонима : {update.message.text}"
        for u in rep.getUsers().itertuples():
            if not rep.DF_NULL in u.roles and not u.roles is None:
                context.bot.send_message(u.id, send_text)
    elif update.message.reply_to_message.text == ASK_REPLY__SEND_ADMIN:
        send_text = f"Общее сообщение от {update.effective_user.name} : {update.message.text}"
        for u in rep.getUsers().itertuples():
            if rep.ROLE_ADMIN in u.roles:
                context.bot.send_message(u.id, send_text)

    elif update.message.reply_to_message.text == ASK_REPLY__ADD_SECURITIES:
        if not rep.isUserRole(update.effective_user.id, rep.ROLE_TRACK):
            return
        u = rep.getUser(update.effective_user.id)
        message_secs = [s.replace(" ", "") for s in update.message.text.lower().split(",")]
        available_secs = [f[len(f"{moex.HIST_MOEX_PATH}/stock_shares_"):-len(".csv")].lower() for f in glob.glob(f"{moex.HIST_MOEX_PATH}/stock_shares_*.csv")]
        non_available_sec = [s for s in message_secs if not s in available_secs]
        message_available_sec = [s for s in message_secs if s in available_secs]
        userSecs = [f for f in u['following_sec'].split(' ') if not f == rep.DF_NULL]
        userSecs = set(userSecs + message_available_sec)
        u['following_sec'] = ' '.join(userSecs)
        rep.setUser(u)

        warningAppendText = ""
        if len(non_available_sec) > 0:
            warningAppendText = f"Следующие инструменты не поддерживаются: {', '.join(non_available_sec)}"
        update.message.reply_text(f"Список иструментов обновлен. {warningAppendText}")

    elif update.message.reply_to_message.text == ASK_REPLY__REMOVE_SECURITIES:
        if not rep.isUserRole(update.effective_user.id, rep.ROLE_TRACK):
            return
        u = rep.getUser(update.effective_user.id)
        message_secs = [s.replace(" ", "") for s in update.message.text.lower().split(",")]
        userSecs = [f for f in set(u['following_sec'].split(' ')) if not f == rep.DF_NULL]

        u['following_sec'] = ' '.join(set(userSecs) - set(message_secs))
        rep.setUser(u)
        update.message.reply_text(f"Список иструментов обновлен")
    elif update.message.reply_to_message.text == ASK_REPLY__TODAY_SECURITIES:
        if not rep.isUserRole(update.effective_user.id, rep.ROLE_TRACK):
            return
        message_secs = [s.replace(" ", "") for s in update.message.text.upper().split(",")]
        for sec in message_secs:
            daily_predicts.secDailyPredict(sec)


@wrap_telegram_handler
def message_handler(update: Update, context: CallbackContext):
    update.message.reply_text(f"{update.effective_user.first_name}, используй команды для работы (смотри /help)")


@wrap_telegram_handler
def help_handler(update: Update, context: CallbackContext):
    update.message.reply_text(rep.getHelp(update.effective_user.id))


@wrap_telegram_handler
def ping_handler(update: Update, context: CallbackContext):
    update.message.reply_text("Сервис работает.", reply_markup=telegram.ReplyKeyboardRemove())


def startBotServer(token):
    try:
        os.makedirs(TELEGRAM_BOT_PATH, exist_ok=True)
        chmodForAll(TELEGRAM_BOT_PATH, 0o777, 0o666)
    except:
        pass

    bot_sender.initBot(token, force=True)
    bot = telegram.Bot(token)
    updater = telegram.ext.Updater(token)
    dispatcher = updater.dispatcher

    print(f"bot starting {bot.get_me().first_name} : {bot.get_me().username}")

    dispatcher.add_handler(CommandHandler('start', start_handler))
    dispatcher.add_handler(CommandHandler('track', track_handler))
    dispatcher.add_handler(CommandHandler('untrack', track_handler))
    dispatcher.add_handler(CommandHandler('notrack', track_handler))
    dispatcher.add_handler(CommandHandler('today', track_handler))
    dispatcher.add_handler(CommandHandler('help', help_handler))
    dispatcher.add_handler(CommandHandler('ping', ping_handler))
    dispatcher.add_handler(CommandHandler('me', me_handler))
    dispatcher.add_handler(CommandHandler('send', send_handler))
    dispatcher.add_handler(MessageHandler(filters=telegram.ext.Filters.reply, callback=reply_handler))
    dispatcher.add_handler(MessageHandler(filters=telegram.ext.Filters.text, callback=message_handler))
    dispatcher.add_handler(CallbackQueryHandler(callback_handler))

    updater.start_polling()

    updater.idle()


if __name__ == "__main__":
    startBotServer(TELEGRAM_BOT_TOKEN_DEBUG)
