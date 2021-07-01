from configs import TELEGRAM_BOT_TOKEN_RELEASE
from tel_bot import *
from utils.dateframes import *

ROLES = [
    'admin',
    'user1',  # взамодействие по всем стратегиям
    'user2',  # стратегии кроме нейронок
    'user3',  # только по скольщящим
    None,  # non  authorized
]

SEC_COMMON = 'COM_SEC'  # группа общих инструментов (на выбор администратора)

OWNER_ID = 185592855

DF_NULL = 'null'


def getUsers():
    df = loadDataFrame(f"{TELEGRAM_BOT_PATH}/users")
    if df is None:
        df = pd.DataFrame()
    df = pd.DataFrame(df, columns=['id', 'login', 'username', 'roles', 'following_sec', 'last_message_date'])
    df = df.fillna(value=DF_NULL)
    return df


def saveUsers(df):
    return saveDataFrame(df, f"{TELEGRAM_BOT_PATH}/users")


def getUser(userId):
    df = getUsers()
    df = df.loc[df['id'] == userId]
    return df.iloc[-1] if len(df) > 0 else None


def touchUser(user):  # series
    df = getUsers()
    if len(df[df['id'] == user['id']]) > 0:
        return
    df = df.append(user, ignore_index=True)
    saveUsers(df)


def commandsFor(userId):
    commands = ['/help', '/send', '/track', '/untrack', '/list', '/ping']
    return commands


def getHelp(userId=None):
    for filename in ['help.txt', 'tel_bot/help.txt']:
        with open(filename, 'r') as f:
            return f.read()
