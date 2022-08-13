from aiohttp import BasicAuth
import base64
import cryptography
from cryptography.fernet import Fernet

import os, sys

fernet = Fernet(b'null')

AIRFLOW_DATA_PATH = "/mnt/ai_data/airflow_data/trader"

MOEX_AUTH = BasicAuth(login='smoll9521@gmail.com',
                      password=fernet.decrypt(b'gAAAAABg3hpIHJaanhYIqFIQXZy9txq9HIpjAzChyszRQH5brTXVIaJbQj5j2CnzuoK2AQsgOtXK7hDVrC7bmlKoL-jYyqPhTA==').decode('utf-8'))

TELEGRAM_BOT_TOKEN_DEBUG = fernet.decrypt(b'gAAAAABg3h3e1C997rIGLY0nJwCoSZkFBOHPvJ_WJev9cjXgj9QOUuCeUVvPoTvTvWl6stHUCbdCUH8I0y_-BeI5yTyRk5ructMAZ_Z5EoxVDtCGyVpKrnHPycks87IsxE2BGVhfoyxI').decode('utf-8')
TELEGRAM_BOT_TOKEN_RELEASE = fernet.decrypt(b'gAAAAABg3hoBl9Z5ewr7EoV9RV9sUUqO7Ztck0UAJnang7IlKWjaTXv-FNwZBRnbqK1yPQHhBbeSdnxq5UxaAIil2UDaPtvGPidYxrCVzv_Yxl9BuTLJQ2AYB3G1Clo5yBK3rFae-uyl').decode('utf-8')
