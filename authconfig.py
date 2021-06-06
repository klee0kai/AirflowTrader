from aiohttp import BasicAuth
import base64

MOEX_AUTH = BasicAuth(login='smoll9521@gmail.com', password=base64.b64decode('dWVGOFhFRG0='))
