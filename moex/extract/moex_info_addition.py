import asyncio

from moex import *

logging.basicConfig(level=logging.DEBUG)

# extract common moex info:
#     instruments
#     engines
#     markets


START_STATE_PATH = f"{COMMON_MOEX_PATH}/start_state.log"

UPDATE_INTERVAL = None
IS_AIRFLOW = False

MAX_ITERATION = 1000  # чтоб не использовать while true


async def extractMoexSecuritiesAsync():
    markets_df = loadDataFrame(f"{COMMON_MOEX_PATH}/markets")
    markets_df =markets_df[['trade_engine_name','market_name']]

    for i, r in markets_df.iterrows():
        engine = r['trade_engine_name']
        market = r['market_name']
        async with AiohttpMoexClientSession() as session:
            request_url = f"{MOEX_ISS_URL}/iss/engines/{engine}/markets/{market}/securities.json?land=ru"
            iss = aiomoex.ISSClient(session, request_url)
            data = await iss.get()
            df = pd.DataFrame(data['securities'])
            saveDataFrame(df, f"{COMMON_MOEX_PATH}/securities_{engine}_{market}")


def extractMoexAdditionalInfo(interval=None, airflow=False):
    print(f"extractMoexAdditionalInfo interval interval = {interval} airflow = {airflow}")
    os.makedirs(COMMON_MOEX_PATH, exist_ok=True)
    global IS_AIRFLOW, UPDATE_INTERVAL
    UPDATE_INTERVAL = interval
    IS_AIRFLOW = airflow
    skip_flag = False
    start_state = {}
    if airflow and not interval is None and os.path.exists(START_STATE_PATH):
        with open(START_STATE_PATH, "r") as f:
            start_state = json.loads(f.read())
            if 'end_additional' in start_state and datetime.utcnow() < datetime.fromisoformat(start_state['end_additional']) + interval:
                print("extractMoexAdditionalInfo loaded - skip_flag = True")
                skip_flag = True

    start_state['start_additional'] = datetime.utcnow()

    if not skip_flag:
        print(f"extract moex securities {COMMON_MOEX_PATH}")
        asyncio.run(extractMoexSecuritiesAsync())

    if not skip_flag:
        start_state['end_additional'] = datetime.utcnow()
        with open(START_STATE_PATH, "w", encoding='utf-8') as f:
            f.write(json.dumps(start_state, default=json_serial))

    chmodForAll(COMMON_MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    asyncio.run(extractMoexSecuritiesAsync())
