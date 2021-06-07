
from moex import *

logging.basicConfig(level=logging.DEBUG)

# extract common moex info:
#     instruments
#     engines
#     markets


START_STATE_PATH = f"{COMMON_INFO_PATH}/start_state.log"

UPDATE_INTERVAL = None
IS_AIRFLOW = False

MAX_ITERATION = 1000  # чтоб не использовать while true


async def extractMoexInfoAsync():
    request_url = f"{MOEX_ISS_URL}/iss.json"
    async with AiohttpClientSession() as session:
        iss = aiomoex.ISSClient(session, request_url)
        data = await iss.get()
        for key in data.keys():
            df = pd.DataFrame(data[key])
            with open(f"{COMMON_INFO_PATH}/{key}.csv", "w") as f:
                f.write(df.to_csv())
                print(f"created {f.name}")
            with open(f"{COMMON_INFO_PATH}/{key}.txt", "w") as f:
                f.write(df.to_string())
                print(f"created {f.name}")


async def extractMoexSecuritiesAsync():
    start = 0
    async with AiohttpClientSession() as session:
        f_csv = open(f"{COMMON_INFO_PATH}/securities.csv", "w")
        f_txt = open(f"{COMMON_INFO_PATH}/securities.txt", "w")

        for ia in range(0, MAX_ITERATION):  #
            request_url = f"{MOEX_ISS_URL}/iss/securities.json?start={start}&land=ru"
            iss = aiomoex.ISSClient(session, request_url)
            data = await iss.get()
            df = pd.DataFrame(data['securities'])
            if len(df) <= 0:
                break
            start += len(df)
            f_csv.write(df.to_csv())
            f_txt.write(df.to_string())
        f_csv.close()
        f_txt.close()
        print(f"created {f_csv.name} and {f_txt.name}")

    markets = [  # (engine , market )
        ('stock', 'shares'),
        ('currency', 'selt'),
        ('currency', 'otc'),
    ]

    for engine, market in markets:
        async with AiohttpClientSession() as session:
            f_csv = open(f"{COMMON_INFO_PATH}/securities_{engine}_{market}.csv", "w")
            f_txt = open(f"{COMMON_INFO_PATH}/securities_{engine}_{market}.txt", "w")
            request_url = f"{MOEX_ISS_URL}/iss/engines/{engine}/markets/{market}/securities.json?land=ru"
            # request_url = f"{MOEX_ISS_URL}/iss/securities.json?start=0&land=ru"
            iss = aiomoex.ISSClient(session, request_url)
            data = await iss.get()
            df = pd.DataFrame(data['securities'])
            start += len(df)
            f_csv.write(df.to_csv())
            f_txt.write(df.to_string())
            f_csv.close()
            f_txt.close()
            print(f"created {f_csv.name} and {f_txt.name}")


def extractMoexAllCommonInfo(interval=None, airflow=False):
    print(f"extractMoexAllCommonInfo interval interval = {interval} airflow = {airflow}")
    os.makedirs(COMMON_INFO_PATH, exist_ok=True)

    global IS_AIRFLOW, UPDATE_INTERVAL
    UPDATE_INTERVAL = interval
    IS_AIRFLOW = airflow

    if airflow and not interval is None and os.path.exists(START_STATE_PATH):
        with open(START_STATE_PATH, "r") as f:
            start_state = json.loads(f.read())
            if 'end' in start_state and datetime.utcnow() < datetime.fromisoformat(start_state['end']) + interval:
                print("extractMoexAllCommonInfo loaded")
                return
                # raise AirflowSkipException()

    start_state = {'start': datetime.utcnow()}

    print(f"extract moex info to {COMMON_INFO_PATH}")
    asyncio.run(extractMoexInfoAsync())
    asyncio.run(extractMoexSecuritiesAsync())

    start_state['end'] = datetime.utcnow()
    with open(START_STATE_PATH, "w", encoding='utf-8') as f:
        f.write(json.dumps(start_state, default=json_serial))

    chmodForAll(COMMON_INFO_PATH, 0x777, 0o666)


if __name__ == "__main__":
    extractMoexAllCommonInfo()
    # extractMoexAllCommonInfo(interval=timedelta(days=14), airflow=True)
