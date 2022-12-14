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


async def extractMoexInfoAsync():
    reqs = [
        (f"{MOEX_ISS_URL}/iss.json", ""),
        (f"{MOEX_ISS_URL}/iss/securitytypes.json", "securitytypes_"),
        (f"{MOEX_ISS_URL}/iss/statistics/engines/stock/markets/shares/correlations.json", "shares_correlations_"),
        (f"{MOEX_ISS_URL}/iss/statistics/engines/stock/splits.json", "splits_"),
    ]
    for r in reqs:
        async with AiohttpMoexClientSession() as session:
            iss = aiomoex.ISSClient(session, r[0])
            data = await iss.get()
            for key in data.keys():
                df = pd.DataFrame(data[key])
                saveDataFrame(df, f"{COMMON_MOEX_PATH}/{r[1]}{key}")


async def extractSecurityListsing():
    # todo error 403
    pass
    # req_url = f"{MOEX_ISS_URL}/iss/statistics/engines/stock/securitieslisting.json"
    # async with AiohttpMoexClientSession() as session:
    #     req = await session.get(req_url)
    #     j_s = await req.json()
    #     for k in j_s.keys():
    #         df = pd.DataFrame(j_s[k])
    #         saveDataFrame(df, f"{COMMON_MOEX_PATH}/securitieslisting_{k}")


async def extractMoexSessions():
    markets = [
        ["stock", "shares"]
    ]
    async with AiohttpMoexClientSession() as session:
        for engine, market in markets:
            request_url = f"{MOEX_ISS_URL}/iss/history/engines/{engine}/markets/{market}/sessions.json"
            iss = aiomoex.ISSClient(session, request_url)
            data = await iss.get()
            for key in data.keys():
                df = pd.DataFrame(data[key])
                saveDataFrame(df, f"{COMMON_MOEX_PATH}/sessions_{engine}_{market}_{key}")


async def extractMoexWorkTime():
    async with AiohttpMoexClientSession() as session:
        for engine in ['stock', 'currency']:
            request_url = f"{MOEX_ISS_URL}/iss/engines/{engine}.json"
            iss = aiomoex.ISSClient(session, request_url)
            data = await iss.get()
            for key in data.keys():
                df = pd.DataFrame(data[key])
                saveDataFrame(df, f"{COMMON_MOEX_PATH}/engine_{engine}_{key}")


async def extractMoexSecuritiesAsync():
    start = 0
    async with AiohttpMoexClientSession() as session:
        f_csv = open(f"{COMMON_MOEX_PATH}/securities.csv", "w")
        f_txt = open(f"{COMMON_MOEX_PATH}/securities.txt", "w")

        for ia in range(0, MAX_ITERATION):  #
            request_url = f"{MOEX_ISS_URL}/iss/securities.json?start={start}&land=ru"
            iss = aiomoex.ISSClient(session, request_url)
            data = await iss.get()
            df = pd.DataFrame(data['securities'])
            # df_marketdata = pd.DataFrame(data['marketdata'])  # todo
            if len(df) <= 0:
                break
            start += len(df)
            f_csv.write(df.to_csv())
            f_txt.write(df.to_string())
        f_csv.close()
        f_txt.close()
        print(f"created {f_csv.name} and {f_txt.name}")


async def extractTodayTurnovers():
    async with AiohttpMoexClientSession() as session:
        dfAll = None
        fileName = f"{COMMON_MOEX_PATH}/turnovers"

        request_url = f"{MOEX_ISS_URL}/iss/turnovers.json?date=today&land=ru"
        data = await aiomoex.ISSClient(session, request_url).get()

        df = pd.DataFrame(data['turnovers'])
        df2 = pd.DataFrame(data['turnoversprevdate'])
        if not df2.empty:
            df = df2.append(df)

        print(f"loaded turnovers:  {len(df)}")
        df.columns = df.columns.map(lambda x: x.lower())
        saveDataFrame(df.sort_values(by=['updatetime', 'name']), fileName)


async def extractTodayAggregates():
    securities = []
    markets = [  # (engine , market )
        ('stock', 'shares'),
        ('currency', 'selt'),
        ('currency', 'otc'),
    ]

    for engine, market in markets:
        dfSec = loadDataFrame(f"{COMMON_MOEX_PATH}/securities_{engine}_{market}")
        if not dfSec is None:
            dfSec.columns = dfSec.columns.map(lambda x: x.lower())
            securities += list(dfSec['secid'].drop_duplicates().values)

    dfAll = None
    fileName = f"{COMMON_MOEX_PATH}/aggregates"

    for i, sec in enumerate(securities):
        request_url = f"{MOEX_ISS_URL}/iss/securities/{sec}/aggregates.json?date=today&land=ru"
        if not dfAll is None and i % 100 == 0:
            saveDataFrame(dfAll.sort_values(by=['secid', 'tradedate', 'secid', 'market_name']), fileName)

        async with AiohttpMoexClientSession() as session:
            data = await aiomoex.ISSClient(session, request_url).get()
            df = pd.DataFrame(data['aggregates'])
            if df.empty:
                print(f"error load aggregates for {sec}")
                continue

            if not dfAll is None:
                dfAll = dfAll.append(df)
            else:
                dfAll = df

    if not dfAll is None:
        saveDataFrame(dfAll.sort_values(by=['secid', 'tradedate', 'secid', 'market_name']), fileName)


def extractMoexAllCommonInfo(interval=None, airflow=False):
    print(f"extractMoexAllCommonInfo interval interval = {interval} airflow = {airflow}")
    os.makedirs(COMMON_MOEX_PATH, exist_ok=True)

    global IS_AIRFLOW, UPDATE_INTERVAL
    UPDATE_INTERVAL = interval
    IS_AIRFLOW = airflow

    skip_flag = False
    start_state = {}
    if airflow and not interval is None and os.path.exists(START_STATE_PATH):
        with open(START_STATE_PATH, "r") as f:
            start_state = json.loads(f.read())
            if 'end' in start_state and datetime.utcnow() < datetime.fromisoformat(start_state['end']) + interval:
                print("extractMoexAllCommonInfo loaded - skip_flag = True")
                skip_flag = True

    start_state['start'] = datetime.utcnow()

    if not skip_flag:
        print(f"extract moex info to {COMMON_MOEX_PATH}")
        asyncio.run(extractMoexInfoAsync())
        asyncio.run(extractMoexSecuritiesAsync())

    if not skip_flag or not isDataframeExist(f"{COMMON_MOEX_PATH}/turnovers"):
        print(f"extract moex turnovers to {COMMON_MOEX_PATH}")
        asyncio.run(extractTodayTurnovers())

    if not skip_flag or not isDataframeExist(f"{COMMON_MOEX_PATH}/aggregates"):
        print(f"extract moex aggregates to {COMMON_MOEX_PATH}")
        asyncio.run(extractTodayAggregates())

    if not skip_flag:
        print(f"extract moex worktime {COMMON_MOEX_PATH}")
        asyncio.run(extractMoexWorkTime())

    if not skip_flag:
        asyncio.run(extractMoexSessions())
        asyncio.run(extractSecurityListsing())

    if not skip_flag:
        start_state['end'] = datetime.utcnow()
        with open(START_STATE_PATH, "w", encoding='utf-8') as f:
            f.write(json.dumps(start_state, default=json_serial))

    chmodForAll(COMMON_MOEX_PATH, 0o777, 0o666)


if __name__ == "__main__":
    # todo load columns info
    # extractMoexAllCommonInfo()

    asyncio.run(extractMoexSecuritiesAsync())
    # asyncio.run(extractMoexWorkTime())
    # asyncio.run(extractMoexInfoAsync())
    # asyncio.run(extractSecurityListsing())

    # extractMoexAllCommonInfo(interval=timedelta(days=14), airflow=True)
