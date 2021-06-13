import aiomoex

from moex import *
from lxml import html as lxmlHtml
import urllib.parse

logging.basicConfig(level=logging.DEBUG)

# extract common moex info:
#     instruments
#     engines
#     markets


START_STATE_PATH = f"{API_MOEX_PATH}/start_state.log"

UPDATE_INTERVAL = None
IS_AIRFLOW = False


async def extractApi():
    mocky = {
        'security': 'IMOEX',
        'engine': 'stock',
        'market': 'shares',
        'board': 'TQBR',
        'session': None,
        'boardgroup': None,
        'datatype': None,
        'securitygroup': None,
        'collection': None,

    }

    api_defs = []
    async with AiohttpClientSession() as  session:
        request_url = f"{MOEX_ISS_URL}/iss/reference"
        req = await session.get(request_url)
        data = await req.text()
        html_root = lxmlHtml.fromstring(data)
        # hrev links
        for anchor in html_root.xpath('//a'):
            if not 'href' in anchor.attrib:
                continue
            url = anchor.attrib['href']

            ref_desc = urllib.parse.urljoin(request_url + '/', url)
            api_url = urllib.parse.urljoin(MOEX_ISS_URL + '/', anchor.text)
            api_url_columns = urllib.parse.urljoin(api_url + '/', './columns.json')

            api_def = {
                'relative_ref': anchor.text,
                'ref_desc': ref_desc,
                'api_url': api_url,
                'api_url_columns': api_url_columns,
                'desc_async': await session.get(ref_desc),
                'api_columns_async': aiomoex.ISSClient(session, api_url_columns).get(),
            }
            api_defs += [api_def]

        for api_def in api_defs:
            ref_desc = api_def['ref_desc']
            api_url = api_def['api_url']

            desc_data = await api_def['desc_async'].text()
            columns_data = await api_def['api_columns_async']

            desc_text = lxmlHtml.fromstring(desc_data).body.text_content()
            api_fileName = api_def['relative_ref'].replace('/', "_")
            with open(f'{API_MOEX_PATH}/{api_fileName}', 'w+') as f:
                relative = api_def['relative_ref']
                f.write(f"REF: {relative}\n")
                f.write(f"FULL_REF: {api_url}\n\n")
                f.write(f"DESC: \n{desc_text}\n\n")
                f.write(f"COLUMNS: \n")

                for key in columns_data.keys():
                    df = pd.DataFrame(columns_data[key])
                    f.write(f'{key}:\n')
                    f.write(df.to_string())


def extractMoexApi(interval=None, airflow=False):
    print(f"extractMoexApi interval interval = {interval} airflow = {airflow}")
    os.makedirs(API_MOEX_PATH, exist_ok=True)

    global IS_AIRFLOW, UPDATE_INTERVAL
    UPDATE_INTERVAL = interval
    IS_AIRFLOW = airflow

    skip_flag = False
    if airflow and not interval is None and os.path.exists(START_STATE_PATH):
        with open(START_STATE_PATH, "r") as f:
            start_state = json.loads(f.read())
            if 'end' in start_state and datetime.utcnow() < datetime.fromisoformat(start_state['end']) + interval:
                print("extractMoexApi loaded - skip_flag = True")
                skip_flag = True
    start_state = {'start': datetime.utcnow()}

    if not skip_flag:
        print(f"extract moex api to {API_MOEX_PATH}")
        asyncio.run(extractApi())
        pass

    if not skip_flag:
        start_state['end'] = datetime.utcnow()
        with open(START_STATE_PATH, "w", encoding='utf-8') as f:
            f.write(json.dumps(start_state, default=json_serial))

    chmodForAll(API_MOEX_PATH, 0x777, 0o666)


if __name__ == '__main__':
    extractMoexApi()
