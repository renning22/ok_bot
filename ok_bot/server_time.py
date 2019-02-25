import dateutil.parser as dp
import requests

OK_TIMESERVER_ADDRESS = 'http://www.okex.com/api/general/v3/time'


def get_server_time_iso():
    response = requests.get(OK_TIMESERVER_ADDRESS)
    if response.status_code == 200:
        return response.json()['iso']
    raise RuntimeError('failed to request server time')


def get_server_timestamp():
    server_time = get_server_time_iso()
    parsed_t = dp.parse(server_time)
    return parsed_t.timestamp()
