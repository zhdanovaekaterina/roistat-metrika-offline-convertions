import requests as req
import config
import json
import time

start_time = time.time()

request_headers = {'Api-key': config.roistat_token,
                   'Content-type': 'application/json'}  # установка заголовков

# диапазон дат, за который нужны значения (прошлая неделя)
# начальная дата за 1 день до начала недели (вытягивает часовой пояс 0)
filters = {
        "filters": {
            "and": [
                ["date", ">", "2022-06-26T21:00:00+0000"],
                ["date", "<", "2022-07-03T21:00:00+0000"]
            ]
        },
        "extend": ["visit", "order"]
    }


def get_all_calls():
    """Возвращает количество звонков за прошлую неделю."""
    url = 'https://cloud.roistat.com/api/v1/project/calltracking/call/list'

    params = {'project': config.roistat_project}

    r = req.post(url, headers=request_headers, params=params, json=filters)
    data = json.loads(r.text)

    all_calls_1, all_calls_2 = 0, 0
    calls_1, calls_2 = 0, 0

    for i in range(len(data['data'])):
        if data['data'][i]['script_name'] == 'Динамический: okno.ru':
            all_calls_1 += 1
            if data['data'][i]['visit']['metrika_client_id'] is not None:
                calls_1 += 1
        elif data['data'][i]['script_name'] == 'Динамический: okno-moskva.ru':
            all_calls_2 += 1
            if data['data'][i]['visit']['metrika_client_id'] is not None:
                calls_2 += 1

    return all_calls_1, all_calls_2, calls_1, calls_2


def main():
    calls_number = get_all_calls()
    print(f'Всего звонков okno.ru - {calls_number[0]}\n'
        f'Всего звонков okno-moskva.ru - {calls_number[1]}\n'
        f'Звонков okno.ru со счетчиком Метрики - {calls_number[2]}\n'
        f'Звонков okno-moskva.ru со счетчиком Метрики - {calls_number[3]}\n')


if __name__ == '__main__':
    main()
