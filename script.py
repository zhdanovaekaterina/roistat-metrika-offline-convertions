from re import S
from sqlite3 import paramstyle
import requests as req
import config
import pprint
import json
import time
import pandas as pd
from tapi_yandex_metrika import YandexMetrikaStats

start_time = time.time()

request_headers = {'Api-key': config.roistat_token,
    'Content-type': 'application/json'
    }

start_date = '2022-05-30T00:00:00+0000'
end_date = '2022-06-01T00:00:00+0000'
limit = 100


def get_all_statuses():
    '''Возвращает словарь всех статусов.'''
    api = '/project/integration/status/list'
    full_url = config.roistat_api_url + api

    params = {'project': config.roistat_project}

    r = req.post(full_url, headers=request_headers, params=params)
    data = json.loads(r.text)

    statuses = {}
    for i in range(data['total']):
        key = data['data'][i]['id']
        value = data['data'][i]['name']
        statuses[key] = value
  
    return statuses

def get_target_statuses(all_statuses:dict):
    '''Принимает словарь из ID и значений статусов. Возвращает список ID целевых статусов.'''
    non_target_statuses = ('Не обработан', 'Не целевой')
    target_statuses_dict = {}
    for k, v in all_statuses.items():
        if v not in non_target_statuses:
            target_statuses_dict[k] = v
    # target_statuses = list(target_statuses_dict.keys())
    target_statuses = list(target_statuses_dict.values())
    return target_statuses

def get_all_deals(start_date:str, end_date:str):
    '''Принимает дату начала и дату окончания периода в формате UTC0. Возвращает сделки за указанный период.'''
    api = '/project/integration/order/list'
    full_url = config.roistat_api_url + api

    params = {
        'project': config.roistat_project,
        'limit': limit
    }

    filters = {
        "filters": {
            "and": [
                ["creation_date", ">", start_date],
                ["creation_date", "<", end_date]
            ]
        }
    }

    r = req.post(full_url, headers=request_headers, params=params, json=filters)
    data = json.loads(r.text)
    return data

def get_roistat_target_deals(data):
    '''Принимает массив сделок. Возвращает дата фрейм из сделок, которые находятся на целевых статусах.'''
    non_target_statuses = ('1001', '1003')
    fields_to_export = ['id', 'cost', 'creation_date', 'roistat', 'source_type']
    deals = []
    for i in range(limit):
        if data['data'][i]['status']['id'] not in non_target_statuses:
            deal = {}
            for k in range(len(fields_to_export)):
                deal[fields_to_export[k]] = data['data'][i][fields_to_export[k]]
            deal['status'] = data['data'][i]['status']['id']
            deals.append(deal)
    roistat_data = pd.DataFrame(deals)
    return roistat_data

def get_metrika_data(counter, file_name):
    '''Принимает ID счетчика и имя файла, в который запишет результат (необходим на этапе проверки данных). Получает данные из Яндекс Метрики.'''
    ACCESS_TOKEN = config.metrika_token
    METRIC_IDS = counter

    api = YandexMetrikaStats(access_token=ACCESS_TOKEN, receive_all_data=True)

    params = dict(
    ids = METRIC_IDS,
    metrics = "ym:s:users,ym:s:visits",
    dimensions = "ym:s:date,ym:s:visitID,ym:s:paramsLevel1",    # ym:s:paramsLevel1 передает название, а не значение параметра
    date1 = "2daysAgo",
    date2 = "yesterday",
    sort = "ym:s:date",
    accuracy="full",
    limit = 1000)

    result = api.stats().get(params=params)
    result = result().data
    result = result[0]['data']

    dict_data = {}

    for i in range(0, len(result)-1):
        dict_data[i] = {
            'date':result[i]["dimensions"][0]["name"],
            'metrika-id':result[i]["dimensions"][1]["name"],
            'roistat-visit-id':result[i]["dimensions"][2]["name"],
            'users':result[i]["metrics"][0],
            'visits':result[i]["metrics"][1]
        }

    dict_keys = dict_data[0].keys()
    df = pd.DataFrame.from_dict(dict_data, orient='index',columns=dict_keys)

    # Выгрузка данных в эксель для проверки
    df.to_excel(f"{file_name}.xlsx",
        sheet_name='data',
        index=False)

    return(df)


def main():
    # data = get_all_deals(start_date, end_date)
    # roistat_data = get_roistat_target_deals(data)
    # print(roistat_data)
    metrika_data_1 = get_metrika_data(config.counter_1, 'okno_ru')
    # print(metrika_data_1.head(10))
    metrika_data_2 = get_metrika_data(config.counter_2, 'okno_moskva_ru')
    # print(metrika_data_2.head(10))


    end_time = time.time()
    total_time = end_time - start_time
    print('Total time: ', '%.3f' % total_time, ' s.')

if __name__ == '__main__':
    main()


