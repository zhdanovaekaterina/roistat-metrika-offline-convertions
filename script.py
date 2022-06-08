from re import S
from sqlite3 import paramstyle
import requests as req
import config
import pprint
import json
import time
import pandas as pd

start_time = time.time()

url = 'https://cloud.roistat.com/api/v1'
client_project = '72370'
request_headers = {'Api-key': config.roistat_token,
    'Content-type': 'application/json'
    }

start_date = '2022-05-30T00:00:00+0000'
end_date = '2022-06-01T00:00:00+0000'
limit = 100

def get_all_statuses():
    '''Возвращает словарь всех статусов.'''
    api = '/project/integration/status/list'
    full_url = url + api

    params = {'project': client_project}

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
    full_url = url + api

    params = {
        'project': client_project,
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

def main():
    data = get_all_deals(start_date, end_date)
    roistat_data = get_roistat_target_deals(data)

    end_time = time.time()
    total_time = end_time - start_time
    print('Total time: ', '%.3f' % total_time, ' s.')

if __name__ == '__main__':
    main()


