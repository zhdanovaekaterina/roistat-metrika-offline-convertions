import requests as req
import config
import json
import time
import datetime as dt
from datetime import date, datetime, timedelta
import pandas as pd
from tapi_yandex_metrika import YandexMetrikaStats
import re
import logging

# TODO: Разобраться с тем, почему logging не пишет сообщения в файл
start_time = time.time()
logging.basicConfig(filename='logfile.txt', level=logging.DEBUG)

request_headers = {'Api-key': config.roistat_token,
    'Content-type': 'application/json'
    }

clients = [config.clients[0], config.clients[1]]
counters = [config.counter_1, config.counter_2]

# TODO: Добавить формат 0Х, если время состоит из 1 цифры
# TODO: Вынести установку начального и конечного времени в отдельную функцию
time_zone = config.time_zone
start_time_rs = str(24 - ((time_zone + 24) % 24))
end_time_rs = str(23 - ((time_zone + 24) % 24))
if time_zone < 0:
    first_day = str(date.today() - timedelta(days=1))
    second_day = str(date.today())
else:
    first_day = str(date.today() - timedelta(days=2))
    second_day = str(date.today() - timedelta(days=1))
start_date = f'{first_day}T{start_time_rs}:00:00+0000'               # Время по UTC+0
end_date = f'{second_day}T{end_time_rs}:59:59+0000'


def time_convertion(date_time:str):
    '''Принимает значение даты и времени в формате строки в UTC+0. Возвращает время в формате timestamp (в секундах) в UTC+3.'''
    date_time = date_time[:10] + ' ' + date_time[11:19]
    if time_zone < 0:                                                 # Конвертация часового пояса
        date_time_obj = dt.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S') - timedelta(hours=abs(time_zone))
    else:
        date_time_obj = dt.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S') + timedelta(hours=time_zone)
    date_time_obj = round(date_time_obj.timestamp())
    return date_time_obj

def get_all_statuses():
    '''Возвращает словарь всех статусов.'''
    url = 'https://cloud.roistat.com/api/v1/project/integration/status/list'

    params = {'project': config.roistat_project}

    r = req.post(url, headers=request_headers, params=params)
    data = json.loads(r.text)

    statuses = {}
    for i in range(data['total']):
        key = data['data'][i]['id']
        value = data['data'][i]['name']
        statuses[key] = value

    return statuses

def get_target_statuses(all_statuses:dict):
    '''Принимает словарь из ID и значений статусов. Возвращает список ID целевых статусов.'''
    non_target_statuses = ('Не обработан', 'Не целевой', 'Закрыто и не реализовано')
    target_statuses_dict = {}
    for k, v in all_statuses.items():
        if v not in non_target_statuses:
            target_statuses_dict[k] = v
    target_statuses = list(target_statuses_dict.values())
    return target_statuses

def get_all_deals(start_date:str, end_date:str):
    '''Принимает дату начала и дату окончания периода в формате UTC0. Возвращает сделки за указанный период.'''
    url = 'https://cloud.roistat.com/api/v1/project/integration/order/list'
    
    params = {
        'project': config.roistat_project
    }

    filters = {
        "filters": {
            "and": [
                ["creation_date", ">", start_date],
                ["creation_date", "<", end_date]
            ]
        }
    }

    r = req.post(url, headers=request_headers, params=params, json=filters)
    data = json.loads(r.text)
    return data

def get_roistat_target_deals(data):
    '''Принимает массив сделок. Возвращает дата фрейм из сделок, которые находятся на целевых статусах и имеют численное значение roistat id.'''
    non_target_statuses = ('1001', '1003', '13')
    fields_to_export = ['roistat', 'id', 'source_type', 'creation_date']
    deals = []
    for i in range(len(data['data'])):
        if (data['data'][i]['status']['id'] not in non_target_statuses) and (re.fullmatch(r'^\d{7,8}$', data['data'][i]['roistat'])):                               # отбираются только те сделки, которые находятся на целевых статусов и имеют численное значение roistat id
            deal = {}
            for k in range(len(fields_to_export)):
                deal[fields_to_export[k]] = data['data'][i][fields_to_export[k]]
            deal['status'] = data['data'][i]['status']['id']
            deals.append(deal)
    for i in range(len(deals)):
        deals[i]['creation_date'] = time_convertion(deals[i]['creation_date'])

    roistat_data = pd.DataFrame(deals)

    return roistat_data

def get_metrika_data(counter):
    '''Принимает ID счетчика и имя файла, в который запишет результат (необходим на этапе проверки данных). Получает данные из Яндекс Метрики: Metrika ID и roistat-visit-id.'''
    ACCESS_TOKEN = config.metrika_token
    METRIC_IDS = counter

    api = YandexMetrikaStats(access_token=ACCESS_TOKEN, receive_all_data=True)

    params = dict(
    ids = METRIC_IDS,
    metrics = "ym:s:visits",
    dimensions = "ym:s:clientID,ym:s:paramsLevel2",
    date1 = "yesterday",
    date2 = "yesterday",
    accuracy="full",
    limit = 10000)                                                  # максимальное кол-во визитов в день

    result = api.stats().get(params=params)
    result = result().data
    result = result[0]['data']

    dict_data = {}

    for i in range(0, len(result)-1):
        if result[i]["dimensions"][1]["name"] != None:              # в словарь записываются только пары, у которых есть roistat-visit-id
            dict_data[i] = {
                'ClientId':result[i]["dimensions"][0]["name"],
                'roistat':result[i]["dimensions"][1]["name"]
            }

    dict_keys = dict_data[0].keys()
    df = pd.DataFrame.from_dict(dict_data, orient='index',columns=dict_keys)

    return(df)

def merge_data(roistat_data, metrika_data, file_name):
    '''Получает два датафрейма с данными из Ройстат и из Метрики. Добавляет Metrika ID к данным сделки. Выгружает данные в файл csv в текущей директории, готовый к загрузке в Метрику.'''
    joined_data = pd.merge(roistat_data, metrika_data)
    joined_data = joined_data.drop(['roistat', 'id', 'source_type', 'status'], axis=1)
    joined_data.insert(1, 'Target', 'test_target_lead')
    joined_data.insert(1, 'Price', 0)
    joined_data.insert(1, 'Currency', 'RUB')

    joined_data.rename(columns={'creation_date': 'DateTime'}, inplace=True)
    joined_data = joined_data.reindex(columns=['ClientId', 'Target', 'DateTime', 'Price', 'Currency'])
    joined_data.to_csv(f'{file_name}_joined_{second_day}.csv', index=False)

def upload_data(counter_id, data):
    '''Получает номер счетчика и имя файла в текущей директории, готового к загрузке. Загружает данные в Метрику, возвращает в консоль время загрузки и статус ответа.'''

    counter = counter_id
    token = config.metrika_token

    file = open(f"{data}_joined_{second_day}.csv", "r").read()
    id_type = "CLIENT_ID"
    comment = f'Целевые за {second_day}, тест'

    url = "https://api-metrika.yandex.net/management/v1/counter/{}/offline_conversions/upload?client_id_type={}& [comment=<{}>]".format(counter, id_type, comment)
    headers = {
    "Authorization": "OAuth {}".format(token)
    }

    request = req.post(url, headers=headers, files={"file":file})
    if request.status_code == 200:
        logging.info(f'Данные за {second_day} успешно загружены в Метрику {datetime.now()}')
    else:
        logging.info(f'Данные за {second_day} не загружены в Метрику. Код ответа: {request.status_code}')

def main():
    data = get_all_deals(start_date, end_date)
    roistat_data = get_roistat_target_deals(data)
    for i in range(len(clients)):
        logging.info(f'Проект {clients[i]}')
        metrika_data = get_metrika_data(counters[i])
        merge_data(roistat_data, metrika_data, clients[i])
        upload_data(counters[i], clients[i])

    end_time = time.time()
    total_time = round((end_time - start_time), 3)
    logging.info(f'Время выполнения: {total_time} s.')

if __name__ == '__main__':
    main()
