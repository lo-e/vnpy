#-- coding: utf-8 --

import requests
import time
import os
import csv
from datetime import datetime, timedelta
from vnpy.trader.utility import DIR_SYMBOL

main_url = 'https://api.bybit.com'

# ====== 获取bar数据 ======
# symbol：'BTCUSD'
# interval：'1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'M', 'W', 'Y'
# from：'%Y-%m-%d %H:%M:%S'
# limit：<= 200
def bybit_get_bar_data(symbol:str, interval:str, from_time:str, limit:int=200):
    timeArray = time.strptime(from_time, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    url = f'{main_url}/v2/public/kline/list?symbol={symbol}&interval={interval}&from={timeStamp}&limit={limit}'
    resp = requests.get(url, headers={}, params={})
    data = resp.json()
    bar_data = data.get('result', [])

    # 数据整理
    result_list = []
    since = ''
    until = ''
    for dic in bar_data:
        # 转换时间戳
        the_timestamp = dic['open_time']
        datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(the_timestamp))
        if not since:
            since = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))
        until = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))

        dic.pop('open_time')
        dic.pop('interval')
        dic.pop('turnover')
        dic['datetime'] = datetime_str
        result_list.append(dic)

    if not len(result_list):
        return None

    # 写入csv
    contract = f'BYBIT.{symbol}'
    csv_path = get_csv_path()
    dir_path = csv_path + f'{contract}{DIR_SYMBOL}{interval}{DIR_SYMBOL}'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = dir_path + f'{since}__{until}.csv'
    field_names = ['datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    with open(file_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()
        # 写入csv文件
        writer.writerows(result_list)

    return datetime.strptime(until, "%Y-%m-%d-%H%M%S")

def get_csv_path():
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    csv_path = path.rstrip(file_name) + f'CSVs{DIR_SYMBOL}'
    return csv_path

if __name__ == '__main__':
    symbol = 'BTCUSD'
    interval = '1'
    from_time = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    bybit_get_bar_data(symbol=symbol, interval=interval, from_time=from_time)
    print('completed！')
