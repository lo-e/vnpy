#-- coding: utf-8 --

import requests
import time
import os
import csv
from datetime import datetime, timedelta
from vnpy.trader.utility import DIR_SYMBOL

main_url = 'https://ftx.com'

# ====== 获取bar数据 ======
# symbol：'BTC-PERP'
# interval：'60'【分钟】, '3600'【小时】, '86400'【日】, '604800'【周】
# from：'%Y-%m-%d %H:%M:%S'
# limit：<= 200
def ftx_get_bar_data(symbol:str, interval:str, start_time:str='', end_time:str=''):
    api = f"/api/markets/{symbol}/candles"

    since = ''
    until = ''
    result_list = []
    base_url = f'{main_url}{api}?resolution={interval}'

    url = base_url
    if start_time:
        timeArray = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = int(time.mktime(timeArray))
        url += f'&start_time={start_time}'

    if end_time:
        timeArray = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = int(time.mktime(timeArray))
        url += f'&end_time={end_time}'

    resp = requests.get(url, headers={}, params={})
    data = resp.json()
    bar_data_list = data.get('result', [])

    if bar_data_list:
        # 数据整理
        for data in bar_data_list:
            ts = data['time']
            o = data['open']
            h = data['high']
            l = data['low']
            c = data['close']
            vol = data['volume']
            data_dic = {}
            # 转换时间戳
            the_timestamp = int(ts) / 1000
            datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(the_timestamp))
            if not since:
                since = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))
            until = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))

            data_dic['datetime'] = datetime_str
            data_dic['symbol'] = symbol
            data_dic['open'] = str(o)
            data_dic['high'] = str(h)
            data_dic['low'] = str(l)
            data_dic['close'] = str(c)
            data_dic['volume'] = str(vol)
            result_list.append(data_dic)

        print(f'======  {symbol} {since} -> {until} ======')

    if not len(result_list):
        return None

    # 写入csv
    contract = f'FTX.{symbol}'
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

    return datetime.strptime(since, "%Y-%m-%d-%H%M%S")

def get_csv_path():
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    csv_path = path.rstrip(file_name) + f'CSVs{DIR_SYMBOL}'
    return csv_path

if __name__ == '__main__':
    #"""
    symbol = 'BTC-PERP'
    interval = '86400'
    start_time = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d") + ' 00:00:00'
    end_time = (datetime.now()).strftime("%Y-%m-%d") + ' 00:00:00'
    #start_time = ''
    #end_time = ''
    ftx_get_bar_data(symbol=symbol, interval=interval, start_time=start_time, end_time=end_time)
    print('completed！')
    #"""
