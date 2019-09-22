# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from .OneTokenDataService import get_bar_data, get_csv_path
from .CSVsToLocal import CSVs1TokenBarLocalEngine
from .BarToLocal import BarLocalEngine
from datetime import datetime, timedelta
import shutil
import os

class TurtleCryptoDataDownloading(object):
    def __init__(self):
        pass

    def download(self, contract_list):
        #"""
        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        # 获取bar数据
        duration = '1m'
        start = datetime.now() - timedelta(1)
        start = datetime(start.year, start.month, start.day)
        until = datetime.now() - timedelta(1)
        until = datetime(until.year, until.month, until.day)

        since = start
        while since <= until:
            for contract in contract_list:
                print(f'下载数据：{since}\t{contract}')
                get_bar_data(contract=contract, since=datetime.strftime(since, '%Y-%m-%d'),
                             until=datetime.strftime(since + timedelta(1), '%Y-%m-%d'), duration=duration)
            since += timedelta(1)
        #"""

        # 1m数据入数据库
        print('\n====== 1m数据入数据库 ======')
        engine = CSVs1TokenBarLocalEngine(duration='1m')
        engine.startWork()

    def generate(self, contract_list):
        result = True
        complete_msg = ''
        back_msg = ''
        lost_msg = ''

        if not contract_list:
            return False, complete_msg, back_msg, lost_msg

        # 1m数据合成Daily数据
        print('\n====== 1m数据合成Daily数据 ======')
        engine = BarLocalEngine(duration='1m')

        from_day = datetime.now() - timedelta(days=1)
        to_day = datetime.now()
        start_date = datetime.strptime(f'{from_day.year}-{from_day.month}-{from_day.day} 08:00:00', '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(f'{to_day.year}-{to_day.month}-{to_day.day} 07:59:00', '%Y-%m-%d %H:%M:%S')

        re_list = []
        for contract in contract_list:
            elements = contract.split('/')
            symbol = '.'.join([elements[-1], elements[0]]).upper()
            re, c_msg, b_msg, l_msg = engine.Crypto_1Min_Daily(symbol=symbol, start_date=start_date, end_date=end_date)
            re_list.append(re)
            complete_msg += c_msg + '\n\n'
            back_msg += b_msg + '\n\n'
            lost_msg += l_msg + '\n\n'

        for re in re_list:
            if not re:
                result = False
                break
        return result, complete_msg, back_msg, lost_msg