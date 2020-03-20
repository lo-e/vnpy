# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from .OneTokenDataService import get_bar_data, get_csv_path
from .BybitDataService import bybit_get_bar_data
from .CSVsToLocal import CSVs1TokenBarLocalEngine, CSVsBybitBarLocalEngine
from .BarToLocal import BarLocalEngine
from datetime import datetime, timedelta
import shutil
import os

class TurtleCryptoDataDownloading(object):
    def __init__(self):
        pass

    def download_from_onetoken(self, contract_list, days=1):
        #"""
        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        # 获取bar数据
        duration = '1m'
        start = datetime.now() - timedelta(days=days)
        start = datetime(start.year, start.month, start.day)
        until = datetime.now() - timedelta(0)
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

    def download_from_bybit(self, contract_list, days=1):
        #"""
        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        # 获取bar数据
        interval = '1'
        from_date = datetime.now() - timedelta(days=days)

        for contract in contract_list:
            from_time = datetime(from_date.year, from_date.month, from_date.day)
            while from_time:
                print(f'下载数据：{from_time}\t{contract}')
                from_time = bybit_get_bar_data(symbol=contract, interval=interval, from_time=datetime.strftime(from_time, "%Y-%m-%d %H:%M:%S"))
                if from_time:
                    from_time = from_time + timedelta(minutes=1)
        #"""

        # 1m数据入数据库
        print('\n====== 1m数据入数据库 ======')
        engine = CSVsBybitBarLocalEngine(duration='1')
        engine.startWork()

    def generate_for_onetoken(self, contract_list, days=1):
        result = True
        complete_msg = ''
        back_msg = ''
        lost_msg = ''

        if not contract_list:
            return False, complete_msg, back_msg, lost_msg

        # 1m数据合成Daily数据
        print('\n====== 1m数据合成Daily数据 ======')
        engine = BarLocalEngine()

        from_day = datetime.now() - timedelta(days=days)
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

    def generate_for_bybit(self, contract_list, days=1):
        result = True
        complete_msg = ''
        back_msg = ''
        lost_msg = ''

        if not contract_list:
            return False, complete_msg, back_msg, lost_msg

        # 1m数据合成Daily数据
        print('\n====== 1m数据合成Daily数据 ======')
        engine = BarLocalEngine()

        from_day = datetime.now() - timedelta(days=days)
        to_day = datetime.now()
        start_date = datetime.strptime(f'{from_day.year}-{from_day.month}-{from_day.day} 08:00:00', '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(f'{to_day.year}-{to_day.month}-{to_day.day} 07:59:00', '%Y-%m-%d %H:%M:%S')

        for contract in contract_list:
            symbol = f'{contract}.BYBIT'
            re, c_msg, b_msg, l_msg = engine.Crypto_1Min_Daily(symbol=symbol, start_date=start_date, end_date=end_date)
            if not re:
                result = False
            complete_msg += c_msg + '\n\n'
            back_msg += b_msg + '\n\n'
            lost_msg += l_msg + '\n\n'

        return result, complete_msg, back_msg, lost_msg