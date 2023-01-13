# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from .OneTokenDataService import get_bar_data, get_csv_path
from .BybitDataService import bybit_get_bar_data
from .OKExDataService import okex_get_bar_data
from .FTXDataService import ftx_get_bar_data
from .BinanceDataService import binance_get_bar_data, Binancetype
from .CSVsToLocal import CSVs1TokenBarLocalEngine, CSVsBybitBarLocalEngine, CSVsOKExBarLocalEngine, CSVsFTXBarLocalEngine, CSVsBinanceBarLocalEngine
from .BarToLocal import BarLocalEngine
from datetime import datetime, timedelta
import shutil
import os
from vnpy.trader.constant import Interval
from time import sleep

class TurtleCryptoDataDownloading(object):
    def __init__(self):
        pass

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
                download_failed = False
                try:
                    from_time = bybit_get_bar_data(symbol=contract, interval=interval, from_time=datetime.strftime(from_time, "%Y-%m-%d %H:%M:%S"))
                except Exception:
                    download_failed = True
                    print('****** 下载中断 ******')

                if download_failed:
                    sleep(2)

                elif from_time:
                    from_time = from_time + timedelta(minutes=1)
        #"""

        # 1m数据入数据库
        print('\n====== 1m数据入数据库 ======')
        engine = CSVsBybitBarLocalEngine(duration='1')
        engine.startWork()

    def download_from_okex(self, contract_list, days=1):
        #"""
        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        # 获取bar数据
        interval = '1D'
        for contract in contract_list:
            okex_get_bar_data(symbol=contract, interval=interval, from_time='', limit=days)
        #"""

        # 1D数据入数据库
        print('\n====== 1D数据入数据库 ======')
        engine = CSVsOKExBarLocalEngine(duration=interval)
        engine.startWork()

    def download_from_ftx(self, contract_list, interval:Interval, days=1):
        # 获取bar数据
        if interval == Interval.MINUTE:
            interval_str = '60'
        elif interval == Interval.DAILY:
            interval_str = '86400'
        else:
            return

        #"""
        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        start_time = datetime.now() - timedelta(days=days)
        for contract in contract_list:
            until_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            while until_time:
                print(f'下载数据：{until_time}\t{contract}')
                until_time = ftx_get_bar_data(symbol=contract, interval=interval_str, start_time='', end_time=until_time)
                if until_time and until_time >= start_time:
                    if interval == Interval.MINUTE:
                        until_time = until_time - timedelta(minutes=1)
                    elif interval == Interval.DAILY:
                        until_time = until_time - timedelta(days=1)
                    until_time = until_time.strftime('%Y-%m-%d %H:%M:%S')
                    print('\n')
                else:
                    until_time = ''
        #"""

        #"""
        #1D数据入数据库
        print('\n====== 1D数据入数据库 ======')
        engine = CSVsFTXBarLocalEngine(duration=interval_str)
        engine.startWork()
        #"""

    def download_from_binance(self, contract_list, type:Binancetype, days=1):
        #"""
        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        # 获取bar数据
        interval = '1m'
        from_date = datetime.now() - timedelta(days=days)

        for contract in contract_list:
            from_time = datetime(from_date.year, from_date.month, from_date.day)
            while from_time:
                print(f'下载数据：{from_time}\t{contract}')
                from_time = binance_get_bar_data(symbol=contract, interval=interval, symbol_type=type, start_time=datetime.strftime(from_time, "%Y-%m-%d %H:%M:%S"))
                if from_time:
                    from_time = from_time + timedelta(minutes=1)
                    print('\n')
        #"""

        #"""
        # 1m数据入数据库
        print('\n====== 1m数据入数据库 ======')
        engine = CSVsBinanceBarLocalEngine(duration='1m')
        engine.startWork()
        #"""

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

    def generate_8h_for_bybit(self, contract_list, days=1):
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
            re, c_msg, b_msg, l_msg = engine.Crypto_1Min_8H(symbol=symbol, start_date=start_date, end_date=end_date)
            if not re:
                result = False
            complete_msg += c_msg + '\n\n'
            back_msg += b_msg + '\n\n'
            lost_msg += l_msg + '\n\n'

        return result, complete_msg, back_msg, lost_msg