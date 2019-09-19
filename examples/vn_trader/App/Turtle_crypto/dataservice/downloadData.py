# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from .OneTokenDataService import get_bar_data
from .CSVsToLocal import CSVs1TokenBarLocalEngine
from .BarToLocal import BarLocalEngine
from datetime import datetime, timedelta

class TurtleCryptoDataDownloading(object):
    def __init__(self):
        pass

    def download(self):
        # """
        # 获取bar数据
        contractList = ['okef/btc.usd.q', 'okef/eth.usd.q', 'okef/eos.usd.q', 'okswap/btc.usd.td', 'okswap/eth.usd.td',
                        'okswap/eos.usd.td']
        duration = '1m'
        start = datetime.now() - timedelta(1)
        start = datetime(start.year, start.month, start.day)
        until = datetime.now() - timedelta(1)
        until = datetime(until.year, until.month, until.day)

        since = start
        while since <= until:
            for contract in contractList:
                print(f'下载数据：{since}\t{contract}')
                get_bar_data(contract=contract, since=datetime.strftime(since, '%Y-%m-%d'),
                             until=datetime.strftime(since + timedelta(1), '%Y-%m-%d'), duration=duration)
            since += timedelta(1)

        # 1m数据入数据库
        print('\n====== 1m数据入数据库 ======')
        engine = CSVs1TokenBarLocalEngine(duration='1m')
        engine.startWork()

        # 1m数据合成Daily数据
        print('\n====== 1m数据合成Daily数据 ======')
        engine = BarLocalEngine(duration='1m')
        start_date = datetime.strftime(start, '%Y-%m-%d')
        end_date = datetime.strftime(datetime.now() + timedelta(1), '%Y-%m-%d')
        for contract in contractList:
            elements = contract.split('/')
            symbol = '.'.join([elements[-1], elements[0]]).upper()
            engine.Crypto_1Min_Daily(symbol=symbol, start_date=start_date, end_date=end_date)
        # """

    def generate(self):
        return_msg = 'abcxyz'
        return True, return_msg