# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from dataService import *
from csv import DictReader

if __name__ == '__main__':
    """
    downloadMinuteBarBySymbol('RB99', 5)
    downloadDailyBarBySymbol('RB99')
    downloadTickBySymbol('RB1905', '2018-12-21')
    """
    downloadDailyBarBySymbol('TA99')
    """
    filename = 'symbol_list.csv'
    count = 0
    with open(filename) as f:
        r = DictReader(f)
        for d in r:
            symbol = d['vtSymbol']
            downloadDailyBarBySymbol(symbol)
            count += 1
    print  '合约数：%d' % count
    """