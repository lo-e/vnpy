# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from dataService import *
from csv import DictReader
import re

if __name__ == '__main__':
    """
    downloadMinuteBarBySymbol('RB99', 5)
    downloadDailyBarBySymbol('RB99')
    downloadTickBySymbol('RB1905', '2018-12-21')
    """

    #"""
    input = raw_input(u'输入合约类型【88主力 888平滑主力 99指数】')
    if input == '88' or input == '888' or input == '99':
        filename = 'symbol_list.csv'
        count = 0
        with open(filename) as f:
            r = DictReader(f)
            for d in r:
                startSymbol = re.sub("\d", "", d['vtSymbol'])
                symbol = startSymbol + input
                downloadDailyBarBySymbol(symbol)
                count += 1
        print  '合约数：%d' % count
    #"""