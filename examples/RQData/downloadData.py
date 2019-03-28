# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from dataService import *
from csv import DictReader
import re
from datetime import datetime

DOMINANT_DB_NAME = 'Dominant_db'

if __name__ == '__main__':
    """
    downloadMinuteBarBySymbol('RB99', 5)
    downloadDailyBarBySymbol('RB99')
    downloadTickBySymbol('RB1905', '2018-12-21')
    """

    """
    # 下载真实主力合约bar数据到数据库
    symbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']
    for underlyingSymbol in symbolList:
        startDate = None
        #startDate = datetime.strptime('2010-1-1', '%Y-%m-%d')
        downloadDominantSymbol(underlyingSymbol, startDate)
    """

    """
    # 显示近一年来主力合约
    underlyingSymbol = 'AL'
    dominantList = showYearDominantSymbol(underlyingSymbol)
    for value in dominantList:
        print '%s\t%s' % (value[0], value[1])
    """

    """
    # 下载测试中的指数、主力合约
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
                print '\n'
        print  '合约数：%d' % count
    """

    #"""
    print u'下载真实主力合约到数据库'
    symbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']

    toDatabase = True
    for underlyingSymbol in symbolList:
        dominantList = dominantSymbolToDatabase(underlyingSymbol, startDate=datetime(1900, 01, 01), endDate=datetime.now(), toDatabase=toDatabase)
        # 查看历史数据最早的日期
        if not toDatabase:
            print('*' * 26 + underlyingSymbol + '*' * 26)
            startIndex = dominantList.index[0]
            startRqSymbol = dominantList[startIndex]
            endIndex = dominantList.index[-1]
            endRqSymbol = dominantList[endIndex]
            print('%s\t%s -- %s\t%s\n' % (startIndex, startRqSymbol, endIndex, endRqSymbol))
    print '完成！\n'

    """

    """
    print u'【显示最新主力合约】'
    #symbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']
    symbolList = ['IF', 'AL', 'HC', 'SM', 'J', 'TA']
    for underlyingSymbol in symbolList:
        symbol = showDominantSymbol(underlyingSymbol)
        print symbol
    print '\n\n'
    """

    """
    print u'【下载正在模拟实盘的海龟组合品种】'
    filename = 'turtle_trader_list.csv'
    count = 0
    with open(filename) as f:
        r = DictReader(f)
        for d in r:
            symbol = d['vtSymbol'].upper()
            downloadDailyBarBySymbol(symbol)
            count += 1
    print  '合约数：%d\n\n' % count
    """

    """
    print u'【下载模拟实盘中的指数、主力合约】'
    symbolList = ['IF', 'AL', 'HC', 'SM', 'J', 'TA']
    #input = raw_input(u'输入合约类型【88主力 888平滑主力 99指数】')
    input = '99'
    count = 0
    if input == '88' or input == '888' or input == '99':
        for symbol in symbolList:
            symbol = symbol + input
            downloadDailyBarBySymbol(symbol)
            count += 1
            print '\n'
        print  '合约数：%d' % count
    #"""