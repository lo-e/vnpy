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
        symbolList = ['RB', 'CU', 'NI', 'ZN', 'RU', 'AL', 'HC', 'J', 'I', 'PP', 'AP', 'TA', 'A', 'AG', 'AU', 'B', 'BB', 'BU', 'C', 'CF', 'CS', 'CY', 'EG', 'FB', 'FG', 'FU', 'JD', 'JM', 'JR', 'L', 'LR', 'M', 'MA', 'OI', 'P', 'PB', 'PM', 'RI', 'RM', 'RS', 'SC', 'SF', 'SM', 'SN', 'SP', 'SR', 'V', 'WH', 'WR', 'Y', 'ZC', 'IF', 'IC', 'IH']
        #symbolList = ['RB', 'M', 'C', 'MA', 'TA', 'I', 'BU', 'AG', 'Y', 'SR']
        #symbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']
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


    """ ========================================================== """

    #"""
    print u'下载真实主力合约代码到数据库'
    #symbolList = ['RB', 'CU', 'NI', 'ZN', 'RU', 'AL', 'HC', 'J', 'I', 'PP', 'AP', 'TA', 'A', 'AG', 'AU', 'B', 'BB', 'BU', 'C', 'CF', 'CS', 'CY', 'EG', 'FB', 'FG', 'FU', 'JD', 'JM', 'JR', 'L', 'LR', 'M', 'MA', 'OI', 'P', 'PB', 'PM', 'RI', 'RM', 'RS', 'SC', 'SF', 'SM', 'SN', 'SP', 'SR', 'V', 'WH', 'WR', 'Y', 'ZC', 'IF', 'IC', 'IH']
    symbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']

    toDatabase = True
    for underlyingSymbol in symbolList:
        dominantList = dominantSymbolToDatabase(underlyingSymbol, toDatabase=toDatabase)
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
    print u'【显示最新主力合约代码】'
    #symbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']
    symbolList = ['RB', 'HC', 'SM', 'J', 'ZC', 'TA']
    for underlyingSymbol in symbolList:
        date, symbol = showDominantSymbol(underlyingSymbol)
        print symbol, '\t', date
    print '\n\n'
    """

    """
    print u'【下载近一年真实主力合约bar数据到数据库】'
    #symbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']
    symbolList = ['RB', 'HC', 'SM', 'J', 'ZC', 'TA']
    startDate = datetime.now().replace(year=datetime.now().year - 1)
    for underlyingSymbol in symbolList:
        downloadDominantSymbol(underlyingSymbol, startDate)
    print  '%s 完成\n\n' % symbolList
    """

    """
    print u'【下载指数、主力连续合约】'
    #symbolList = ['RB', 'CU', 'NI', 'ZN', 'RU', 'AL', 'HC', 'J', 'I', 'PP', 'AP', 'TA', 'A', 'AG', 'AU', 'B', 'BB', 'BU', 'C', 'CF', 'CS', 'CY', 'EG', 'FB', 'FG', 'FU', 'JD', 'JM', 'JR', 'L', 'LR', 'M', 'MA', 'OI', 'P', 'PB', 'PM', 'RI', 'RM', 'RS', 'SC', 'SF', 'SM', 'SN', 'SP', 'SR', 'V', 'WH', 'WR', 'Y', 'ZC', 'IF', 'IC', 'IH']
    #symbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']
    symbolList = ['RB', 'HC', 'SM', 'J', 'ZC', 'TA']
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