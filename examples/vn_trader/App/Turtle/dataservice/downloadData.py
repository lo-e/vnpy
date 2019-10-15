# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from .dataService import *
from csv import DictReader
import re
from datetime import datetime

class TurtleDataDownloading(object):
    def __init__(self):
        pass

    def download(self):
        # Mode  'all'所有合约；'main'主要合约；'current'当前模拟/实盘合约；'test'测试合约
        downloadMode = 'current'
        allSymbolList = ['RB', 'CU', 'NI', 'ZN', 'RU', 'AL', 'HC', 'J', 'I', 'PP', 'AP', 'TA', 'A', 'AG', 'AU', 'B',
                         'BB', 'BU', 'C', 'CF', 'CS', 'CY', 'EG', 'FB', 'FG', 'FU', 'JD', 'JM', 'JR', 'L', 'LR', 'M',
                         'MA', 'OI', 'P', 'PB', 'PM', 'RI', 'RM', 'RS', 'SC', 'SF', 'SM', 'SN', 'SP', 'SR', 'V', 'WH',
                         'WR', 'Y', 'ZC', 'IF', 'IC', 'IH']
        mainSymbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']
        currentSymbolList = ['RB', 'HC', 'I', 'SM', 'J', 'ZC', 'TA', 'RU', 'M']
        testSymbolList = ['AL']

        if downloadMode == 'all':
            symbolList = allSymbolList
        elif downloadMode == 'main':
            symbolList = mainSymbolList
        elif downloadMode == 'current':
            symbolList = currentSymbolList
        elif downloadMode == 'test':
            symbolList = testSymbolList
        else:
            print(u'模式设置错误！')
            exit(0)

        result = True
        return_msg = ''
        # """
        msg = '【下载真实主力合约代码到数据库】'
        return_msg += msg + '\n'
        print(msg)
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
        msg = '完成！\n'
        return_msg += msg + '\n'
        print(msg)
        """

        """
        msg = '【显示最新主力合约代码】'
        return_msg += msg + '\n'
        print(msg)
        for underlyingSymbol in symbolList:
            date, symbol = showDominantSymbol(underlyingSymbol)
            date = date.to_pydatetime()
            msg = f'{symbol}      {date}'
            return_msg += msg + '\n'
            print(msg)
            if date <= datetime.now():
                result = False

        return_msg += '\n\n'
        print('\n\n')

        """

        """
        msg = '【下载近一年真实主力合约bar数据到数据库】'
        return_msg += msg + '\n'
        print(msg)
        startDate = None;
        startDate = datetime.now().replace(year=datetime.now().year - 1)
        for underlyingSymbol in symbolList:
            if startDate:
                msg = downloadDominantSymbol(underlyingSymbol, startDate)
                return_msg += msg + '\n'
            else:
                msg = downloadDominantSymbol(underlyingSymbol)
                return_msg += msg + '\n'
        msg = f'{symbolList} 完成\n'
        return_msg += msg + '\n'
        print(msg)
        """

        """
        msg = '【下载指数、主力连续合约】'
        return_msg += msg + '\n'
        print(msg)
        # input = raw_input(u'输入合约类型【88主力 888平滑主力 99指数】')
        input = '99'
        count = 0
        if input == '88' or input == '888' or input == '99':
            for symbol in symbolList:
                symbol = symbol + input
                msg = downloadDailyBarBySymbol(symbol)
                count += 1
                return_msg += msg + '\n'
                print('\n')

            msg = f'合约数：{count}'
            return_msg += msg + '\n'
            print(msg)
        # """

        if not result:
            return_msg = f'======\n数据未更新\n======\n\n' + return_msg
        return result, return_msg

if __name__ == '__main__':
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
    turtleD = TurtleDataDownloading()
    turtleD.download()
    #"""