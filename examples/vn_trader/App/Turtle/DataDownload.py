from dataservice import *
from csv import DictReader
import re

if __name__ == '__main__':
    #"""
    # 下载实盘所需数据
    dataDownload = TurtleDataDownloading()
    #dataDownload.download()
    #dataDownload.download_tushare()
    #dataDownload.download_jq()
    #dataDownload.download_minute_jq()
    dataDownload.download_all_minute_jq()
    #"""

    """
    # 下载【symbol_list.csv】中的指数、主力合约
    input = input(u'输入合约类型【88主力 888平滑主力 99指数】')
    if input == '88' or input == '888' or input == '99':
        filename = 'dataservice\\symbol_list.csv'
        count = 0
        with open(filename, errors='ignore') as f:
            r = DictReader(f)
            for d in r:
                startSymbol = re.sub("\d", "", d['vtSymbol'])
                symbol = startSymbol + input
                downloadDailyBarBySymbol(symbol)
                count += 1
                print('\n')
        print(f'合约数：{count}')
    """

    """
    # 下载分钟数据
    symbol = 'IF88'
    start = '2019-11-01'
    end = '2019-12-31'
    downloadMinuteBarBySymbol(symbol, start=start, end=end)
    """

    """
    # 下载Tick数据
    symbol = 'SM2001'
    date = '2019-11-15'
    downloadTickBySymbol(symbol=symbol, date=date)
    """

