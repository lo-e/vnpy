from dataservice import *

if __name__ == '__main__':
    #"""
    dataDownload = TurtleDataDownloading()
    dataDownload.download()
    #"""

    """
    # 下载测试中的指数、主力合约
    input = input(u'输入合约类型【88主力 888平滑主力 99指数】')
    if input == '88' or input == '888' or input == '99':
        filename = 'symbol_list.csv'
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
    symbol = 'IF88'
    start = '2019-10-27'
    end = '2019-10-29'
    downloadMinuteBarBySymbol(symbol, start=start, end=end)
    """