# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from .dataService import *
from .tushareService import *
from .joinquantService import *
from datetime import datetime

class TurtleDataDownloading(object):
    def __init__(self):
        pass

    def download(self, symbol_list:list=None):
        symbolList = None
        if symbol_list:
            symbolList = symbol_list
        else:
            # Mode  'all'所有合约；'main'主要合约；'current'当前模拟/实盘合约；'test'测试合约
            downloadMode = 'current'
            allSymbolList = ['RB', 'CU', 'NI', 'ZN', 'RU', 'AL', 'HC', 'J', 'I', 'PP', 'AP', 'TA', 'A', 'AG', 'AU', 'B',
                             'BB', 'BU', 'C', 'CF', 'CS', 'CY', 'EG', 'FB', 'FG', 'FU', 'JD', 'JM', 'JR', 'L', 'LR', 'M',
                             'MA', 'OI', 'P', 'PB', 'PM', 'RI', 'RM', 'RS', 'SC', 'SF', 'SM', 'SN', 'SP', 'SR', 'V', 'WH',
                             'WR', 'Y', 'ZC', 'IF', 'IC', 'IH']
            mainSymbolList = ['IF', 'IC', 'IH', 'AL', 'RB', 'I', 'HC', 'SM', 'JM', 'J', 'ZC', 'TA']
            currentSymbolList = ['RB', 'HC', 'SM', 'J', 'ZC', 'TA', 'I', 'RU', 'IF']
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

    def download_tushare(self, symbol_list: list = None):
        #"""
        underlying_list = ['RB', 'HC', 'SM', 'J', 'ZC', 'TA']
        days = 2
        today = datetime.strptime(datetime.now().strftime('%Y%m%d'), '%Y%m%d')

        result = True
        return_msg = ''

        # 获取主力合约代码并存入数据库
        dominant_start_msg = '====== 获取主力合约代码并存入数据库 ======'
        print(dominant_start_msg)
        return_msg += dominant_start_msg + '\n'

        from_date = today - timedelta(days=days)
        # from_date = datetime.strptime('2019-12-12', '%Y-%m-%d')
        for target_symbol in underlying_list:
            dominant_msg = get_and_save_dominant_symbol_from(symbol=target_symbol, from_date=from_date)
            return_msg += dominant_msg + '\n\n'
            print('\n')


        # 下载最近两个主力合约的日线数据
        download_daily_msg = '====== 下载最近两个主力合约的日线数据 ======'
        print(download_daily_msg)
        return_msg += '\n' + download_daily_msg + '\n'

        downloaded_bar_datetime_list = []
        for underlying_symbol in underlying_list:
            # 数据库查询最近两个主力合约
            collection = dbDominant[underlying_symbol]
            cursor = collection.find().sort('date', direction=DESCENDING)
            symbol_list = []
            for dic in cursor:
                symbol = dic['symbol']
                symbol_list.append(symbol)
                if len(symbol_list) >= 2:
                    break

            # 下载指定天数的日线数据
            start = (today - timedelta(days=days)).strftime('%Y%m%d')
            end = today.strftime('%Y%m%d')
            for symbol in symbol_list:
                ts_code = trasform_tscode(symbol=symbol)
                bar_list, msg = downloadDailyData(ts_code=ts_code, start=start, end=end, to_database=True)
                for downloaded_bar in bar_list:
                    if downloaded_bar.datetime not in downloaded_bar_datetime_list:
                        downloaded_bar_datetime_list.append(downloaded_bar.datetime)
                print(msg)
                return_msg += msg + '\n'
            print('\n')
            return_msg += '\n'

        # 添加指数日线数据到数据库【RB99】
        index_daily_msg = '====== 添加指数日线数据到数据库 ======'
        print(index_daily_msg)
        return_msg += '\n' + index_daily_msg + '\n'

        add_date_list = []
        for downloaded_datetime in downloaded_bar_datetime_list:
            for underlying_symbol in underlying_list:
                symbol = underlying_symbol + '99'
                bar = BarData(gateway_name='', symbol=symbol, exchange='', datetime=downloaded_datetime,
                              endDatetime=None)
                collection = dbDaily[bar.symbol]
                collection.update_many({'datetime': bar.datetime}, {'$set': bar.__dict__}, upsert=True)
            add_date_list.append(downloaded_datetime.strftime('%Y-%m-%d'))

        msg = ''
        for date_str in add_date_list:
            if msg:
                msg += '\t' + date_str
            else:
                msg += date_str
        print(f'【{msg}】')
        return_msg += msg

        if not len(downloaded_bar_datetime_list):
            result = False
        if not result:
            return_msg = f'======\n数据未更新\n======\n\n' + return_msg
        return result, return_msg
        #"""

        """
        # 下载Daily_Bar数据
        bar_list, msg = downloadDailyData(ts_code='RBL.SHF', start='', end='')
        print(msg)
        """

        """
        # 添加空的Daily_Bar数据到数据库
        symbol = 'A0000'
        the_datetime = datetime.strptime('2019-12-13', '%Y-%m-%d')
        bar = BarData(gateway_name='', symbol=symbol, exchange='', datetime=the_datetime, endDatetime=None)
        collection = dbDaily[bar.symbol]
        collection.insert_one(bar.__dict__)
        """

        """
        # 添加空的Dominant数据到数据库
        underline = 'HC'
        symbol = 'HC2005'
        date = datetime.strptime('2019-12-13', '%Y-%m-%d')
        collection = dbDominant[underline]
        collection.insert_one({'symbol':symbol,
                               'date':date})
        """

    def download_jq(self, symbol_list: list = None):
        #"""
        underlying_list = ['RB', 'HC', 'SM', 'J', 'ZC', 'TA', 'I', 'RU']
        #underlying_list = ['CF', 'CJ', 'CS', 'CU', 'CY', 'EG', 'OI', 'PB', 'RM', 'SC', 'SF', 'SM', 'SN', 'SP', 'SR', 'TA', 'WR', 'ZC', 'ZN', 'TF', 'TS', 'FG', 'JM', 'RI', 'RU', 'SA', 'NR']
        days = 0
        today = datetime.strptime(datetime.now().strftime('%Y%m%d'), '%Y%m%d')

        result = True
        return_msg = ''

        # 获取主力合约代码并存入数据库
        dominant_start_msg = '====== 获取主力合约代码并存入数据库 ======'
        print(dominant_start_msg)
        return_msg += dominant_start_msg + '\n'

        from_date = today - timedelta(days=days)
        # from_date = datetime.strptime('2019-12-12', '%Y-%m-%d')
        for underlying_symbol in underlying_list:
            dominant_msg = jq_get_and_save_dominant_symbol_from(underlying_symbol=underlying_symbol, from_date=from_date)
            return_msg += dominant_msg + '\n\n'
            print('\n')

        # 下载最近两个主力合约的日线数据
        download_daily_msg = '====== 下载最近两个主力合约的日线数据 ======'
        print(download_daily_msg)
        return_msg += '\n' + download_daily_msg + '\n'

        downloaded_bar_datetime_list = []
        for underlying_symbol in underlying_list:
            # 数据库查询最近两个主力合约
            collection = dbDominant[underlying_symbol]
            cursor = collection.find().sort('date', direction=DESCENDING)
            symbol_list = []
            for dic in cursor:
                symbol = dic['symbol']
                symbol_list.append(symbol)
                if len(symbol_list) >= 2:
                    break

            # 下载指定天数的日线数据
            start = (today - timedelta(days=days)).strftime('%Y-%m-%d')
            end = today.strftime('%Y-%m-%d')
            for symbol in symbol_list:
                bar_list, msg = download_bar_data(symbol=symbol, start=start, end=end, to_database=True)
                for the_bar in bar_list:
                    if the_bar.datetime not in downloaded_bar_datetime_list:
                        downloaded_bar_datetime_list.append(the_bar.datetime)
                print(msg)
                return_msg += msg + '\n'
            print('\n')
            return_msg += '\n'

        # 添加指数日线数据到数据库【RB99】
        index_msg = '====== 添加指数日线数据到数据库 ======'
        print(index_msg)
        return_msg += '\n' + index_msg + '\n'

        add_date_list = []
        for downloaded_datetime in downloaded_bar_datetime_list:
            for underlying_symbol in underlying_list:
                symbol = underlying_symbol + '99'
                bar = BarData(gateway_name='', symbol=symbol, exchange='', datetime=downloaded_datetime,
                              endDatetime=None)
                collection = dbDaily[bar.symbol]
                collection.update_many({'datetime': bar.datetime}, {'$set': bar.__dict__}, upsert=True)
            add_date_list.append(downloaded_datetime.strftime('%Y-%m-%d'))
        msg = ''
        for date_str in add_date_list:
            if msg:
                msg += '\t' + date_str
            else:
                msg += date_str
        print(f'【{msg}】')
        return_msg += msg

        # 打印和返回结果
        if not len(downloaded_bar_datetime_list):
            result = False
        if not result:
            return_msg = f'======\n数据未更新\n======\n\n' + return_msg
        return result, return_msg
        #"""

    def download_minute_jq(self, symbol_list:list=None, days=1):
        return_msg = ''
        last_datetime = None

        if not symbol_list:
            symbol_list = ['CF2009', 'CF2005',
                           'CS2009', 'CS2005',
                           'CJ2009', 'CJ2005',
                           'CU2005', 'CU2004',
                           'CY2009', 'CY2005',
                           'EG2009', 'EG2005',
                           'OI2009', 'OI2005',
                           'PB2006', 'PB2005',
                           'RM2009', 'RM2005',
                           'SC2006', 'SC2005',
                           'SF2009', 'SF2005',
                           'SM2009', 'SM2005',
                           'SN2009', 'SN2005',
                           'SP2009', 'SP2005',
                           'SR2009', 'SR2005',
                           'TA2009', 'TA2005',
                           'WR2010', 'WR2005',
                           'ZC2009', 'ZC2005',
                           'ZN2005', 'ZN2004',
                           'TF2009', 'TF2006',
                           'TS2009', 'TS2006',
                           'FG2009', 'FG2005',
                           'JM2009', 'JM2005',
                           'RU2009', 'RU2005',
                           'SA2009', 'SA2005',
                           'NR2006', 'NR2005']
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        next_day = today + timedelta(days=1)
        start = today - timedelta(days=days)
        end = start + timedelta(days=1)
        while end <= next_day:
            if end == next_day:
                end = datetime.now()
            for symbol in symbol_list:
                bar_list, msg = download_bar_data(symbol=symbol, start=start.strftime('%Y-%m-%d %H:%M:%S'), end=end.strftime('%Y-%m-%d %H:%M:%S'), frequency='1m', to_database=True)
                if bar_list:
                    last_datetime = bar_list[-1].datetime
                print(msg)
                return_msg = return_msg + msg + '\n'
            start = end
            end = end + timedelta(days=1)
            return_msg += '\n'

        return last_datetime, return_msg

    def download_all_minute_jq(self, symbol_list:list=None, days=1):
        return_msg = ''
        last_datetime = None

        if not symbol_list:
            symbol_list = []

        for symbol in symbol_list:
            bar_list, msg = download_bar_data(symbol=symbol, start='', end='', frequency='1m', to_database=True)
            if bar_list:
                last_datetime = bar_list[-1].datetime
            print(msg)
            return_msg = return_msg + msg + '\n'

        return last_datetime, return_msg