#-- coding: utf-8 --

from pymongo import MongoClient, ASCENDING
from datetime import datetime, timedelta
from vnpy.trader.object import BarData
from vnpy.app.cta_strategy.base import TICK_DB_NAME, DAILY_DB_NAME, MinuteDataBaseName, HourDataBaseName

class BarLocalEngine(object):
    def __init__(self):
        super(BarLocalEngine, self).__init__()
        self.client = MongoClient('localhost', 27017, serverSelectionTimeoutMS = 600)

    def Crypto_1Min_Daily(self, symbol, start_date=None, end_date=None):
        result = True
        complete_msg = ''
        back_msg = ''
        lost_msg = ''

        Daily_db = self.client[DAILY_DB_NAME]
        Daily_collection = Daily_db[symbol]
        Min_db = self.client[MinuteDataBaseName(duration=1)]
        Min_collection = Min_db[symbol]

        flt = {}
        start_end = {}
        if start_date:
            start_end['$gte'] = start_date
        if end_date:
            start_end['$lte'] = end_date
        if start_end:
            flt = {'datetime': start_end}
        cursor = Min_collection.find(flt).sort('datetime', ASCENDING)
        next_datetime = None
        daily_bar = None
        daily_bar_datetime = None
        daily_bar_end = None
        for dic in cursor:
            the_datetime = dic['datetime']
            if not next_datetime:
                next_datetime = datetime.strptime(f'{the_datetime.year}-{the_datetime.month}-{the_datetime.day} 08:00:00',
                                                  '%Y-%m-%d %H:%M:%S')
                if the_datetime > next_datetime:
                    next_datetime += timedelta(days=1)
                daily_bar_datetime = next_datetime
                temp = daily_bar_datetime + timedelta(days=1)
                daily_bar_end = datetime.strptime(f'{temp.year}-{temp.month}-{temp.day} 07:59:00',
                                                  '%Y-%m-%d %H:%M:%S')
            if the_datetime == next_datetime:
                if the_datetime == daily_bar_datetime:
                    # Daily的开始
                    daily_bar = BarData(gateway_name='', symbol=dic['symbol'], exchange=None, datetime=daily_bar_datetime, endDatetime=None)
                    daily_bar.open_price = dic['open_price']
                    daily_bar.close_price = dic['close_price']
                    daily_bar.high_price = dic['high_price']
                    daily_bar.low_price = dic['low_price']
                else:
                    if not daily_bar:
                        raise('出现异常，检查代码！')

                    daily_bar.close_price = dic['close_price']
                    daily_bar.high_price = max(daily_bar.high_price, dic['high_price'])
                    daily_bar.low_price = min(daily_bar.low_price, dic['low_price'])

                next_datetime += timedelta(minutes=1)
                if the_datetime == daily_bar_end:
                    # Daily的结束
                    # 保存bar到数据库
                    msg = f'保存Daily数据：{symbol}\t{daily_bar_datetime}\n'
                    complete_msg += msg + '\n'
                    print(msg)
                    Daily_collection.update_many({'datetime': daily_bar.datetime}, {'$set': daily_bar.__dict__}, upsert=True)
                    daily_bar = None

                    daily_bar_datetime = next_datetime
                    temp = daily_bar_datetime + timedelta(days=1)
                    daily_bar_end = datetime.strptime(f'{temp.year}-{temp.month}-{temp.day} 07:59:00',
                                                      '%Y-%m-%d %H:%M:%S')

            elif the_datetime > next_datetime:
                # Daily数据中间缺失，到Tick数据库获取数据并合成
                d_bar, b_msg, l_msg = self.Crypto_Daily_With_Tick(symbol, next_datetime, the_datetime, daily_bar)
                daily_bar = d_bar
                back_msg += b_msg
                lost_msg += l_msg

                next_datetime = the_datetime + timedelta(minutes=1)
                if the_datetime <= daily_bar_end:
                    if not daily_bar:
                        # Daily的开始
                        daily_bar = BarData(gateway_name='', symbol=dic['symbol'], exchange=None, datetime=daily_bar_datetime,
                                            endDatetime=None)
                        daily_bar.open_price = dic['open_price']
                        daily_bar.close_price = dic['close_price']
                        daily_bar.high_price = dic['high_price']
                        daily_bar.low_price = dic['low_price']
                    else:
                        daily_bar.close_price = dic['close_price']
                        daily_bar.high_price = max(daily_bar.high_price, dic['high_price'])
                        daily_bar.low_price = min(daily_bar.low_price, dic['low_price'])

                    if the_datetime == daily_bar_end:
                        # Daily的结束
                        # 保存bar到数据库
                        msg = f'保存Daily数据：{symbol}\t{daily_bar_datetime}\n'
                        complete_msg += msg + '\n'
                        print(msg)
                        Daily_collection.update_many({'datetime': daily_bar.datetime}, {'$set': daily_bar.__dict__},
                                                     upsert=True)
                        daily_bar = None

                        daily_bar_datetime = next_datetime
                        temp = daily_bar_datetime + timedelta(days=1)
                        daily_bar_end = datetime.strptime(f'{temp.year}-{temp.month}-{temp.day} 07:59:00',
                                                          '%Y-%m-%d %H:%M:%S')

                else:
                    if daily_bar:
                        # Daily的结束
                        # 保存bar到数据库
                        msg = f'保存Daily数据：{symbol}\t{daily_bar_datetime}\n'
                        complete_msg += msg + '\n'
                        print(msg)
                        Daily_collection.update_many({'datetime': daily_bar.datetime}, {'$set': daily_bar.__dict__},
                                                     upsert=True)

                    daily_bar_datetime = datetime.strptime(
                        f'{the_datetime.year}-{the_datetime.month}-{the_datetime.day} 08:00:00',
                        '%Y-%m-%d %H:%M:%S')
                    temp = daily_bar_datetime + timedelta(days=1)
                    daily_bar_end = datetime.strptime(f'{temp.year}-{temp.month}-{temp.day} 07:59:00',
                                                      '%Y-%m-%d %H:%M:%S')

                    # Daily的开始
                    daily_bar = BarData(gateway_name='', symbol=dic['symbol'], exchange=None,
                                        datetime=daily_bar_datetime,
                                        endDatetime=None)
                    daily_bar.open_price = dic['open_price']
                    daily_bar.close_price = dic['close_price']
                    daily_bar.high_price = dic['high_price']
                    daily_bar.low_price = dic['low_price']

            else:
                if not daily_bar:
                    continue
                else:
                    raise ('出现异常，检查代码！')

        if next_datetime != daily_bar_datetime and daily_bar:
            # Daily数据结尾缺失，到Tick数据库获取数据并合成
            d_bar, b_msg, l_msg = self.Crypto_Daily_With_Tick(symbol, next_datetime, daily_bar_end + timedelta(minutes=1), daily_bar)
            if l_msg:
                result = False
            daily_bar = d_bar
            back_msg += b_msg
            lost_msg += l_msg

            # Daily的结束
            # 保存bar到数据库
            msg = f'保存Daily数据：{symbol}\t{daily_bar_datetime}\n'
            complete_msg += msg + '\n'
            print(msg)
            Daily_collection.update_many({'datetime': daily_bar.datetime}, {'$set': daily_bar.__dict__},
                                         upsert=True)

        return result, complete_msg, back_msg, lost_msg

    def Crypto_Daily_With_Tick(self, symbol, from_min, to_min, daily_bar):
        back_msg = ''
        lost_msg = ''

        elements = symbol.split('.')
        exchange = '.' + elements[-1]
        tick_symbol = symbol.rstrip(exchange)
        Tick_db = self.client[TICK_DB_NAME]
        Tick_collection = Tick_db[tick_symbol]

        next_min = from_min
        while next_min < to_min:
            # 到Tick数据库获取数据并合成
            tick_flt = {'datetime': {'$gte': next_min,
                                     '$lte': next_min + timedelta(minutes=1)}}
            tick_cursor = Tick_collection.find(tick_flt).sort('datetime', ASCENDING)
            min_bar = None
            tick_count = 0
            for tick_dic in tick_cursor:
                tick_datetime = tick_dic['datetime']
                tick_price = tick_dic['last_price']
                if tick_price == 0:
                    break
                if tick_datetime >= next_min + timedelta(minutes=1):
                    break

                tick_count += 1
                if not min_bar:
                    min_bar = BarData(gateway_name='', symbol='', exchange=None, datetime=next_min,
                                      endDatetime=None)
                    min_bar.open_price = tick_price
                    min_bar.high_price = tick_price
                    min_bar.low_price = tick_price
                    min_bar.close_price = tick_price
                else:
                    min_bar.close_price = tick_price
                    min_bar.high_price = max(min_bar.high_price, tick_price)
                    min_bar.low_price = min(min_bar.low_price, tick_price)
            if tick_count:
                if not daily_bar:
                    # Daily的开始
                    daily_bar = BarData(gateway_name='', symbol=symbol, exchange=None, datetime=daily_bar_datetime,
                                        endDatetime=None)
                    daily_bar.open_price = min_bar.open_price
                    daily_bar.close_price = min_bar.high_price
                    daily_bar.high_price = min_bar.low_price
                    daily_bar.low_price = min_bar.close_price
                else:
                    daily_bar.close_price = min_bar.close_price
                    daily_bar.high_price = max(daily_bar.high_price, min_bar.high_price)
                    daily_bar.low_price = min(daily_bar.low_price, min_bar.low_price)

                msg = f'****** {symbol}\t{next_min} TICK填补成功【{tick_count}】  ******'
                back_msg += msg + '\n'
            else:
                msg = f'!!!!!! {symbol}\t{next_min}缺失 !!!!!!'
                lost_msg += msg + '\n'

            next_min += timedelta(minutes=1)

        return daily_bar, back_msg, lost_msg


if __name__ == '__main__':
    #"""
    # 1Token_Bar
    engine = BarLocalEngine(duration='1m')
    symbol = 'ETH.USD.Q.OKEF'
    start_date = '2019-9-01'
    end_date = '2019-10-01'
    engine.Crypto_1Min_Daily(symbol=symbol, start_date=start_date, end_date=end_date)
    #"""