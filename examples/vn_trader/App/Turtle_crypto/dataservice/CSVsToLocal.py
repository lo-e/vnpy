#-- coding: utf-8 --

import pymongo
import datetime
from vnpy.trader.object import TickData, BarData
from vnpy.app.cta_strategy.base import TICK_DB_NAME, DAILY_DB_NAME, MinuteDataBaseName, HourDataBaseName
import os
import csv
from time import time
from sys import exit
import re
from .OneTokenDataService import get_csv_path
from vnpy.trader.utility import DIR_SYMBOL

class CSVs1TokenBarLocalEngine(object):
    def __init__(self, duration:str):
        super(CSVs1TokenBarLocalEngine, self).__init__()
        # 周期
        self.duration = duration
        # 项目路径
        self.walkingDir = get_csv_path()
        # 获取数据库
        self.client = pymongo.MongoClient('localhost', 27017)

    def startWork(self):
        totalCount = 0
        totalStartTime = time()
        for root, subdirs, files in os.walk(self.walkingDir):
            for theFile in files:
                count = 0
                startTime = time()

                # 排除不合法文件
                if theFile.startswith('.'):
                    continue

                if DIR_SYMBOL in root:
                    dirName = root.split(DIR_SYMBOL)[-1]

                if '.csv' in theFile and dirName == self.duration:
                    if dirName == '1d':
                        db_name = DAILY_DB_NAME
                    elif dirName == '1m' or dirName == '5m' or dirName == '15m' or dirName == '30m':
                        duration = re.sub("\D", "", dirName)
                        db_name = MinuteDataBaseName(duration)
                    else:
                        print('输入的周期不正确！！')
                        exit(0)
                    self.bar_db = self.client[db_name]
                    # 读取文件
                    filePath = root + DIR_SYMBOL + theFile
                    with open(filePath, 'r') as f:
                        reader = csv.DictReader(f)
                        # 开始导入数据
                        for row in reader:
                            # 合约
                            symbol = row['symbol']
                            if not symbol:
                                print('合约不存在')
                                continue

                            # 确定日期
                            date = row['datetime']
                            if not date:
                                print('日期不存在')
                                continue
                            try:
                                theDatetime = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                            except Exception:
                                print(f'日期转换异常：{date}')
                                raise
                                exit(0)

                            count += 1
                            totalCount += 1
                            if count == 1:
                                print('=' * 6, symbol, '\t', theFile, '=' * 6)

                            # 转换symbol
                            symbolElements = symbol.split('/')
                            if len(symbolElements) < 2:
                                exit(0)
                            symbol = symbolElements[1].upper() + '.' + symbolElements[0].upper()
                            # 数据库collection
                            collection = self.bar_db[symbol]
                            collection.create_index('datetime')
                            # 创建BarData对象
                            bar = BarData(gateway_name='', symbol=symbol, exchange=None, datetime=theDatetime, endDatetime=None)
                            bar.open_price = float(row['open'])
                            bar.close_price = float(row['close'])
                            bar.high_price = float(row['high'])
                            bar.low_price = float(row['low'])
                            bar.volume = float(row['volume'])
                            # 保存bar到数据库
                            collection.update_many({'datetime': bar.datetime}, {'$set': bar.__dict__}, upsert = True)
                # 打印进程
                if count:
                    sub = time() - startTime
                    print('用时：', sub, 's')
                    print('数据量：', count, '\n')
                    """ fake """
                    if count < 1440:
                        print('*'*60, '\n')

        # 打印进程
        print('所有数据导入完成')
        if totalCount:
            sub = time() - totalStartTime
            print('总用时：', sub, 's')
            print(u'总数据量：', totalCount, '\n')

class CSVsBybitBarLocalEngine(object):
    def __init__(self, duration:str):
        super(CSVsBybitBarLocalEngine, self).__init__()
        # 周期
        self.duration = duration
        # 项目路径
        self.walkingDir = get_csv_path()
        # 获取数据库
        self.client = pymongo.MongoClient('localhost', 27017)

    def startWork(self):
        totalCount = 0
        totalStartTime = time()
        for root, subdirs, files in os.walk(self.walkingDir):
            for theFile in files:
                count = 0
                startTime = time()

                # 排除不合法文件
                if theFile.startswith('.'):
                    continue

                if DIR_SYMBOL in root:
                    dirName = root.split(DIR_SYMBOL)[-1]

                if '.csv' in theFile and dirName == self.duration:
                    if dirName == 'D':
                        db_name = DAILY_DB_NAME
                    elif dirName == '1' or dirName == '3' or dirName == '5' or dirName == '15' or dirName == '30' or dirName == '60':
                        db_name = MinuteDataBaseName(self.duration)
                    else:
                        print('输入的周期不正确！！')
                        exit(0)
                    self.bar_db = self.client[db_name]
                    # 读取文件
                    filePath = root + DIR_SYMBOL + theFile
                    with open(filePath, 'r') as f:
                        reader = csv.DictReader(f)
                        # 开始导入数据
                        for row in reader:
                            # 合约
                            symbol = row['symbol']
                            if not symbol:
                                print('合约不存在')
                                continue

                            # 确定日期
                            date = row['datetime']
                            if not date:
                                print('日期不存在')
                                continue
                            try:
                                theDatetime = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                            except Exception:
                                print(f'日期转换异常：{date}')
                                raise
                                exit(0)

                            count += 1
                            totalCount += 1
                            if count == 1:
                                print('=' * 6, symbol, '\t', theFile, '=' * 6)

                            # 转换symbol
                            symbol = f'{symbol}.BYBIT'
                            # 数据库collection
                            collection = self.bar_db[symbol]
                            collection.create_index('datetime')
                            # 创建BarData对象
                            bar = BarData(gateway_name='', symbol=symbol, exchange=None, datetime=theDatetime, endDatetime=None)
                            bar.open_price = float(row['open'])
                            bar.close_price = float(row['close'])
                            bar.high_price = float(row['high'])
                            bar.low_price = float(row['low'])
                            bar.volume = float(row['volume'])
                            # 保存bar到数据库
                            collection.update_many({'datetime': bar.datetime}, {'$set': bar.__dict__}, upsert = True)
                # 打印进程
                if count:
                    sub = time() - startTime
                    print('用时：', sub, 's')
                    print('数据量：', count, '\n')
                    """ fake """
                    if count < 200:
                        print('*'*60, '\n')

        # 打印进程
        print('所有数据导入完成')
        if totalCount:
            sub = time() - totalStartTime
            print('总用时：', sub, 's')
            print(u'总数据量：', totalCount, '\n')