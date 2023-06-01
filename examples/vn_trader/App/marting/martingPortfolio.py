# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset
import re
from datetime import datetime
from copy import copy
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from time import time
from threading import Thread

MAX_PRODUCT_POS = 4  # 单品种最大持仓
MAX_DIRECTION_POS = 12  # 单方向最大持仓


class MartingPortfolio(object):
    """马丁组合"""

    # 参数
    name = ""
    portfolioValue = 0  # 组合市值

    # 变量
    today = None

    paramList = ["name", "portfolioValue"]
    varList = ["today", "is_downloading", "downloading_cost", "downloading_wait"]
    syncList = ["today"]

    def __init__(self, engine, setting):
        self.engine = engine
        self.on_update_today()

        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()  # 数据下载引擎
        self.is_downloading = False  # 是否正在下载
        self.downloading_wait = 10000  # 数据下载等待时间（秒）
        self.downloading_cost = 0 # 下载更新一次花费的时间
        self.downloading_time = 0 # 下载开始的时间戳

        # 策略合约列表
        self.strategy_symbols = []

        # 设置参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]

    def on_update_today(self):
        self.today = copy(self.engine.today)
        # 同步到数据库
        self.engine.savePortfolioSyncData()

    def on_timer(self):
        # fake
        self.strategy_symbols = ['SHIB1000USDT', 'AAVEUSDT', 'ADAUSDT', 'APEUSDT', 'ATOMUSDT', 'AVAXUSDT', 'BCHUSDT', 'CHZUSDT', 'CRVUSDT', 'DOGEUSDT', 'DOTUSDT', 'EOSUSDT', 'ETCUSDT', 'FILUSDT', 'LINKUSDT', 'LTCUSDT', 'MATICUSDT', 'NEARUSDT', 'SANDUSDT', 'SOLUSDT', 'SUSHIUSDT', 'UNIUSDT', 'XRPUSDT']
        
        # 下载结束
        if not len(self.download_engine.threads):
            if self.is_downloading:
                # 刚结束下载，开始生成window_bar
                pass

            self.is_downloading = False
            self.downloading_wait += 1
            self.downloading_time = 0
        
        # 下载花费时间计算
        if self.is_downloading:
            self.downloading_cost = int(time() - self.downloading_time)

        # 每隔设定的时间开始下载
        if self.downloading_wait >= 5 * 60:
            self.is_downloading = True
            self.downloading_wait = 0
            self.downloading_time = time()

            thread = Thread(target=self.download_data)
            thread.start()

        # 组合状态更新
        self.engine.put_portfolio_event()

    def download_data(self):
        contract_list = []
        for symbol in self.strategy_symbols:
            contract_list.append(symbol.split(".")[0])
        self.download_engine.download_from_bybit(
            contract_list=contract_list, from_data_base=True
        )
