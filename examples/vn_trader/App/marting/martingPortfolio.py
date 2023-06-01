# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset
import re
from datetime import datetime
from copy import copy
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from time import time

MAX_PRODUCT_POS = 4  # 单品种最大持仓
MAX_DIRECTION_POS = 12  # 单方向最大持仓


class MartingPortfolio(object):
    """马丁组合"""

    name = ""

    # 参数
    portfolioValue = 0  # 组合市值

    # 变量
    unitDict = defaultdict(int)  # 每个品种的持仓情况
    totalLong = 0  # 总的多头持仓
    totalShort = 0  # 总的空头持仓
    today = None

    paramList = ["name", "portfolioValue"]

    varList = ["today", "is_downloading", "downloading_cost", "downloading_wait"]

    syncList = ["today"]

    def __init__(self, engine, setting):
        """Constructor"""
        self.engine = engine
        self.on_update_today()

        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()  # 数据下载引擎
        self.download_enable = False  # 允许下载开关
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

    def onBar(self, bar):
        """"""
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar)

    def newSignal(self, vtSymbol, direction, offset):
        """对交易信号进行过滤，符合条件的才发单执行"""

        # 开仓
        if offset == Offset.OPEN:
            # 买入
            if direction == Direction.LONG:
                # 组合持仓不能超过上限
                if self.totalLong >= MAX_DIRECTION_POS:
                    return False

                # 单品种持仓不能超过上限
                if self.unitDict.get(vtSymbol, 0) >= MAX_PRODUCT_POS:
                    return False

            # 卖出
            else:
                if self.totalShort <= -MAX_DIRECTION_POS:
                    return False

                if self.unitDict.get(vtSymbol, 0) <= -MAX_PRODUCT_POS:
                    return False

        self.updateUnit(vtSymbol, direction, offset)
        return True

    def updateUnit(self, vtSymbol, direction, offset):
        """"""

        # 计算合约持仓
        if offset == Offset.OPEN:
            if direction == Direction.LONG:
                self.unitDict[vtSymbol] = self.unitDict.get(vtSymbol, 0) + 1

            else:
                self.unitDict[vtSymbol] = self.unitDict.get(vtSymbol, 0) - 1
        else:
            if vtSymbol in self.unitDict.keys():
                self.unitDict.pop(vtSymbol)

        # 计算总持仓、类别持仓
        self.totalLong = 0
        self.totalShort = 0

        for symbol, unit in self.unitDict.items():
            # 总持仓
            if unit > 0:
                self.totalLong += unit
            elif unit < 0:
                self.totalShort += unit

        # 同步到数据库
        self.engine.savePortfolioSyncData()

    def on_update_today(self):
        self.today = copy(self.engine.today)
        # 同步到数据库
        self.engine.savePortfolioSyncData()

    def on_timer(self):
        # 下载等待
        if not len(self.download_engine.threads):
            self.is_downloading = False
            self.downloading_wait += 1
            if self.downloading_time:
                self.downloading_cost = int(time() - self.downloading_time)
                self.downloading_time = 0

        #  满足条件开始下载
        if self.download_enable and self.downloading_wait >= 5 * 60:
            self.is_downloading = True
            self.downloading_wait = 0
            self.downloading_time = time()

            contract_list = []
            for symbol in self.strategy_symbols:
                contract_list.append(symbol.split(".")[0])
            self.download_engine.download_from_bybit(
                contract_list=contract_list, from_data_base=True
            )

        # 组合状态更新
        self.engine.put_portfolio_event()
