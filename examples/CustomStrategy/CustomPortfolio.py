# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange
from vnpy.trader.utility import ArrayManager
from datetime import  datetime
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
import re
from vnpy.app.cta_strategy.base import (DAILY_DB_NAME, DOMINANT_DB_NAME)
from CustomStrategy import CustomSignal

class CustomPortfolio(object):
    def __init__(self, engine):
        self.engine = engine
        self.signal_dict = defaultdict(list)

        self.portfolioValue = 0         # 组合市值
        self.trading_dict = {}          # 交易中的信号字典
        self.pos_dict = {}              # 真实持仓量字典
    
    def init(self, portfolioValue, signal_setting_list):
        self.portfolioValue = portfolioValue
        
        for symbol_setting in signal_setting_list:
            signal = CustomSignal(self, symbol_setting)
            symbol = symbol_setting["symbol"]
            l = self.signal_dict[symbol]
            l.append(signal)
            
            self.pos_dict[symbol] = 0
    
    def on_bar(self, bar):
        for signal in self.signal_dict[bar.symbol]:
            signal.on_bar(bar)
    
    def sendOrder(self, signal, direction, offset, price, volume):
        # ------------------------------------
        # 获取当前交易中的信号，如果不是本信号，则忽略
        currentSignal = self.trading_dict.get(signal.symbol, None)
        if currentSignal and currentSignal is not signal:
            return

        if offset == Offset.OPEN:
            # 开仓则缓存该信号的交易状态
            self.trading_dict[signal.symbol] = signal

        else:
            # 平仓则清除该信号
            self.trading_dict.pop(signal.symbol)
        # ------------------------------------

        # 计算合约持仓
        if direction == Direction.LONG:
            self.pos_dict[signal.symbol] += volume

        else:
            self.pos_dict[signal.symbol] -= volume
        
        # 向回测引擎中发单记录
        self.engine.sendOrder(signal.symbol, direction, offset, price, volume)
    