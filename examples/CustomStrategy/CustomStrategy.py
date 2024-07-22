# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange
from vnpy.trader.utility import ArrayManager
from datetime import  datetime, timedelta
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
import re
from vnpy.app.cta_strategy.base import (DAILY_DB_NAME, DOMINANT_DB_NAME)

class CustomResult(object):
    """一次完整的开平交易"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.unit = 0
        self.entry = 0                  # 开仓均价
        self.exit = 0                   # 平仓均价
        self.pnl = 0                    # 盈亏
    
    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓或者加仓"""
        cost = self.unit * self.entry    # 计算之前的开仓成本
        cost += change * price           # 加上新仓位的成本
        self.unit += change              # 加上新仓位的数量
        self.entry = cost / self.unit    # 计算新的平均开仓成本

    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.exit = price
        self.pnl = self.unit * (self.exit - self.entry)
    
class CustomSignal(object):
    
    def __init__(self, portfolio, symbol):
        self.portfolio = portfolio                      # 投资组合
        self.symbol = symbol                            # 合约代码
        self.am = ArrayManager(5)                       # K线容器

        """ fake """
        self.next_dt = None

    def on_bar(self, bar):
        print(f"{bar.symbol}\t{bar.datetime}")
        
        if self.next_dt:
            if bar.datetime != self.next_dt:
                print("error")
        self.next_dt = bar.datetime + timedelta(minutes=1)

