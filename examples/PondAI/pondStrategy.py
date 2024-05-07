# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange
from vnpy.trader.utility import ArrayManager
from datetime import  datetime
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
from vnpy.app.cta_strategy.base import (DAILY_DB_NAME, DOMINANT_DB_NAME)

class TradeResult(object):
    """ 一次完整的开平交易 """
    def __init__(self):
        self.unit = 0
        self.entry = 0                  # 开仓均价
        self.exit = 0                   # 平仓均价
        self.pnl = 0                    # 盈亏
    
    def open(self, price, change):
        """ 开仓或者加仓 """
        cost = self.unit * self.entry    # 计算之前的开仓成本
        cost += change * price           # 加上新仓位的成本
        self.unit += change              # 加上新仓位的数量
        self.entry = cost / self.unit    # 计算新的平均开仓成本

    def close(self, price):
        """ 平仓 """
        self.exit = price
        self.pnl = self.unit * (self.exit - self.entry)
    
class PondSignal(object):
    def __init__(self, portfolio, symbol):
        self.portfolio = portfolio      # 投资组合
        self.symbol = symbol            # 合约代码
        self.pos = 0                    # 信号持仓
        self.bar = None                 # 最新K线
        self.start = False              # 初始交易确定

        self.signal_df = self.portfolio.engine.symbol_signal_dict[self.symbol]  # 预测信号数据
        self.end_prediction_dt = self.signal_df["datetime"].iloc[-1]            # 预测信号最后一条数据的datetime

    def onBar(self, bar):
        if not bar.check_valid():
            # raise('Bar数据校验不通过！！')
            return
        
        self.bar = bar
        self.generateSignal(bar)

    def generateSignal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        target_prediction_dt = bar.datetime
        if target_prediction_dt >= self.end_prediction_dt:
            # 预测信号的最后时间平仓
            if self.pos > 0:
                self.sell(bar.close_price, abs(self.pos))

            elif self.pos < 0:
                self.cover(bar.close_price, abs(self.pos))

            return

        target_prediction_data = None
        filtered_data = self.signal_df[self.signal_df["datetime"] <= target_prediction_dt]
        if not filtered_data.empty:
            if not self.start:
                next_rows = self.signal_df[self.signal_df["datetime"] > target_prediction_dt]
                if not next_rows.empty:
                    self.start = True
                    
                else:
                    raise("预测信号无法确定（未初始交易)")
            
            target_prediction_data = filtered_data.iloc[-1]
            prediction = target_prediction_data["prediction"]
            prediction = float(prediction)
            open_volume = self.portfolio.portfolioValue / (len(self.portfolio.engine.symbolList) * bar.close_price)
            if prediction == 0:
                if self.pos > 0:
                    # 卖出平仓
                    self.sell(bar.close_price, abs(self.pos))

                elif self.pos < 0:
                    # 买入平仓
                    self.cover(bar.close_price, abs(self.pos))

                # print(f"{self.symbol}\t{bar.datetime}\t平仓")
            
            elif prediction > 0:
                if self.pos == 0:
                    # 买入开仓
                    self.buy(bar.close_price, abs(open_volume))
                
                if self.pos < 0:
                    # 先买入平仓
                    self.cover(bar.close_price, abs(self.pos))

                    # 再买入开仓
                    self.buy(bar.close_price, abs(open_volume))

                # print(f"{self.symbol}\t{bar.datetime}\t多")

            elif prediction < 0:
                if self.pos == 0:
                    # 卖出开仓
                    self.short(bar.close_price, abs(open_volume))
                
                if self.pos > 0:
                    # 先卖出平仓
                    self.sell(bar.close_price, abs(self.pos))

                    # 再卖出开仓
                    self.short(bar.close_price, abs(open_volume))

                # print(f"{self.symbol}\t{bar.datetime}\t空")
                
        elif self.start:
            raise("预测信号无法确定")
    
    def buy(self, price, volume):
        """ 买入开仓 """
        self.newSignal(Direction.LONG, Offset.OPEN, price, volume)
    
    def sell(self, price, volume):
        """ 卖出平仓 """
        self.newSignal(Direction.SHORT, Offset.CLOSE, price, volume)
    
    def short(self, price, volume):
        """ 卖出开仓 """
        self.newSignal(Direction.SHORT, Offset.OPEN, price, volume)
    
    def cover(self, price, volume):
        """ 买入平仓 """
        self.newSignal(Direction.LONG, Offset.CLOSE, price, volume)
    
    def newSignal(self, direction, offset, price, volume):
        self.portfolio.newSignal(self, direction, offset, price, volume)

class PondPortfolio(object):
    def __init__(self, engine):
        self.engine = engine
        
        self.signalDict = defaultdict(list)
        self.tradingDict = {}       # 交易中的信号字典
        self.posDict = {}           # 真实持仓量字典

        self.portfolioValue = 0     # 组合市值
    
    def init(self, portfolioValue, symbolList):
        self.portfolioValue = portfolioValue

        for symbol in symbolList:
            signal = PondSignal(self, symbol)
            l = self.signalDict[symbol]
            l.append(signal)

            self.posDict[symbol] = 0
    
    def onBar(self, bar):
        for signal in self.signalDict[bar.symbol]:
            signal.onBar(bar)
    
    def newSignal(self, signal, direction, offset, price, volume):
        """对交易信号进行过滤，符合条件的才发单执行"""
        # 过滤虚假开仓
        if not volume:
            return

        # 获取当前交易中的信号，如果不是本信号，则忽略
        currentSignal = self.tradingDict.get(signal.symbol, None)
        if currentSignal and currentSignal is not signal:
            return

        if offset == Offset.OPEN:
            # 开仓则缓存该信号的交易状态
            self.tradingDict[signal.symbol] = signal

        else:
            # 平仓则清除该信号
            self.tradingDict.pop(signal.symbol)

        # 策略持仓更新
        if direction == Direction.LONG:
            self.posDict[signal.symbol] += volume
            signal.pos += volume

        else:
            self.posDict[signal.symbol] -= volume
            signal.pos -= volume

        # 向回测引擎中发单记录
        self.engine.sendOrder(signal.symbol, direction, offset, price, volume)
    