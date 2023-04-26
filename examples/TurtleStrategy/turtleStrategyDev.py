# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange
from vnpy.trader.utility import ArrayManager
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
import re
from vnpy.app.cta_strategy.base import DAILY_DB_NAME, DOMINANT_DB_NAME


########################################################################
class AISignal(object):
    # ----------------------------------------------------------------------
    def __init__(self, portfolio, symbol, direction, ma_window):
        # 常量
        self.portfolio = portfolio  # 投资组合
        self.symbol = symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.unit_value = self.portfolio.portfolioValue * 0.5 * 0.01 # 最小持仓价值
        self.symbol_min_volume = self.portfolio.engine.min_volume_dict[self.symbol] # 合约最小交易数量
        
        # 变量
        self.bar:BarData = None  # 最新K线
        self.am = ArrayManager(self.ma_window + 1)  # K线容器
        self.position = 0 # 持仓量
        self.position_price = 0  # 持仓均价
        self.position_reduce_price = 0 # 减仓价格
        self.position_increase_price = 0 # 加仓价格
        self.ma_price = 0  # 均线价格

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        if not bar.check_valid():
            raise ("Bar数据校验不通过！！")
        self.bar = bar
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.generateSignal(bar)
        self.calculateIndicator()

    # ----------------------------------------------------------------------
    def generateSignal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # 检查减仓
        if self.position_reduce_price:
            if self.direction == Direction.LONG:
                pass

            if self.direction == Direction.SHORT:
                pass

        # 检查加仓
        if self.position_increase_price:
            if self.direction == Direction.LONG:
                pass

            if self.direction == Direction.SHORT:
                pass

    # ----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算入场指标"""
        
        # 均线价格
        self.ma_price = self.am.sma(self.ma_window)

        # 减仓价格
        if (self.position - self.symbol_min_volume) * self.position_price > self.unit_value:
            if self.direction == Direction.LONG:
                self.position_increase_price = self.position_price * (1 + 0.01)

            elif self.direction == Direction.SHORT:
                self.position_increase_price = self.position_price * (1 - 0.01)

        # 加仓价格
        if self.direction == Direction.LONG:
            if self.direction == Direction.LONG:
                self.position_reduce_price = self.position_price * (1 - 0.02)

            elif self.direction == Direction.SHORT:
                self.position_reduce_price = self.position_price * (1 + 0.02)

    # ----------------------------------------------------------------------
    def newSignal(self, direction, offset, price, volume):
        self.portfolio.newSignal(self, direction, offset, price, volume)

    # ----------------------------------------------------------------------
    def buy(self, price, volume):
        """买入开仓"""
        price = self.calculateTradePrice(Direction.LONG, price)
        # 对价格四舍五入
        priceTick = self.portfolio.engine.priceTickDict[self.symbol]
        price = int(round(price / priceTick, 0)) * priceTick

        self.open(price, volume)
        self.newSignal(Direction.LONG, Offset.OPEN, price, volume)

        # 以最后一次加仓价格，加上两倍N计算止损
        self.longStop = price - self.atrVolatility * 2
        """ modify by loe """
        self.priceHigh = price
        self.priceLow = price

    # ----------------------------------------------------------------------
    def sell(self, price):
        """卖出平仓"""
        price = self.calculateTradePrice(Direction.SHORT, price)
        # 对价格四舍五入
        priceTick = self.portfolio.engine.priceTickDict[self.symbol]
        price = int(round(price / priceTick, 0)) * priceTick

        volume = abs(self.unit)

        self.close(price)
        self.newSignal(Direction.SHORT, Offset.CLOSE, price, volume)

    # ----------------------------------------------------------------------
    def short(self, price, volume):
        """卖出开仓"""
        price = self.calculateTradePrice(Direction.SHORT, price)
        # 对价格四舍五入
        priceTick = self.portfolio.engine.priceTickDict[self.symbol]
        price = int(round(price / priceTick, 0)) * priceTick

        self.open(price, -volume)
        self.newSignal(Direction.SHORT, Offset.OPEN, price, volume)

        # 以最后一次加仓价格，加上两倍N计算止损
        self.shortStop = price + self.atrVolatility * 2
        """ modify by loe """
        self.priceHigh = price
        self.priceLow = price

    # ----------------------------------------------------------------------
    def cover(self, price):
        """买入平仓"""
        price = self.calculateTradePrice(Direction.LONG, price)
        # 对价格四舍五入
        priceTick = self.portfolio.engine.priceTickDict[self.symbol]
        price = int(round(price / priceTick, 0)) * priceTick

        volume = abs(self.unit)

        self.close(price)
        self.newSignal(Direction.LONG, Offset.CLOSE, price, volume)

    # ----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change

    # ----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.unit = 0

    # ----------------------------------------------------------------------
    def calculateTradePrice(self, direction, price):
        """计算成交价格"""
        # 买入时，停止单成交的最优价格不能低于当前K线开盘价
        if direction == Direction.LONG:
            tradePrice = max(self.bar.open_price, price)
            
        # 卖出时，停止单成交的最优价格不能高于当前K线开盘价
        else:
            tradePrice = min(self.bar.open_price, price)

        return tradePrice


########################################################################
class TurtlePortfolio(object):
    # ----------------------------------------------------------------------
    def __init__(self, engine):
        self.engine = engine

        self.signalDict = defaultdict(list)

        self.unitDict = {}  # 每个品种的持仓情况
        self.totalLong = 0  # 总的多头持仓
        self.totalShort = 0  # 总的空头持仓
        self.categoryLongUnitDict = defaultdict(int)  # 高度关联品种多头持仓情况
        self.categoryShortUnitDict = defaultdict(int)  # 高度关联品种空头持仓情况
        self.maxBond = []  # 历史占用保证金的最大值
        self.tradingStart = None  # 开始交易日期

        self.tradingDict = {}  # 交易中的信号字典

        self.sizeDict = {}  # 合约大小字典
        self.multiplierDict = {}  # 按照波动幅度计算的委托量单位字典
        self.posDict = {}  # 真实持仓量字典

        self.portfolioValue = 0  # 组合市值

    # ----------------------------------------------------------------------
    def init(self, portfolioValue, symbolList, sizeDict):
        """"""
        self.portfolioValue = portfolioValue
        self.sizeDict = sizeDict

        for symbol in symbolList:
            signal1 = AISignal(self, symbol, Direction.LONG, 9)
            signal2 = AISignal(self, symbol, Direction.SHORT, 9)

            l = self.signalDict[symbol]
            l.append(signal1)
            l.append(signal2)

            self.unitDict[symbol] = 0
            self.posDict[symbol] = 0

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """"""
        for signal in self.signalDict[bar.symbol]:
            signal.onBar(bar)

    # ----------------------------------------------------------------------
    def newSignal(self, signal, direction, offset, price, volume):
        """对交易信号进行过滤，符合条件的才发单执行"""
        unit = self.unitDict[signal.symbol]

        # 如果当前无仓位，则重新根据波动幅度计算委托量单位
        if not unit:
            size = self.sizeDict[signal.symbol]
            riskValue = self.portfolioValue * 0.01
            """ modify by loe """
            multiplier = 0
            if signal.atrVolatility * size:
                multiplier = riskValue / (signal.atrVolatility * size)

                min_volume = self.engine.min_volume_dict[signal.symbol]
                if min_volume <= 0:
                    raise ("策略最小交易数量设置错误！！")
                multiplier = round(multiplier / min_volume, 0) * min_volume

            self.multiplierDict[signal.symbol] = multiplier
        else:
            multiplier = self.multiplierDict[signal.symbol]

        # 过滤虚假开仓
        if multiplier == 0:
            return

        # 平仓
        if offset != Offset.OPEN:
            if direction == Direction.LONG:
                # 必须有空头持仓
                if unit >= 0:
                    return

                # 平仓数量不能超过空头持仓
                volume = min(volume, abs(unit))
            else:
                if unit <= 0:
                    return

                volume = min(volume, abs(unit))

        # 获取当前交易中的信号，如果不是本信号，则忽略
        currentSignal = self.tradingDict.get(signal.symbol, None)
        if currentSignal and currentSignal is not signal:
            return

        # 开仓则缓存该信号的交易状态
        if offset == Offset.OPEN:
            self.tradingDict[signal.symbol] = signal
        # 平仓则清除该信号
        else:
            self.tradingDict.pop(signal.symbol)

        self.sendOrder(signal.symbol, direction, offset, price, volume, multiplier)

    # ----------------------------------------------------------------------
    def sendOrder(self, symbol, direction, offset, price, volume, multiplier):
        """"""

        # 计算合约持仓
        if direction == Direction.LONG:
            self.unitDict[symbol] += volume
            self.posDict[symbol] += volume * multiplier

        else:
            self.unitDict[symbol] -= volume
            self.posDict[symbol] -= volume * multiplier

        # 计算总持仓、类别持仓
        self.totalLong = 0
        self.totalShort = 0
        self.categoryLongUnitDict = defaultdict(int)
        self.categoryShortUnitDict = defaultdict(int)

        for theSymbol, unit in self.unitDict.items():
            # 总持仓
            if unit > 0:
                self.totalLong += unit
            elif unit < 0:
                self.totalShort += unit

        # 向回测引擎中发单记录
        self.engine.sendOrder(symbol, direction, offset, price, volume * multiplier)
