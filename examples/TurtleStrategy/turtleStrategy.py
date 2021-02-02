# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange
from vnpy.trader.utility import ArrayManager
from datetime import  datetime
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData

""" modify by loe """
import re
from vnpy.app.cta_strategy.base import (DAILY_DB_NAME, DOMINANT_DB_NAME)

MAX_PRODUCT_POS = 4         # 单品种最大持仓
MAX_CATEGORY_POS = 6        # 高度关联最大持仓
MAX_DIRECTION_POS = 12      # 单方向最大持仓

CATEGORY_DICT = {'finance':['IF','IC','IH'],
                'nonferrous_metal':['AL', 'CU'],
                 'ferrous_metal':['RB','I','HC','SM'],
                 'coal':['JM','J','ZC'],
                 'chemical_industry':['TA', 'RU', 'PP', 'L', 'EG'],
                 'soft_commodity':['CF', 'SR'],
                 'Oils_fats':['M', 'P', 'OI', 'RM'],
                 'farmer_products':['AP'],
                 'light_industry':['SP', 'FG'],
                 'precious_metal':['AG'],
                 'cereal':['C']}

ACTUAL_TRADE = True        # 实盘合约交易

########################################################################
class TurtleResult(object):
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
    

########################################################################
class TurtleSignal(object):
    """海龟信号"""

    #----------------------------------------------------------------------
    def __init__(self, portfolio, symbol,
                 entryWindow, exitWindow, atrWindow,
                 profitCheck=False):
        """Constructor"""
        self.portfolio = portfolio      # 投资组合
        
        self.symbol = symbol            # 合约代码
        self.is_crypto = self.portfolio.engine.is_crypto_dict[self.symbol]
        self.entryWindow = entryWindow  # 入场通道周期数
        self.exitWindow = exitWindow    # 出场通道周期数
        self.atrWindow = atrWindow      # 计算ATR周期数
        self.profitCheck = profitCheck  # 是否检查上一笔盈利

        """ modify by loe """
        self.am = ArrayManager(self.entryWindow+1)      # K线容器
        #self.am = ArrayManager(60)
        self.atrAm = ArrayManager(self.atrWindow+1)     # K线容器
        #self.atrAm = ArrayManager(60)
        
        self.atrVolatility = 0          # ATR波动率
        self.entryUp = 0                # 入场通道
        self.entryDown = 0
        self.exitUp = 0                 # 出场通道
        self.exitDown = 0
        """ modify by loe """
        self.priceHigh = 0
        self.priceLow = 0
        
        self.longEntry1 = 0             # 多头入场位
        self.longEntry2 = 0
        self.longEntry3 = 0
        self.longEntry4 = 0
        self.longStop = 0               # 多头止损位
        
        self.shortEntry1 = 0            # 空头入场位
        self.shortEntry2 = 0
        self.shortEntry3 = 0
        self.shortEntry4 = 0
        self.shortStop = 0              # 空头止损位
        
        self.unit = 0                   # 信号持仓
        self.result = None              # 当前的交易
        self.resultList = []            # 交易列表
        self.bar = None                 # 最新K线
        """ modify by loe """
        self.startTimeCross = False       # 是否过滤了无效开平交易

        self.client = None              # 数据库
        self.symbolDominantData = []    # 主力合约代码列表
        self.dominantDate = []          # 主力合约切换月前后日期
        self.actualSymbol = ''          # 实盘交易合约
        self.actualBarList = []         # 实盘交易合约bar数据
        self.newDominantIniting = False # 主力换月初始化状态
        self.newDominantOpen = True     # 主力换月后新主力开仓门槛【原则：原主力有实际同向持仓；门槛一直延续至下一轮信号】

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """ modify by loe """
        actualBar = None
        if ACTUAL_TRADE and not self.is_crypto and not self.newDominantIniting:
            # 获取数据库
            if not self.client:
                self.client = MongoClient('localhost', 27017)

            # 获取主力合约列表
            if not self.symbolDominantData:
                startSymbol = re.sub("\d", "", self.symbol)
                db = self.client[DOMINANT_DB_NAME]
                collection = db[startSymbol]
                cursor = collection.find().sort('date')
                for dic in cursor:
                    self.symbolDominantData.append(dic)

            exchange = False
            # 主力换月，更新domiantDate
            if not len(self.dominantDate) or bar.datetime < self.dominantDate[0] or bar.datetime >= self.dominantDate[-1]:
                exchange = True
                i = 0
                while i < len(self.symbolDominantData):
                    dominantDic = self.symbolDominantData[i]
                    if dominantDic['date'] < bar.datetime:
                        i += 1
                        continue
                    elif dominantDic['date'] == bar.datetime:
                        break
                    else:
                        if not self.dominantDate:
                            return
                        else:
                            raise '回测数据日期找不到对应主力合约，检查代码！'
                            break
                self.symbolDominantData = self.symbolDominantData[i:]

                startD = self.symbolDominantData[0]
                if len(self.symbolDominantData) < 2:
                    endD = {'date': datetime(6666, 1, 1)}
                else:
                    endD = self.symbolDominantData[1]

                self.dominantDate = []
                self.dominantDate.append(startD['date'])
                self.dominantDate.append(endD['date'])
                self.actualSymbol = startD['symbol']

            if exchange:
                # 记录前主力实际持仓
                oldUnit = self.portfolio.unitDict[self.symbol]

                # 旧主力合约以开盘价限价单平仓
                if self.unit > 0:
                    actualBar = self.getActualBar(bar.datetime)
                    limitVolume = abs(self.unit)
                    limitPrice = actualBar.open_price

                    self.close(limitPrice)
                    self.newSignal(Direction.SHORT, Offset.CLOSE, limitPrice, limitVolume)

                elif self.unit < 0:
                    actualBar = self.getActualBar(bar.datetime)
                    limitVolume = abs(self.unit)
                    limitPrice = actualBar.open_price

                    self.close(limitPrice)
                    self.newSignal(Direction.LONG, Offset.CLOSE, limitPrice, limitVolume)

                # 获取新的主力合约bar数据列表
                self.actualBarList = []
                db = self.client[DAILY_DB_NAME]
                collection = db[self.actualSymbol]
                cursor = collection.find().sort('datetime')
                for dic in cursor:
                    exchange = Exchange.RQ
                    b = BarData(gateway_name='', symbol='', exchange=exchange, datetime=None, endDatetime=None)
                    b.__dict__ = b.merge_data(dic)
                    self.actualBarList.append(b)

                # 新主力合约模拟回测历史数据，获取入场状态
                if self.unit:
                    raise '前主力平仓出错！'
                    exit()

                self.newDominantIniting = True
                self.am = ArrayManager(self.entryWindow + 1)  # K线容器
                self.atrAm = ArrayManager(self.atrWindow + 1)  # K线容器
                self.atrVolatility = 0
                self.longEntry1 = 0
                self.longEntry2 = 0
                self.longEntry3 = 0
                self.longEntry4 = 0
                self.shortEntry1 = 0
                self.shortEntry2 = 0
                self.shortEntry3 = 0
                self.shortEntry4 = 0
                self.longStop = 0
                self.shortStop = 0
                self.resultList = []

                i = 0
                while i < len(self.actualBarList):
                    backBar = self.actualBarList[i]
                    if backBar.datetime < bar.datetime:
                        self.onBar(backBar)
                        i += 1
                    else:
                        break

                self.actualBarList = self.actualBarList[i:]

                self.newDominantIniting = False

            # 获取真实合约bar
            actualBar = self.getActualBar(bar.datetime)

            # 替换bar数据
            # 这里替换close_price是为了turtleEngine计算每日盈亏，替换的是指数合约close_price的值
            bar.close_price = actualBar.close_price
            bar = actualBar

            # 新主力合约初始化建仓
            if exchange:
                # 重置新主力开仓门槛
                self.newDominantOpen = False
                if (oldUnit > 0 and self.unit > 0) or (oldUnit < 0 and self.unit < 0) or self.unit == 0:
                    self.newDominantOpen = True

                if self.newDominantOpen:
                    i = 0
                    while i < abs(self.unit):
                        if self.unit > 0:
                            self.newSignal(Direction.LONG, Offset.OPEN, actualBar.open_price, 1)

                        elif self.unit < 0:
                            self.newSignal(Direction.SHORT, Offset.OPEN, actualBar.open_price, 1)

                        i += 1

        if not bar.check_valid():
            raise('Bar数据校验不通过！！')
        self.bar = bar
        self.am.update_bar(bar)
        """ modify by loe """
        self.atrAm.update_bar(bar)
        if not self.am.inited or not self.atrAm.inited:
            return

        self.generateSignal(bar)
        self.calculateIndicator()

    # ----------------------------------------------------------------------
    """ modify by loe """
    def getActualBar(self, date):
        i = 0
        get = False
        theBar = None

        while i < len(self.actualBarList):
            theBar = self.actualBarList[i]
            if theBar.datetime < date:
                i += 1
                continue
            elif theBar.datetime == date:
                get = True
                break
            else:
                raise '实盘合约bar数据出错！'
                exit()
        if not get:
            raise '实盘合约bar数据出错！'
            exit()

        self.actualBarList = self.actualBarList[i + 1:]
        return theBar

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # 如果指标尚未初始化，则忽略
        if not self.longEntry1:
            return
        
        # 优先检查平仓
        if self.unit > 0:
            """ modify by loe """
            longExit = max(self.longStop, self.exitDown, self.priceHigh - 100*self.atrVolatility)
            
            if bar.low_price <= longExit:
                self.sell(longExit)
                return

        elif self.unit < 0:
            """ modify by loe """
            shortExit = min(self.shortStop, self.exitUp, self.priceLow + 100*self.atrVolatility)

            if bar.high_price >= shortExit:
                self.cover(shortExit)
                return

        # 没有仓位或者持有多头仓位的时候，可以做多（加仓）
        if self.unit >= 0:
            trade = False
            
            if bar.high_price >= self.longEntry1 and self.unit < 1:
                self.buy(self.longEntry1, 1)
                trade = True
            
            if bar.high_price >= self.longEntry2 and self.unit < 2:
                self.buy(self.longEntry2, 1)
                trade = True
            
            if bar.high_price >= self.longEntry3 and self.unit < 3:
                self.buy(self.longEntry3, 1)
                trade = True
            
            if bar.high_price >= self.longEntry4 and self.unit < 4:
                self.buy(self.longEntry4, 1)
                trade = True
            
            if trade:
                return

        # 没有仓位或者持有空头仓位的时候，可以做空（加仓）
        if self.unit <= 0:
            if bar.low_price <= self.shortEntry1 and self.unit > -1:
                self.short(self.shortEntry1, 1)
            
            if bar.low_price <= self.shortEntry2 and self.unit > -2:
                self.short(self.shortEntry2, 1)
            
            if bar.low_price <= self.shortEntry3 and self.unit > -3:
                self.short(self.shortEntry3, 1)
            
            if bar.low_price <= self.shortEntry4 and self.unit > -4:
                self.short(self.shortEntry4, 1)
            
    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.entryUp, self.entryDown = self.am.donchian(self.entryWindow)
        self.exitUp, self.exitDown = self.am.donchian(self.exitWindow)
        """ modify by loe """
        self.priceHigh = max(self.bar.high_price, self.priceHigh)
        self.priceLow = min(self.bar.low_price, self.priceLow)
        
        # 有持仓后，ATR波动率和入场位等都不再变化
        if not self.unit:
            #self.atrVolatility = self.am.atr(self.atrWindow)
            """ modify by loe """
            self.atrVolatility = self.atrAm.atr(self.atrWindow)
            
            self.longEntry1 = self.entryUp
            self.longEntry2 = self.entryUp + self.atrVolatility * 0.5
            self.longEntry3 = self.entryUp + self.atrVolatility * 1
            self.longEntry4 = self.entryUp + self.atrVolatility * 1.5
            self.longStop = 0
            
            self.shortEntry1 = self.entryDown
            self.shortEntry2 = self.entryDown - self.atrVolatility * 0.5
            self.shortEntry3 = self.entryDown - self.atrVolatility * 1
            self.shortEntry4 = self.entryDown - self.atrVolatility * 1.5
            self.shortStop = 0
        
    #----------------------------------------------------------------------
    def newSignal(self, direction, offset, price, volume):
        """ modify by loe """
        if self.newDominantIniting:
            return

        if not self.newDominantOpen:
            return

        if self.portfolio.tradingStart:
            # 如果开始正式交易的时候该信号有历史仓位，则忽略这笔开平交易
            if self.bar.datetime >= self.portfolio.tradingStart:
                if abs(self.unit) == 1:
                    self.startTimeCross = True

                if self.startTimeCross:
                    self.portfolio.newSignal(self, direction, offset, price, volume)
        else:
            self.portfolio.newSignal(self, direction, offset, price, volume)
    
    #----------------------------------------------------------------------
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
    
    #----------------------------------------------------------------------
    def sell(self, price):
        """卖出平仓"""
        price = self.calculateTradePrice(Direction.SHORT, price)
        # 对价格四舍五入
        priceTick = self.portfolio.engine.priceTickDict[self.symbol]
        price = int(round(price / priceTick, 0)) * priceTick

        volume = abs(self.unit)
        
        self.close(price)
        self.newSignal(Direction.SHORT, Offset.CLOSE, price, volume)
    
    #----------------------------------------------------------------------
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
    
    #----------------------------------------------------------------------
    def cover(self, price):
        """买入平仓"""
        price = self.calculateTradePrice(Direction.LONG, price)
        # 对价格四舍五入
        priceTick = self.portfolio.engine.priceTickDict[self.symbol]
        price = int(round(price / priceTick, 0)) * priceTick

        volume = abs(self.unit)
        
        self.close(price)
        self.newSignal(Direction.LONG, Offset.CLOSE, price, volume)

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change 
        
        if not self.result:
            self.result = TurtleResult()
        self.result.open(price, change)
    
    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.unit = 0
        
        self.result.close(price)
        self.resultList.append(self.result)
        self.result = None

        """ modify by loe """
        self.newDominantOpen = True

    #----------------------------------------------------------------------
    def getLastPnl(self):
        """获取上一笔交易的盈亏"""
        if not self.resultList:
            return 0
        
        result = self.resultList[-1]
        return result.pnl
    
    #----------------------------------------------------------------------
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
    """海龟组合"""

    #----------------------------------------------------------------------
    def __init__(self, engine):
        """Constructor"""
        self.engine = engine
        
        self.signalDict = defaultdict(list)
        
        self.unitDict = {}          # 每个品种的持仓情况
        self.totalLong = 0          # 总的多头持仓
        self.totalShort = 0         # 总的空头持仓
        """ modify by loe """
        self.categoryLongUnitDict = defaultdict(int)      # 高度关联品种多头持仓情况
        self.categoryShortUnitDict = defaultdict(int)     # 高度关联品种空头持仓情况
        self.maxBond = []                                 # 历史占用保证金的最大值
        self.tradingStart = None                          # 开始交易日期
        
        self.tradingDict = {}       # 交易中的信号字典
        
        self.sizeDict = {}          # 合约大小字典
        self.multiplierDict = {}    # 按照波动幅度计算的委托量单位字典
        self.posDict = {}           # 真实持仓量字典
        
        self.portfolioValue = 0     # 组合市值
    
    #----------------------------------------------------------------------
    def init(self, portfolioValue, symbolList, sizeDict):
        """"""
        self.portfolioValue = portfolioValue
        self.sizeDict = sizeDict
        
        for symbol in symbolList:
            signal1 = TurtleSignal(self, symbol, 20, 10, 15, True)
            """ modify by loe """
            #signal2 = TurtleSignal(self, symbol, 5500, 20, 20, False)

            l = self.signalDict[symbol]
            l.append(signal1)
            #l.append(signal2)
            
            self.unitDict[symbol] = 0
            self.posDict[symbol] = 0
    
    #----------------------------------------------------------------------
    def onBar(self, bar):
        """"""
        for signal in self.signalDict[bar.symbol]:
            signal.onBar(bar)
    
    #----------------------------------------------------------------------
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
                    raise('策略最小交易数量设置错误！！')
                multiplier = round(multiplier / min_volume, 0) * min_volume

            self.multiplierDict[signal.symbol] = multiplier
        else:
            multiplier = self.multiplierDict[signal.symbol]

        """ modify by loe """
        # 过滤虚假开仓
        if multiplier == 0:
            return

        # 开仓
        if offset == Offset.OPEN:
            # 检查上一次是否为盈利
            if signal.profitCheck:
                pnl = signal.getLastPnl()
                if pnl > 0:
                    return

            """ modify by loe """
            # 一个unit预计占用保证金不得超过初始资金的20%
            size = self.sizeDict[signal.symbol]
            if price * multiplier * size * 0.1 > self.portfolioValue * 0.2:
                print('%s\t%s预计保证金超限\tprice：%s\tatr：%s' % (signal.bar.datetime, signal.symbol, price, signal.atrVolatility))
                return
                
            # 买入
            if direction == Direction.LONG:
                # 组合持仓不能超过上限
                if self.totalLong >= MAX_DIRECTION_POS:
                    return
                
                # 单品种持仓不能超过上限
                if self.unitDict[signal.symbol] >= MAX_PRODUCT_POS:
                    return

                """ modify by loe """
                # 高度关联品种单方向持仓不能超过上限
                startSymbol = re.sub("\d", "", signal.symbol)
                for key, value in CATEGORY_DICT.items():
                    if startSymbol in value:
                        if self.categoryLongUnitDict[key] >= MAX_CATEGORY_POS:
                            return
                        break

            # 卖出
            else:
                if self.totalShort <= -MAX_DIRECTION_POS:
                    return
                
                if self.unitDict[signal.symbol] <= -MAX_PRODUCT_POS:
                    return

                """ modify by loe """
                startSymbol = re.sub("\d", "", signal.symbol)
                for key, value in CATEGORY_DICT.items():
                    if startSymbol in value:
                        if self.categoryShortUnitDict[key] <= -MAX_CATEGORY_POS:
                            return
                        break

        # 平仓
        else:
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

        """ modify by loe """
        # 计算持仓预计占用的保证金
        bond = 0
        totalUnit = 0
        for tradingSignal in self.tradingDict.values():
            tUnit = abs(self.unitDict[tradingSignal.symbol])
            tMultiplier = self.multiplierDict[tradingSignal.symbol]
            tSize = self.sizeDict[tradingSignal.symbol]
            tPrice = tradingSignal.result.entry

            bond += abs(tUnit*tMultiplier*tSize*tPrice*0.1)
            totalUnit += tUnit
        if self.maxBond:
            lastMax = self.maxBond[0]

            if bond > lastMax:
                self.maxBond = [bond, totalUnit]
        else:
            self.maxBond = [bond, totalUnit]

    
    #----------------------------------------------------------------------
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

            """ modify by loe """
            # 类别持仓
            startSymbol = re.sub("\d", "", theSymbol)
            for key, value in CATEGORY_DICT.items():
                if startSymbol in value:
                    if unit > 0:
                        self.categoryLongUnitDict[key] += unit
                    elif unit < 0:
                        self.categoryShortUnitDict[key] += unit
                    break
        
        # 向回测引擎中发单记录
        self.engine.sendOrder(symbol, direction, offset, price, volume*multiplier)
    