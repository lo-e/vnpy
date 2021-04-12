# encoding: UTF-8

"""
单标的海龟交易策略，实现了完整海龟策略中的信号部分。
"""

from __future__ import division

from vnpy.trader.constant import (Direction, Offset)
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.utility import ArrayManager
from vnpy.app.cta_strategy.base import *
import datetime
import re
from .strategyTurtleInitial import TurtleInitialManager
from vnpy.trader.constant import Interval
from copy import copy

#====== 交易时间 ======
#商品期货
MORNING_START_CF = datetime.time(9, 0)
MORNING_REST_CF = datetime.time(10, 15)
MORNING_RESTART_CF = datetime.time(10, 30)
MORNING_END_CF = datetime.time(11, 30)
AFTERNOON_START_CF = datetime.time(13, 30)
AFTERNOON_END_CF = datetime.time(15, 0)

# 商品期货夜盘时间
NIGHT_START_CF = datetime.time(21, 0)
NIGHT_END_CF_N = datetime.time(23, 0) # 到夜间收盘
NIGHT_END_CF_NM = datetime.time(1, 0) # 到凌晨收盘
NIGHT_END_CF_M = datetime.time(2, 30) # 到凌晨收盘

#股指期货
MORNING_PRE_START_SF = datetime.time(6, 0)
MORNING_START_SF = datetime.time(9, 30)
MORNING_END_SF = datetime.time(11, 30)
AFTERNOON_START_SF = datetime.time(13, 0)
AFTERNOON_END_SF = datetime.time(15, 0)


def isFinanceSymbol(symbol):
    financeSymbols = ['IF', 'IC', 'IH']
    the_symbol = copy(symbol)
    the_symbol = the_symbol.upper()
    startSymbol = re.sub("\d", "", the_symbol)
    if startSymbol in financeSymbols:
        return True
    else:
        return False


########################################################################
class TurtleStrategy(CtaTemplate):
    """海龟交易策略"""
    className = 'TurtleStrategy'
    author = u'loe'

    # 策略参数
    last_symbol = ''                     # 换月主力合约
    entryWindow = 20                    # 入场通道窗口
    exitWindow = 10                     # 出场通道窗口
    atrWindow = 15                      # 计算ATR波动率的窗口

    # 策略变量
    lastSymbolClearNeed = False         # 需要清空前主力合约仓位
    lastClearPos = 0
    posInitialNeed = False              # 需要初始化新主力合约仓位
    hasClose = False                    # 当前交易日平仓tag，执行平仓的交易日不进行后续任何开平交易
    newDominantOpen = True              # 主力换月后新主力开仓门槛【原则：原主力有实际同向持仓；门槛一直延续至下一轮信号】

    entryUp = 0                         # 入场通道上轨
    entryDown = 0                       # 入场通道下轨
    exitUp = 0                          # 出场通道上轨
    exitDown = 0                        # 出场通道下轨
    atrVolatility = 0                   # ATR波动率
    
    longEntry1 = 0                      # 多头入场价格
    longEntry2 = 0
    longEntry3 = 0
    longEntry4 = 0
    shortEntry1 = 0                     # 空头入场价格
    shortEntry2 = 0
    shortEntry3 = 0
    shortEntry4 = 0
    longStop = 0                        # 多头止损价格
    shortStop = 0                       # 空头止损价格

    multiplier = 0                      # unit大小
    virtualUnit = 0                     # 信号仓位
    unit = 0                            # 实际持有仓位
    entry = 0                           # 当前持仓成本（不考虑滑点）
    lastPnl = 0                         # 上一次盈利（不考虑滑点和手续费）
    
    # 参数列表，保存了参数的名称
    parameters = ['strategy_name',
                 'vt_symbol',
                 'last_symbol',
                 'per_size',
                 'tick_price',
                 'is_crypto',
                 'entryWindow',
                 'exitWindow',
                 'atrWindow']


    # 变量列表，保存了变量的名称
    variables = ['posInitialNeed',
               'lastSymbolClearNeed',
               'hasClose',
               'newDominantOpen',
               'entryUp',
               'entryDown',
               'exitUp',
               'exitDown',
               'atrVolatility',
               'longEntry1',
               'longEntry2',
               'longEntry3',
               'longEntry4',
               'shortEntry1',
               'shortEntry2',
               'shortEntry3',
               'shortEntry4',
               'longStop',
               'shortStop',
               'multiplier',
               'virtualUnit',
               'unit',
               'entry',
               'lastPnl']
    
    # 同步列表，保存了需要保存到数据库的变量名称
    syncs =    ['atrVolatility',
                'longEntry1',
                'longEntry2',
                'longEntry3',
                'longEntry4',
                'shortEntry1',
                'shortEntry2',
                'shortEntry3',
                'shortEntry4',
                'longStop',
                'shortStop',
                'multiplier',
                'virtualUnit',
                'unit',
                'entry',
                'lastPnl',
                'newDominantOpen']

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, turtlePortfolio, setting):
        """Constructor"""
        super(TurtleStrategy, self).__init__(cta_engine=ctaEngine, strategy_name='', vt_symbol='', setting=setting)

        # last_symbol需要转换成带交易所后缀
        self.check_last_symbol()
        self.portfolio = turtlePortfolio
        self.am = ArrayManager(self.entryWindow+1)
        self.atrAm = ArrayManager(self.atrWindow+1)
        
    #----------------------------------------------------------------------
    def on_init(self):
        """初始化策略（必须由用户继承实现）"""
        self.hasClose = False
        if self.last_symbol:
            # 主力换月处理
            # 需要清空前主力合约仓位
            # 从数据库载入前主力的持仓情况
            flt = {'strategy_name': self.strategy_name,
                   'vt_symbol': self.last_symbol}
            syncData = self.cta_engine.main_engine.dbQuery(POSITION_DB_NAME, self.__class__.__name__, flt)

            if not syncData:
                self.write_log(f'前主力合约数据库交易数据缺失，需要检查！\tlastSymbol：{self.last_symbol}')
                exit(0)

            d = syncData[0]
            pos = d['pos']
            unit = d['unit']
            if pos:
                self.lastSymbolClearNeed = True
                self.lastClearPos = pos

            # 组合状态管理
            if unit > 0:
                self.portfolio.newSignal(self.last_symbol, Direction.SHORT, Offset.CLOSE)

            elif unit < 0:
                self.portfolio.newSignal(self.last_symbol, Direction.LONG, Offset.CLOSE)

            # 需要建立新主力仓位，初始化新主力状态
            self.newDominantInitial()

        self.barDbName = DAILY_DB_NAME
        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.load_bar(300, interval=Interval.DAILY)
        for bar in initData:
            self.on_bar(bar)
        self.write_log(f'{self.strategy_name}\t策略初始化')

    #----------------------------------------------------------------------
    def on_start(self):
        """启动策略（必须由用户继承实现）"""
        self.write_log(f'{self.strategy_name}\t策略启动')

    #----------------------------------------------------------------------
    def on_stop(self):
        """停止策略（必须由用户继承实现）"""
        self.write_log(f'{self.strategy_name}\t策略停止')

    #----------------------------------------------------------------------
    def on_tick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        # 保存tick数据到数据库
        #self.saveTick(tick)

        if not self.trading:
            return

        # 过滤无效tick
        if not self.is_crypto:
            t = tick.datetime.time()
            isFinance = isFinanceSymbol(tick.symbol)
            if not isFinance:
                if NIGHT_END_CF_M <= t < MORNING_START_CF or MORNING_REST_CF <= t < MORNING_RESTART_CF or MORNING_END_CF <= t < AFTERNOON_START_CF or AFTERNOON_END_CF <= t < NIGHT_START_CF:
                    self.write_log(f'====== 过滤无效tick：{tick.vt_symbol}\t{tick.datetime} ======')
                    return
            else:
                if t < MORNING_START_SF or MORNING_END_SF <= t < AFTERNOON_START_SF or AFTERNOON_END_SF <= t:
                    self.write_log(f'====== 过滤无效tick：{tick.vt_symbol}\t{tick.datetime} ======')
                    return

        # 主力换月时清空前主力仓位
        if tick.vt_symbol == self.last_symbol:
            if self.lastSymbolClearNeed:
                if self.lastClearPos > 0:
                    orderList = self.sendSymbolOrder(self.last_symbol, Direction.SHORT, Offset.CLOSE, self.bestLimitOrderPrice(tick, Direction.SHORT),
                                         abs(self.lastClearPos))
                    if len(orderList):
                        self.pos += self.lastClearPos

                elif self.lastClearPos < 0:
                    orderList = self.sendSymbolOrder(self.last_symbol, Direction.LONG, Offset.CLOSE, self.bestLimitOrderPrice(tick, Direction.LONG),
                                         abs(self.lastClearPos))
                    if len(orderList):
                        self.pos += self.lastClearPos

                self.lastSymbolClearNeed = False
            return

        # 新主力仓位初始化
        if self.posInitialNeed:
            if self.newDominantOpen:
                preCheck = True
                # 过滤虚假开仓
                if self.multiplier == 0:
                    preCheck = False

                # 上次盈利过滤
                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                # 检查是否保证金超限
                if self.checkBondOver(tick.last_price):
                    preCheck = False

                if preCheck:
                    if self.virtualUnit > 0:
                        unitChange = 0
                        i = 0
                        while i < abs(self.virtualUnit):
                            if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                                unitChange += 1
                            i += 1

                        if unitChange > 0:
                            self.unit += unitChange
                            self.buy(self.bestLimitOrderPrice(tick, Direction.LONG), self.multiplier * abs(unitChange))

                    elif self.virtualUnit < 0:
                        unitChange = 0
                        i = 0
                        while i < abs(self.virtualUnit):
                            if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                                unitChange -= 1
                            i += 1

                        if unitChange < 0:
                            self.unit += unitChange
                            self.short(self.bestLimitOrderPrice(tick, Direction.SHORT), self.multiplier * abs(unitChange))

            self.posInitialNeed = False

        # 撮合信号与交易
        if not self.am.inited or not self.atrAm.inited:
            return

        # 当前交易日有过平仓交易，停止一切后续开平操作
        if self.hasClose:
            return

        unitChange = 0
        action = False

        if self.virtualUnit >= 0:
            # 多头开仓加仓
            if tick.last_price >= self.longEntry1 and self.virtualUnit < 1:
                action = True

                # 信号建仓
                self.open(tick.last_price, 1)

                # 先手动更新最大止损，如果有真实交易会在onTrade再次更新
                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                # 过滤虚假开仓
                if self.multiplier == 0:
                    preCheck = False

                # 上次盈利过滤
                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                # 检查是否保证金超限
                if self.checkBondOver(tick.last_price):
                    preCheck = False

                # 检查新主力合约是否允许开仓
                if not self.newDominantOpen:
                    preCheck = False

                # 组合仓位管理
                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if tick.last_price >= self.longEntry2 and self.virtualUnit < 2:
                action = True

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if self.multiplier == 0:
                    preCheck = False

                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price):
                    preCheck = False

                if not self.newDominantOpen:
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if tick.last_price >= self.longEntry3 and self.virtualUnit < 3:
                action = True

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if self.multiplier == 0:
                    preCheck = False

                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price):
                    preCheck = False

                if not self.newDominantOpen:
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if tick.last_price >= self.longEntry4 and self.virtualUnit < 4:
                action = True

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if self.multiplier == 0:
                    preCheck = False

                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price):
                    preCheck = False

                if not self.newDominantOpen:
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.buy(self.bestLimitOrderPrice(tick, Direction.LONG), self.multiplier*abs(unitChange))

                self.put_timer_event()
                return

            # 止损平仓
            if self.virtualUnit > 0:
                longExit = max(self.longStop, self.exitDown)
                if tick.last_price <= longExit:
                    self.close(tick.last_price)
                    self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.CLOSE)
                    if self.pos > 0:
                        self.sell(self.bestLimitOrderPrice(tick, Direction.SHORT), abs(self.pos))
                    # 平仓后更新最新指标
                    self.updateIndicator()
                    self.hasClose = True

                self.put_timer_event()
                return

        if self.virtualUnit <= 0:
            # 空头开仓加仓
            if tick.last_price <= self.shortEntry1 and self.virtualUnit > -1:
                action = True

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if self.multiplier == 0:
                    preCheck = False

                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price):
                    preCheck = False

                if not self.newDominantOpen:
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry2 and self.virtualUnit > -2:
                action = True

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if self.multiplier == 0:
                    preCheck = False

                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price):
                    preCheck = False

                if not self.newDominantOpen:
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry3 and self.virtualUnit > -3:
                action = True

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if self.multiplier == 0:
                    preCheck = False

                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price):
                    preCheck = False

                if not self.newDominantOpen:
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry4 and self.virtualUnit > -4:
                action = True

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if self.multiplier == 0:
                    preCheck = False

                if (not self.is_crypto) and self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price):
                    preCheck = False

                if not self.newDominantOpen:
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.short(self.bestLimitOrderPrice(tick, Direction.SHORT), self.multiplier * abs(unitChange))

                self.put_timer_event()
                return

            # 止损平仓
            if self.virtualUnit < 0:
                shortExit = min(self.shortStop, self.exitUp)
                if tick.last_price >= shortExit:
                    self.close(tick.last_price)
                    self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.CLOSE)
                    if self.pos < 0:
                        self.cover(self.bestLimitOrderPrice(tick, Direction.LONG), abs(self.pos))
                    # 平仓后更新最新指标
                    self.updateIndicator()
                    self.hasClose = True

                self.put_timer_event()
                return

        self.put_timer_event()

    #----------------------------------------------------------------------
    def on_bar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        # 保存K线数据
        self.am.update_bar(bar)
        self.atrAm.update_bar(bar)
        if not self.am.inited or not self.atrAm.inited:
            return
        
        # 计算指标数值
        self.entryUp, self.entryDown = self.am.donchian(self.entryWindow)
        self.exitUp, self.exitDown = self.am.donchian(self.exitWindow)

        # 判断是否要更新交易信号
        if self.virtualUnit == 0:
            self.updateIndicator()
    
        # 发出状态更新事件
        self.put_timer_event()

    #----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    def on_order(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    #----------------------------------------------------------------------
    def on_trade(self, trade):
        """成交推送"""
        # 邮件提醒
        super(TurtleStrategy, self).on_trade(trade)

    #----------------------------------------------------------------------
    def on_stop_order(self, so):
        """停止单推送"""
        pass

    # 计算交易单位N
    def updateMultiplier(self):
        riskValue = self.portfolio.portfolioValue * 0.01
        if riskValue == 0:
            return

        multiplier = 0
        if self.atrVolatility * self.per_size:
            multiplier = riskValue / (self.atrVolatility * self.per_size)
            #multiplier = int(round(multiplier, 0))
            min_volume = 1
            if self.is_crypto:
                contract = self.cta_engine.main_engine.get_contract(vt_symbol=self.vt_symbol)
                if contract:
                    min_volume = contract.min_volume
            multiplier = int(round(multiplier / min_volume, 0)) * min_volume
        self.multiplier = multiplier

    # 计算入场信号指标
    def updateIndicator(self):
        # 计算atr
        self.atrVolatility = self.atrAm.atr(self.atrWindow)

        self.updateMultiplier()

        self.longEntry1 = self.entryUp
        self.longEntry2 = self.longEntry1 + 0.5 * self.atrVolatility
        self.longEntry3 = self.longEntry2 + 0.5 * self.atrVolatility
        self.longEntry4 = self.longEntry3 + 0.5 * self.atrVolatility

        self.shortEntry1 = self.entryDown
        self.shortEntry2 = self.shortEntry1 - 0.5 * self.atrVolatility
        self.shortEntry3 = self.shortEntry2 - 0.5 * self.atrVolatility
        self.shortEntry4 = self.shortEntry3 - 0.5 * self.atrVolatility

        self.longStop = 0
        self.shortStop = 0

    # 检查预计交易保证金是否超限
    def checkBondOver(self, price):
        # 一个unit预计占用保证金不得超过初始资金的20%
        if price * self.multiplier * self.per_size * 0.1 > self.portfolio.portfolioValue * 0.2:
            self.portfolio.addOverBond(self.vt_symbol, price, self.per_size, self.multiplier, self.atrVolatility)
            return True
        else:
            return False

    # 信号建仓
    def open(self, price, change):
        cost = self.virtualUnit * self.entry                 # 计算之前的开仓成本
        cost += change * price                               # 加上新仓位的成本
        self.virtualUnit += change                           # 更新信号持仓
        self.entry = cost / self.virtualUnit                 # 计算新的平均开仓成本

    # 信号平仓
    def close(self, price):
        self.lastPnl = (price - self.entry) * self.virtualUnit
        self.portfolio.addPnl(self.vt_symbol, self.lastPnl, self.multiplier, self.per_size, self.exitUp, self.exitDown, self.longStop, self.shortStop)

        self.virtualUnit = 0
        self.unit = 0
        self.entry = 0
        self.newDominantOpen = True

    # 主力换月，初始化交易状态
    def newDominantInitial(self):
        if self.pos or self.unit:
            return
        self.posInitialNeed = True

        initialManager = TurtleInitialManager(self.vt_symbol, self.entryWindow, self.exitWindow, self.atrWindow)
        initialManager.backtesting()

        self.atrVolatility = initialManager.atrVolatility
        self.longEntry1 = initialManager.longEntry1
        self.longEntry2 = initialManager.longEntry2
        self.longEntry3 = initialManager.longEntry3
        self.longEntry4 = initialManager.longEntry4
        self.shortEntry1 = initialManager.shortEntry1
        self.shortEntry2 = initialManager.shortEntry2
        self.shortEntry3 = initialManager.shortEntry3
        self.shortEntry4 = initialManager.shortEntry4
        self.longStop = initialManager.longStop
        self.shortStop = initialManager.shortStop

        self.updateMultiplier()
        self.virtualUnit = initialManager.unit
        self.unit = 0

        if initialManager.result:
            self.entry = initialManager.result.entry
        self.lastPnl = initialManager.getLastPnl()

        if (self.lastClearPos > 0 and self.virtualUnit > 0) or (self.lastClearPos < 0 and self.virtualUnit < 0) or self.virtualUnit == 0:
            self.newDominantOpen = True
        else:
            self.newDominantOpen = False

    def check_last_symbol(self):
        sepList = self.last_symbol.split('.')
        symbol = sepList[0]
        startSymbol = re.sub("\d", "", symbol).upper()
        for key, value in EXCHANGE_SYMBOL_DICT.items():
            if startSymbol in value:
                self.last_symbol = '.'.join([symbol, key.value])
                break

    def bestLimitOrderPrice(self, tick, direction, multi=20):
        if self.is_crypto:
            return super(TurtleStrategy, self).bestLimitOrderPrice(tick, direction, multi=200)
        else:
            return super(TurtleStrategy, self).bestLimitOrderPrice(tick, direction)

