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
from datetime import timedelta
from vnpy.trader.constant import Interval

""" fake """
import csv
import os

########################################################################
class TurtleStrategyCrypto(CtaTemplate):
    """海龟交易策略"""
    className = 'TurtleStrategyCrypto'
    author = u'loe'

    # 策略参数
    bit_value = 0                       # 数字货币设定价
    entryWindow = 20                    # 入场通道窗口
    exitWindow = 10                     # 出场通道窗口
    atrWindow = 15                      # 计算ATR波动率的窗口

    # 策略变量
    hasClose = False                    # 当前交易日平仓tag，执行平仓的交易日不进行后续任何开平交易

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
    multiplierList = []                 # 每次开仓的unit大小集合
    virtualUnit = 0                     # 信号仓位
    unit = 0                            # 实际持有仓位
    entry = 0                           # 当前持仓成本（不考虑滑点）
    lastPnl = 0                         # 上一次盈利（不考虑滑点和手续费）
    
    # 参数列表，保存了参数的名称
    parameters = ['strategy_name',
                 'vt_symbol',
                 'bit_value',
                 'per_size',
                 'tick_price',
                 'entryWindow',
                 'exitWindow',
                 'atrWindow']


    # 变量列表，保存了变量的名称
    variables = ['hasClose',
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
               'multiplierList',
               'virtualUnit',
               'unit',
               'entry',
               'lastPnl']
    
    # 同步列表，保存了需要保存到数据库的变量名称
    syncs =    ['pos',
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
                'multiplierList',
                'virtualUnit',
                'unit',
                'entry',
                'lastPnl']

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, turtlePortfolio, setting):
        """Constructor"""
        super(TurtleStrategyCrypto, self).__init__(cta_engine=ctaEngine, strategy_name='', vt_symbol='', setting=setting)

        self.portfolio = turtlePortfolio
        self.am = ArrayManager(self.entryWindow+1)
        self.atrAm = ArrayManager(self.atrWindow+1)
        
    #----------------------------------------------------------------------
    def on_init(self):
        """初始化策略（必须由用户继承实现）"""
        self.hasClose = False
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
        if datetime.time(7, 50) <= (tick.datetime + timedelta(hours=8)).time() <= datetime.time(8, 2):
            """ fake """
            #"""
            write_content = {'real_datetime':datetime.datetime.now(),
                             'tick_datetime':tick.datetime,
                             'tick_vt_symbol':tick.vt_symbol}
            self.write_to_file(content=write_content, file_path=f'FAKE_FILE_{tick.symbol}.csv')
            #"""

            self.saveTick(tick)

        if not self.trading:
            return

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
                current_multiplier = self.calMultiplier(tick.last_price, direction=Direction.LONG)

                # 信号建仓
                self.open(tick.last_price, 1)

                # 先手动更新最大止损，如果有真实交易会在onTrade再次更新
                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                # 过滤虚假开仓
                if current_multiplier <= 0:
                    preCheck = False

                # 上次盈利过滤
                if self.lastPnl > 0:
                    preCheck = False

                # 检查是否保证金超限
                if self.checkBondOver(tick.last_price, current_multiplier):
                    preCheck = False

                # 组合仓位管理
                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if tick.last_price >= self.longEntry2 and self.virtualUnit < 2:
                action = True
                current_multiplier = self.calMultiplier(tick.last_price, direction=Direction.LONG)

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                if self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price, current_multiplier):
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if tick.last_price >= self.longEntry3 and self.virtualUnit < 3:
                action = True
                current_multiplier = self.calMultiplier(tick.last_price, direction=Direction.LONG)

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                if self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price, current_multiplier):
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if tick.last_price >= self.longEntry4 and self.virtualUnit < 4:
                action = True
                current_multiplier = self.calMultiplier(tick.last_price, direction=Direction.LONG)

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                if self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price, current_multiplier):
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.buy(self.bestLimitOrderPrice(tick, Direction.LONG), current_multiplier*abs(unitChange))

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
                current_multiplier = self.calMultiplier(tick.last_price, direction=Direction.SHORT)

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                if self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price, current_multiplier):
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry2 and self.virtualUnit > -2:
                action = True
                current_multiplier = self.calMultiplier(tick.last_price, direction=Direction.SHORT)

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                if self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price, current_multiplier):
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry3 and self.virtualUnit > -3:
                action = True
                current_multiplier = self.calMultiplier(tick.last_price, direction=Direction.SHORT)

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                if self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price, current_multiplier):
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry4 and self.virtualUnit > -4:
                action = True
                current_multiplier = self.calMultiplier(tick.last_price, direction=Direction.SHORT)

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                if self.lastPnl > 0:
                    preCheck = False

                if self.checkBondOver(tick.last_price, current_multiplier):
                    preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.short(self.bestLimitOrderPrice(tick, Direction.SHORT), current_multiplier * abs(unitChange))

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
        super(TurtleStrategyCrypto, self).on_trade(trade)

    #----------------------------------------------------------------------
    def on_stop_order(self, so):
        """停止单推送"""

    # 计算交易单位N
    def calMultiplier(self, price, direction:Direction):
        multiplier = 0
        size = self.per_size
        riskValue = self.portfolio.portfolioValue * 0.01 / self.bit_value
        if self.atrVolatility * size:
            if direction == Direction.LONG:
                multiplier = riskValue * (price * (price - 2 * self.atrVolatility)) / (size * self.atrVolatility)
            elif direction == Direction.SHORT:
                multiplier = riskValue * (price * (price + 2 * self.atrVolatility)) / (size * self.atrVolatility)

            multiplier = int(round(multiplier, 0))
        self.multiplierList.append(multiplier)
        return  multiplier

    # 计算入场信号指标
    def updateIndicator(self):
        # 计算atr
        self.atrVolatility = self.atrAm.atr(self.atrWindow)

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
    def checkBondOver(self, price, multiplier):
        # 一个unit预计占用保证金不得超过初始资金的20%
        if self.per_size * multiplier / (price * 20) > self.portfolio.portfolioValue * 0.2 / self.bit_value:
            self.portfolio.addOverBond(self.vt_symbol, price, self.per_size, multiplier, self.atrVolatility)
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
        self.portfolio.addPnl(self.vt_symbol, self.lastPnl, self.multiplierList, self.per_size, self.exitUp, self.exitDown, self.longStop, self.shortStop)

        self.virtualUnit = 0
        self.unit = 0
        self.entry = 0
        self.multiplierList = []

    """ fake """
    def write_to_file(self, content, file_path):
        field_names = list(content.keys())
        file_exist = os.path.exists(file_path)
        with open(file_path, 'a') as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            if not file_exist:
                writer.writeheader()
            # 写入csv文件
            writer.writerow(content)

