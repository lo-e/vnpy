# encoding: UTF-8

"""
使用马丁式加仓的趋势追踪策略
"""

from vnpy.trader.constant import (Direction, Offset)
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.utility import ArrayManager
from vnpy.app.cta_strategy.base import *
import datetime
from datetime import timedelta
from vnpy.trader.constant import Interval
import csv
import os
from vnpy.trader.object import BarData

class MartingStrategy(CtaTemplate):
    """ 马丁策略 """
    className = 'MartingStrategy'
    author = u'loe'

    # 常量
    direction:Direction = Direction.NET  # 交易方向
    ma_window = 9  # 均线参数
    rsi_window = 14  # RSI参数

    # 策略参数

    # 策略变量
    
    # 参数列表，保存了参数的名称
    parameters = ['strategy_name',
                 'vt_symbol',
                 'entryWindow',
                 'exitWindow',
                 'atrWindow']


    # 变量列表，保存了变量的名称
    variables = ['hasClose']
    
    # 同步列表，保存了需要保存到数据库的变量名称
    syncs =    ['pos']

    def __init__(self, ctaEngine, martingPortfolio, setting):
        super(MartingStrategy, self).__init__(cta_engine=ctaEngine, strategy_name='', vt_symbol='', setting=setting)

        self.portfolio = martingPortfolio
        self.unit_value = self.portfolio.portfolioValue * 0.5 * 0.01  # 最小持仓价值
        self.am = ArrayManager(max(self.ma_window, self.rsi_window + 11) + 1)  # K线容器
        self.bar: BarData = None  # 最新K线
        self.position_price = 0  # 持仓均价
        self.position_reduce_price = 0  # 减仓价格
        self.position_increase_price = 0  # 加仓价格
        self.max_loss_value = 0  # 当前持仓最大亏损价值
        self.max_loss_rate = ""  # 当前持仓最大亏损比率
        self.ma_price = 0  # 均线价格
        self.rsi_array = []
        self.calculate_phase_positions(self.portfolio.portfolioValue)  # 马丁格尔倍数仓位管理
        self.phase_position_volume = 0  # 阶段仓位的初始持仓数量
        self.trending_step = 0  # 追踪趋势的等级
        
    def on_init(self):
        """初始化策略（必须由用户继承实现）"""
        self.hasClose = False
        self.barDbName = DAILY_DB_NAME
        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.load_bar(300, interval=Interval.DAILY)
        for bar in initData:
            self.on_bar(bar)
        self.write_log(f'{self.strategy_name}\t策略初始化')

    def on_start(self):
        """启动策略（必须由用户继承实现）"""
        self.write_log(f'{self.strategy_name}\t策略启动')

    def on_stop(self):
        """停止策略（必须由用户继承实现）"""
        self.write_log(f'{self.strategy_name}\t策略停止')

    def on_tick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        """
        # 保存tick数据到数据库
        if datetime.time(7, 50) <= (tick.datetime + timedelta(hours=8)).time() <= datetime.time(8, 2):
            self.saveTick(tick)
        """

        """ fake """
        #self.write_log(f'【real：{datetime.datetime.now()}】\t【tick：{tick.datetime + timedelta(hours=8)}】\t{tick.symbol}')

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
                # if self.lastPnl > 0:
                #     preCheck = False

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

                # if self.lastPnl > 0:
                #     preCheck = False

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

                # if self.lastPnl > 0:
                #     preCheck = False

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

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.OPEN):
                        unitChange += 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.buy(self.bestLimitOrderPrice(tick, Direction.LONG, multi=200), current_multiplier*abs(unitChange))

                self.put_timer_event()
                return

            # 止损平仓
            if self.virtualUnit > 0:
                longExit = max(self.longStop, self.exitDown)
                if tick.last_price <= longExit:
                    self.close(tick.last_price)
                    self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.CLOSE)
                    if self.pos > 0:
                        self.sell(self.bestLimitOrderPrice(tick, Direction.SHORT, multi=200), abs(self.pos))
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

                # if self.lastPnl > 0:
                #     preCheck = False

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

                # if self.lastPnl > 0:
                #     preCheck = False

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

                # if self.lastPnl > 0:
                #     preCheck = False

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

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(self.vt_symbol, Direction.SHORT, Offset.OPEN):
                        unitChange -= 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.short(self.bestLimitOrderPrice(tick, Direction.SHORT, multi=200), current_multiplier * abs(unitChange))

                self.put_timer_event()
                return

            # 止损平仓
            if self.virtualUnit < 0:
                shortExit = min(self.shortStop, self.exitUp)
                if tick.last_price >= shortExit:
                    self.close(tick.last_price)
                    self.portfolio.newSignal(self.vt_symbol, Direction.LONG, Offset.CLOSE)
                    if self.pos < 0:
                        self.cover(self.bestLimitOrderPrice(tick, Direction.LONG, multi=200), abs(self.pos))
                    # 平仓后更新最新指标
                    self.updateIndicator()
                    self.hasClose = True

                self.put_timer_event()
                return

        self.put_timer_event()

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

    def on_order(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    def on_trade(self, trade):
        """成交推送"""
        # 邮件提醒
        super(TurtleStrategyCrypto, self).on_trade(trade)

    # 计算交易单位N
    def calMultiplier(self, price, direction:Direction):
        multiplier = 0
        riskValue = self.portfolio.portfolioValue * 0.01
        if self.atrVolatility:
            if direction == Direction.LONG:
                multiplier = riskValue * (price * (price - 2 * self.atrVolatility)) / self.atrVolatility
            elif direction == Direction.SHORT:
                multiplier = riskValue * (price * (price + 2 * self.atrVolatility)) / self.atrVolatility

            multiplier = int(round(multiplier, 0))
        self.multiplierList.append(multiplier)
        return multiplier

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

    # 信号建仓
    def open(self, price, change):
        cost = self.virtualUnit * self.entry                 # 计算之前的开仓成本
        cost += change * price                               # 加上新仓位的成本
        self.virtualUnit += change                           # 更新信号持仓
        self.entry = cost / self.virtualUnit                 # 计算新的平均开仓成本

    # 信号平仓
    def close(self, price):
        self.lastPnl = (price - self.entry) * self.virtualUnit

        self.virtualUnit = 0
        self.unit = 0
        self.entry = 0
        self.multiplierList = []