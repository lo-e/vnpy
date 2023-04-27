# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange
from vnpy.trader.utility import ArrayManager
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
import re
from vnpy.app.cta_strategy.base import DAILY_DB_NAME, DOMINANT_DB_NAME
from enum import Enum
from vnpy.trader.utility import round_to, floor_to, ceil_to


class PHASE_STEP(Enum):
    PHASE_STEP_ONE = "第一仓位，全仓"
    PHASE_STEP_TWO = "第二仓位，50% / 30%"
    PHASE_STEP_THREE = "第三仓位，15%"


########################################################################
class AISignal(object):
    # ----------------------------------------------------------------------
    def __init__(self, portfolio, symbol, direction, ma_window):
        # 常量
        self.portfolio = portfolio  # 投资组合
        self.symbol = symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.unit_value = self.portfolio.portfolioValue * 0.5 * 0.01  # 最小持仓价值
        self.symbol_min_volume = self.portfolio.engine.min_volume_dict[
            self.symbol
        ]  # 合约最小交易数量
        self.symbol_price_tick = self.portfolio.engine.priceTickDict[
            self.symbol
        ]  # 合约最小价格变动
        if not self.symbol_min_volume or not self.symbol_price_tick:
            exit("检查代码！")

        self.calculate_phase_positions(self.portfolio.portfolioValue)  # 马丁格尔倍数仓位管理
        self.current_phase = 0  # 当前仓位所在阶段
        self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE  # 当前仓位阶段减仓位

        # 变量
        self.bar: BarData = None  # 最新K线
        self.am = ArrayManager(self.ma_window + 1)  # K线容器
        self.position = 0  # 持仓量
        self.position_price = 0  # 持仓均价
        self.position_reduce_price = 0  # 减仓价格
        self.position_increase_price = 0  # 加仓价格
        self.ma_price = 0  # 均线价格

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        if not bar.check_valid():
            raise ("Bar数据校验不通过！！")
        self.bar = bar
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.generate_signal(bar)
        self.calculate_indicator()

    # ----------------------------------------------------------------------
    def calculate_phase_positions(self, portfolio_value):
        self.phase_position_values = []
        init_rate = 0.5 * 0.01  # 初始仓位比率
        init_position = portfolio_value * init_rate
        total_phase_count = 10
        for i in range(total_phase_count):
            phase_position = init_position * (2 ** (i + 1) - 1)
            self.phase_position_values.append(phase_position)

    def get_current_phase(self):
        current_position_value = abs(self.position) * self.position_price
        for i in range(len(self.phase_position_values)):
            phase_positon_value = self.phase_position_values[i]
            if current_position_value <= phase_positon_value * 1.1:
                return i
        return len(self.phase_position_values) - 1

    def generate_signal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # 当前仓位阶段
        self.current_phase = self.get_current_phase()
        phase_position_value = self.phase_position_values[self.current_phase]

        # 检查减仓
        if self.position_reduce_price:
            # 减仓后的目标仓位价值
            target_position_value = abs(self.position) * self.position_price

            # 是否达到目标价位
            reduce_price_cross = False
            if self.direction == Direction.LONG:
                if (
                    self.ma_price >= self.position_reduce_price
                    and bar.low_price < self.position_reduce_price
                    and bar.high_price >= self.position_reduce_price
                ):
                    reduce_price_cross = True

            if self.direction == Direction.SHORT:
                if (
                    self.ma_price <= self.position_reduce_price
                    and bar.high_price > self.position_reduce_price
                    and bar.low_price <= self.position_reduce_price
                ):
                    reduce_price_cross = True

            if reduce_price_cross:
                """价格满足减仓条件"""
                if phase_position_value >= self.portfolio.portfolioValue:
                    # 超过100%组合本金，分三个阶段减仓
                    if self.current_phase_step == PHASE_STEP.PHASE_STEP_ONE:
                        # 减仓
                        self.current_phase_step = PHASE_STEP.PHASE_STEP_TWO
                        target_position_value = phase_position_value * 0.5

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_TWO:
                        # 减仓
                        self.current_phase_step = PHASE_STEP.PHASE_STEP_THREE
                        target_position_value = phase_position_value * 0.85

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_THREE:
                        # 平仓
                        self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE
                        self.position_price = self.position_reduce_price
                        target_position_value = self.unit_value

                    else:
                        exit("检查代码！")

                elif phase_position_value >= self.portfolio.portfolioValue * 0.3:
                    # 超过30%组合本金，分两个阶段减仓
                    if self.current_phase_step == PHASE_STEP.PHASE_STEP_ONE:
                        # 减仓
                        self.current_phase_step = PHASE_STEP.PHASE_STEP_TWO
                        target_position_value = phase_position_value * 0.3

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_TWO:
                        # 平仓
                        self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE
                        self.position_price = self.position_reduce_price
                        target_position_value = self.unit_value

                    else:
                        exit("检查代码！")

                else:
                    # 低于30%组合本金，一次性减仓
                    self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE
                    self.position_price = self.position_reduce_price
                    target_position_value = self.unit_value

                # 计算减仓的合约数量
                changed_volume = (target_position_value / self.position_price) - abs(
                    self.position
                )
                changed_volume = floor_to(changed_volume, self.symbol_min_volume)
                target_position = abs(self.position) + changed_volume

                if changed_volume:
                    # 当前持仓数量更新、发起订单
                    if self.direction == Direction.LONG:
                        self.position = target_position
                        if changed_volume > 0:
                            # 加仓
                            self.newSignal(
                                Direction.LONG,
                                Offset.OPEN,
                                self.position_reduce_price,
                                abs(changed_volume),
                            )

                        elif changed_volume < 0:
                            # 平仓
                            self.newSignal(
                                Direction.SHORT,
                                Offset.CLOSE,
                                self.position_reduce_price,
                                abs(changed_volume),
                            )

                    if self.direction == Direction.SHORT:
                        self.position = target_position * -1
                        if changed_volume > 0:
                            # 加仓
                            self.newSignal(
                                Direction.SHORT,
                                Offset.OPEN,
                                self.position_reduce_price,
                                abs(changed_volume),
                            )

                        elif changed_volume < 0:
                            # 平仓
                            self.newSignal(
                                Direction.LONG,
                                Offset.CLOSE,
                                self.position_reduce_price,
                                abs(changed_volume),
                            )

                    # 减仓操作后停止后续加仓判断
                    return

        # 检查加仓
        if self.position_increase_price:
            if self.direction == Direction.LONG:
                pass

            if self.direction == Direction.SHORT:
                pass

    # ----------------------------------------------------------------------
    def calculate_indicator(self):
        """计算入场指标"""

        # 当前仓位阶段
        self.current_phase = self.get_current_phase()
        phase_position_value = self.phase_position_values[self.current_phase]

        # 均线价格
        self.ma_price = self.am.sma(self.ma_window)

        if self.position_price:
            # ====== 减仓价格 ======
            if self.direction == Direction.LONG:
                if phase_position_value >= self.portfolio.portfolioValue:
                    # 超过100%组合本金，分三个阶段减仓
                    if self.current_phase_step == PHASE_STEP.PHASE_STEP_ONE:
                        self.position_reduce_price = self.position_price * (1 + 0.01)

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_TWO:
                        self.position_reduce_price = self.position_price * (1 + 0.02)

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_THREE:
                        self.position_reduce_price = self.position_price * (1 + 0.03)

                elif phase_position_value >= self.portfolio.portfolioValue * 0.3:
                    # 超过30%组合本金，分两个阶段减仓
                    if self.current_phase_step == PHASE_STEP.PHASE_STEP_ONE:
                        self.position_reduce_price = self.position_price * (1 + 0.01)

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_TWO:
                        self.position_reduce_price = self.position_price * (1 + 0.03)

                else:
                    # 低于30%组合本金，一次性减仓
                    self.position_reduce_price = self.position_price * (1 + 0.01)

            elif self.direction == Direction.SHORT:
                if phase_position_value >= self.portfolio.portfolioValue:
                    # 超过100%组合本金，分三个阶段减仓
                    if self.current_phase_step == PHASE_STEP.PHASE_STEP_ONE:
                        self.position_reduce_price = self.position_price * (1 - 0.01)

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_TWO:
                        self.position_reduce_price = self.position_price * (1 - 0.02)

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_THREE:
                        self.position_reduce_price = self.position_price * (1 - 0.03)

                elif phase_position_value >= self.portfolio.portfolioValue * 0.3:
                    # 超过30%组合本金，分两个阶段减仓
                    if self.current_phase_step == PHASE_STEP.PHASE_STEP_ONE:
                        self.position_reduce_price = self.position_price * (1 - 0.01)

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_TWO:
                        self.position_reduce_price = self.position_price * (1 - 0.03)

                else:
                    # 低于30%组合本金，一次性减仓
                    self.position_reduce_price = self.position_price * (1 - 0.01)

            self.position_reduce_price = round_to(self.position_reduce_price, self.symbol_price_tick)

            # ====== 加仓价格 ======
            if self.direction == Direction.LONG:
                if self.current_phase == 0:
                    self.position_increase_price = self.position_price * (1 - 0.02)

                elif self.current_phase == 1:
                    self.position_increase_price = self.position_price * (1 - 0.04)

                else:
                    self.position_increase_price = self.position_price * (1 - 0.08)

            elif self.direction == Direction.SHORT:
                if self.current_phase == 0:
                    self.position_increase_price = self.position_price * (1 + 0.02)

                elif self.current_phase == 1:
                    self.position_increase_price = self.position_price * (1 + 0.04)

                else:
                    self.position_increase_price = self.position_price * (1 + 0.08)

            self.position_increase_price = round_to(self.position_increase_price, self.symbol_price_tick)

    # ----------------------------------------------------------------------
    def newSignal(self, direction, offset, price, volume):
        self.portfolio.newSignal(self, direction, offset, price, volume)


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
