# encoding: UTF-8

# ====== 版本A ======
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
from vnpy.trader.utility import DIR_SYMBOL

class PHASE_STEP(Enum):
    PHASE_STEP_ONE = "第一仓位，全仓"
    PHASE_STEP_TWO = "第二仓位，50% / 30%"
    PHASE_STEP_THREE = "第三仓位，15%"

class MartingSignal(object):
    def __init__(self, portfolio, symbol, direction, ma_window):
        # 常量
        self.portfolio = portfolio  # 投资组合
        self.symbol = symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.unit_value = self.portfolio.portfolioValue * 0.412 * 0.01  # 最小持仓价值
        self.symbol_min_volume = self.portfolio.engine.min_volume_dict[
            self.symbol
        ]  # 合约最小交易数量
        self.symbol_price_tick = self.portfolio.engine.priceTickDict[
            self.symbol
        ]  # 合约最小价格变动
        if not self.symbol_min_volume or not self.symbol_price_tick:
            exit("检查代码！")

        # 变量
        self.bar: BarData = None  # 最新K线
        self.am = ArrayManager(self.ma_window + 1)  # K线容器
        self.position = 0  # 持仓量
        self.position_price = 0  # 持仓均价
        self.position_reduce_price = 0  # 减仓价格
        self.position_increase_price = 0  # 加仓价格
        self.ma_price = 0  # 均线价格
        self.calculate_phase_positions(self.portfolio.portfolioValue)  # 马丁格尔倍数仓位管理
        self.phase_position_volume = 0
        self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE  # 当前仓位阶段减仓状态

    def on_bar(self, bar):
        if not bar.check_valid():
            raise ("Bar数据校验不通过！！")
        self.bar = bar
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.generate_signal(bar)
        self.calculate_indicator()

    def calculate_phase_positions(self, portfolio_value):
        self.phase_position_values = []
        total_phase_count = 20
        for i in range(total_phase_count):
            phase_position = self.unit_value * (2 ** (i + 1) - 1)
            self.phase_position_values.append(phase_position)

    def get_current_phase(self):
        current_phase_position_value = (
            abs(self.phase_position_volume) * self.position_price
        )
        for i in range(len(self.phase_position_values)):
            phase_positon_value = self.phase_position_values[i]
            if current_phase_position_value <= phase_positon_value * 1.1:
                return i
        return len(self.phase_position_values) - 1

    def generate_signal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # fake
        # if self.symbol == "SOLUSDT.BYBIT" and self.direction == Direction.LONG:
        #     if self.bar.datetime >= datetime.strptime("2022-01-23 05:10:00", "%Y-%m-%d %H:%M:%S"):
        #         a = 2

        # 当前仓位阶段
        current_phase = self.get_current_phase()
        phase_position_value = self.phase_position_values[current_phase]

        # 初始化仓位
        if not self.position:
            # 成交价格
            trade_price = round_to(bar.close_price, self.symbol_price_tick)

            # 初始化阶段减仓状态
            self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE

            # 初始化持仓价格
            self.position_price = trade_price

            # 初始化持仓合约数量
            init_volume = self.unit_value / self.position_price
            init_volume = round_to(init_volume, self.symbol_min_volume)

            # 初始化阶段持仓合约数量
            self.phase_position_volume = init_volume

            # 当前持仓数量更新、发起订单
            if self.direction == Direction.LONG:
                self.position = init_volume
                self.newSignal(
                    Direction.LONG,
                    Offset.OPEN,
                    trade_price,
                    abs(init_volume),
                )

            elif self.direction == Direction.SHORT:
                self.position = init_volume * -1
                self.newSignal(
                    Direction.SHORT,
                    Offset.OPEN,
                    trade_price,
                    abs(init_volume),
                )
            
            else:
                exit("检查代码！")

            # 初始化后停止后续判断
            return

        # 检查减仓
        if self.position_reduce_price:
            # 成交价格
            trade_price = round_to(self.ma_price, self.symbol_price_tick)

            # 是否达到目标价位
            reduce_price_cross = False
            if self.direction == Direction.LONG:
                if (
                    self.ma_price >= self.position_reduce_price
                    and bar.low_price < trade_price
                    and bar.high_price >= trade_price
                ):
                    reduce_price_cross = True

            if self.direction == Direction.SHORT:
                if (
                    self.ma_price <= self.position_reduce_price
                    and bar.high_price > trade_price
                    and bar.low_price <= trade_price
                ):
                    reduce_price_cross = True

            if reduce_price_cross:
                """价格满足减仓条件"""

                # 是否平仓
                is_close = False

                # 减仓后的目标仓位价值
                target_position_value = abs(self.position) * self.position_price

                if phase_position_value >= self.portfolio.portfolioValue:
                    # 超过100%组合本金，分三个阶段减仓
                    if self.current_phase_step == PHASE_STEP.PHASE_STEP_ONE:
                        # 减仓
                        self.current_phase_step = PHASE_STEP.PHASE_STEP_TWO
                        target_position_value = phase_position_value * 0.5

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_TWO:
                        # 减仓
                        self.current_phase_step = PHASE_STEP.PHASE_STEP_THREE
                        target_position_value = phase_position_value * 0.15

                    elif self.current_phase_step == PHASE_STEP.PHASE_STEP_THREE:
                        # 平仓
                        self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE
                        self.position_price = trade_price
                        target_position_value = self.unit_value
                        is_close = True

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
                        self.position_price = trade_price
                        target_position_value = self.unit_value
                        is_close = True

                    else:
                        exit("检查代码！")

                else:
                    # 低于30%组合本金，一次性减仓
                    self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE
                    self.position_price = trade_price
                    target_position_value = self.unit_value
                    is_close = True

                # 计算减仓的合约数量
                changed_volume = (target_position_value / self.position_price) - abs(
                    self.position
                )
                changed_volume = round_to(changed_volume, self.symbol_min_volume)

                # 目标仓位合约数量
                target_position = abs(self.position) + changed_volume

                if is_close:
                    # 平仓后更新阶段仓位合约数量
                    self.phase_position_volume = target_position

                if changed_volume:
                    # 当前持仓数量更新、发起订单
                    if self.direction == Direction.LONG:
                        self.position = target_position
                        if changed_volume > 0:
                            # 加仓
                            self.newSignal(
                                Direction.LONG,
                                Offset.OPEN,
                                trade_price,
                                abs(changed_volume),
                            )

                        elif changed_volume < 0:
                            # 平仓
                            self.newSignal(
                                Direction.SHORT,
                                Offset.CLOSE,
                                trade_price,
                                abs(changed_volume),
                            )

                    elif self.direction == Direction.SHORT:
                        self.position = target_position * -1
                        if changed_volume > 0:
                            # 加仓
                            self.newSignal(
                                Direction.SHORT,
                                Offset.OPEN,
                                trade_price,
                                abs(changed_volume),
                            )

                        elif changed_volume < 0:
                            # 平仓
                            self.newSignal(
                                Direction.LONG,
                                Offset.CLOSE,
                                trade_price,
                                abs(changed_volume),
                            )
                    else:
                        exit("检查代码！")

                    # 减仓操作后停止后续加仓判断
                    return

        # 检查加仓
        if self.position_increase_price:
            # 成交价格
            trade_price = round_to(self.ma_price, self.symbol_price_tick)

            # 是否达到目标价位
            increase_price_cross = False
            if self.direction == Direction.LONG:
                if (
                    self.ma_price <= self.position_increase_price
                    and bar.high_price > trade_price
                    and bar.low_price <= trade_price
                ):
                    increase_price_cross = True

            if self.direction == Direction.SHORT:
                if (
                    self.ma_price >= self.position_increase_price
                    and bar.low_price < trade_price
                    and bar.high_price >= trade_price
                ):
                    increase_price_cross = True

            if increase_price_cross:
                """价格满足加仓条件"""

                # 加仓后重置减仓状态
                self.current_phase_step = PHASE_STEP.PHASE_STEP_ONE

                # 加仓后的目标仓位价值
                next_phase = current_phase + 1
                next_phase = min(len(self.phase_position_values) - 1, next_phase)
                target_position_value = self.phase_position_values[next_phase]

                # 计算加仓的合约数量
                changed_volume = ((target_position_value - abs(self.position) * self.position_price)) / trade_price
                changed_volume = round_to(changed_volume, self.symbol_min_volume)

                # 目标仓位合约数量
                target_position = abs(self.position) + changed_volume

                # 加仓后更新阶段仓位合约数量
                self.phase_position_volume = target_position

                # 更新持仓价格
                self.position_price = (changed_volume * trade_price + abs(self.position) * self.position_price) / (abs(self.position) + changed_volume)

                if changed_volume:
                    # 当前持仓数量更新、发起订单
                    if self.direction == Direction.LONG:
                        self.position = target_position
                        if changed_volume > 0:
                            # 加仓
                            self.newSignal(
                                Direction.LONG,
                                Offset.OPEN,
                                trade_price,
                                abs(changed_volume),
                            )

                        elif changed_volume < 0:
                            # 平仓
                            self.newSignal(
                                Direction.SHORT,
                                Offset.CLOSE,
                                trade_price,
                                abs(changed_volume),
                            )

                    elif self.direction == Direction.SHORT:
                        self.position = target_position * -1
                        if changed_volume > 0:
                            # 加仓
                            self.newSignal(
                                Direction.SHORT,
                                Offset.OPEN,
                                trade_price,
                                abs(changed_volume),
                            )

                        elif changed_volume < 0:
                            # 平仓
                            self.newSignal(
                                Direction.LONG,
                                Offset.CLOSE,
                                trade_price,
                                abs(changed_volume),
                            )
                    
                    else:
                        exit("检查代码！")

    def calculate_indicator(self):
        """计算入场指标"""

        # 当前仓位阶段
        current_phase = self.get_current_phase()
        phase_position_value = self.phase_position_values[current_phase]

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

            # ====== 加仓价格 ======
            if self.direction == Direction.LONG:
                if current_phase == 0:
                    self.position_increase_price = self.position_price * (1 - 0.02)

                elif current_phase == 1:
                    self.position_increase_price = self.position_price * (1 - 0.04)

                else:
                    self.position_increase_price = self.position_price * (1 - 0.08)

            elif self.direction == Direction.SHORT:
                if current_phase == 0:
                    self.position_increase_price = self.position_price * (1 + 0.02)

                elif current_phase == 1:
                    self.position_increase_price = self.position_price * (1 + 0.04)

                else:
                    self.position_increase_price = self.position_price * (1 + 0.08)

    def newSignal(self, direction, offset, price, volume):
        self.portfolio.newSignal(self, direction, offset, price, volume)

class MartingPortfolio(object):
    def __init__(self, engine):
        self.engine = engine
        self.portfolioValue = 0  # 组合市值
        self.signalDict = defaultdict(list)
        self.tradingDict = {}  # 交易中的信号字典
        self.posDict = {}  # 合约持仓量字典
        self.signalPosDict = {}  # 策略持仓量字典
        self.signalTradesDict = {}  # 策略成交订单字典
        self.sizeDict = {}  # 合约大小字典

    def init(self, portfolioValue, symbolList, sizeDict):
        """"""
        self.portfolioValue = portfolioValue
        self.sizeDict = sizeDict

        for symbol in symbolList:
            signal1 = MartingSignal(self, symbol, Direction.LONG, 9)
            signal2 = MartingSignal(self, symbol, Direction.SHORT, 9)

            l = self.signalDict[symbol]
            l.append(signal1)
            l.append(signal2)

    def onBar(self, bar):
        for signal in self.signalDict[bar.symbol]:
            signal.on_bar(bar)

    def newSignal(self, signal, direction, offset, price, volume):
        # 策略当前持仓数量
        signal_key = f"{signal.symbol}_{signal.direction.value}"
        signal_current_pos = self.signalPosDict.get(signal_key, 0)

        # 合约当前持仓数量
        symbol_current_pos = self.posDict.get(signal.symbol, 0)

        # 平仓
        if offset != Offset.OPEN:
            if direction == Direction.LONG:
                # 策略必须有空头持仓且平仓数量不能超过空头持仓
                if signal_current_pos >= 0 or volume > abs(signal_current_pos):
                    exit("检查代码！")

            elif direction == Direction.SHORT:
                # 策略必须有多头持仓且平仓数量不能超过多头持仓
                if signal_current_pos <= 0 or volume > abs(signal_current_pos):
                    exit("检查代码！")

            else:
                exit("检查代码！")

        # 获取当前交易中的信号，如果不是本信号，则忽略
        currentSignal = self.tradingDict.get(signal_key, None)
        if not currentSignal:
            self.tradingDict[signal_key] = signal

        elif currentSignal is not signal:
            exit("检查代码！")

        # 保存合约持仓数量
        if direction == Direction.LONG:
            self.signalPosDict[signal_key] = signal_current_pos + volume
            self.posDict[signal.symbol] = symbol_current_pos + volume

        else:
            self.signalPosDict[signal_key] = signal_current_pos - volume
            self.posDict[signal.symbol] = symbol_current_pos - volume

        # 保存成交数据
        signal_trades_list = self.signalTradesDict.get(signal_key, [])
        trade_data = {"datetime":signal.bar.datetime,
                      "symbol":signal.symbol,
                      "direction":direction,
                      "offset":offset,
                      "volume":volume,
                      "price":price,
                      "signal_position":round_to(signal.position, signal.symbol_min_volume),
                      "signal_position_price":round_to(signal.position_price, signal.symbol_price_tick),
                      "signal_position_value":round_to(signal.position * signal.position_price, 1)}
        signal_trades_list.append(trade_data)
        self.signalTradesDict[signal_key] = signal_trades_list

        # 向回测引擎中发单记录
        self.engine.sendOrder(signal.symbol, direction, offset, price, volume)
