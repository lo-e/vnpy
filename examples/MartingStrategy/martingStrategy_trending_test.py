# encoding: UTF-8

from collections import defaultdict
from rsa import sign
from vnpy.trader.constant import Direction, Offset, Exchange
from vnpy.trader.utility import ArrayManager
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
import re
from vnpy.app.cta_strategy.base import DAILY_DB_NAME, DOMINANT_DB_NAME
from enum import Enum
from vnpy.trader.utility import round_to, floor_to, ceil_to
from vnpy.trader.utility import DIR_SYMBOL
import numpy as np
import os
from pathlib import Path
import json


class MartingSignal(object):
    def __init__(
        self,
        portfolio,
        symbol,
        direction,
        ma_window,
        rsi_window,
        history_data: dict = {},
    ):
        # 常量
        self.portfolio = portfolio  # 投资组合
        self.symbol = symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.rsi_window = rsi_window  # RSI参数
        self.unit_value = self.portfolio.portfolioValue * 0.5 * 0.01  # 最小持仓价值
        self.symbol_min_volume = self.portfolio.engine.min_volume_dict[
            self.symbol
        ]  # 合约最小交易数量
        self.symbol_price_tick = self.portfolio.engine.priceTickDict[
            self.symbol
        ]  # 合约最小价格变动

        self.init_status = history_data.get("backtesting_status", {})  # 回测初始状态
        self.start_dt = None  # 回测开始时间
        backtesting_to = history_data.get("backtesting_to", "")
        if backtesting_to:
            self.start_dt = datetime.strptime(
                backtesting_to, "%Y-%m-%d %H:%M:%S"
            ) + timedelta(minutes=5)

        if not self.symbol_min_volume or not self.symbol_price_tick:
            exit("检查代码！")

        # 变量
        self.start = (
            False if self.init_status else True
        )  # 开始回测开关，当有初始状态时，回测Bar数据需要从start_dt开始
        self.bar: BarData = None  # 最新K线
        self.am = ArrayManager(max(self.ma_window, self.rsi_window + 12))  # K线容器
        self.position = 0  # 持仓量
        self.position_price = 0  # 持仓均价
        self.position_reduce_price = 0  # 减仓价格
        self.position_increase_price = 0  # 加仓价格
        self.max_loss_value = 0  # 当前持仓最大亏损价值
        self.max_loss_rate = ""  # 当前持仓最大亏损比率
        self.ma_price = 0  # 均线价格
        self.trending_step = 0  # 追踪趋势的等级
        self.next_trending_step = 0  # 下一个趋势追踪等级
        self.current_trending_group = [] # 此轮趋势追踪各等级状态列表
        self.close_enable = True # 平仓许可，看趋势追踪后合约bar价格与ma价格的相对位置是否满足要求
        self.calculate_phase_positions()  # 马丁格尔倍数仓位管理

        # 初始化状态
        for name, value in self.init_status.items():
            self.__setattr__(name, value)

        # 保存到backtesting_history.json的变量
        self.syncs = [
            "position",
            "position_price",
            "position_reduce_price",
            "position_increase_price",
            "max_loss_value",
            "max_loss_rate",
            "ma_price",
            "trending_step",
            "next_trending_step",
            "current_trending_group",
            "close_enable",
        ]
        self.saved_sync_data = {}

    def on_bar(self, bar):
        if not bar.check_valid():
            raise ("Bar数据校验不通过！！")
        self.bar = bar
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # 检查是否可以开始回测
        if not self.start:
            if not self.start_dt:
                exit("回测有初始状态，但没有开始时间！")

            if bar.datetime < self.start_dt:
                # 未达到开始时间
                return

            elif bar.datetime == self.start_dt:
                # 开始回测
                self.start = True

            else:
                # 开始回测时间的Bar数据缺失
                exit("开始回测时间的Bar数据缺失！")

        self.calculate_max_loss()
        self.generate_signal(bar)
        self.calculate_indicator()
        self.save_sync_data()

    def calculate_phase_positions(self):
        self.phase_position_values = []
        total_phase_count = 3
        for i in range(total_phase_count):
            phase_position = self.unit_value * (2 ** (i + 1) - 1)
            self.phase_position_values.append(phase_position)

    def get_current_phase(self):
        current_phase_position_value = abs(self.position) * self.position_price
        for i in range(len(self.phase_position_values)):
            phase_positon_value = self.phase_position_values[i]
            if current_phase_position_value <= phase_positon_value * 1.1:
                return i
        return len(self.phase_position_values) - 1

    def calculate_max_loss(self):
        if self.direction == Direction.LONG:
            current_pnl = (self.bar.low_price - self.position_price) * self.position
            if current_pnl < self.max_loss_value:
                self.max_loss_value = current_pnl
                self.max_loss_value = round_to(self.max_loss_value, 1)

                self.max_loss_rate = (
                    (self.bar.low_price / self.position_price) - 1
                ) * 100
                self.max_loss_rate = round_to(self.max_loss_rate, 0.01)
                self.max_loss_rate = f"{self.max_loss_rate}%"

        elif self.direction == Direction.SHORT:
            current_pnl = (self.bar.high_price - self.position_price) * self.position
            if current_pnl < self.max_loss_value:
                self.max_loss_value = current_pnl
                self.max_loss_value = round_to(self.max_loss_value, 1)

                self.max_loss_rate = (
                    (self.bar.high_price / self.position_price) - 1
                ) * -100
                self.max_loss_rate = round_to(self.max_loss_rate, 0.01)
                self.max_loss_rate = f"{self.max_loss_rate}%"

        else:
            exit("检查代码！")

    def generate_signal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # fake
        if self.symbol == "CHZUSDT.BYBIT" and self.direction == Direction.LONG:
            if self.bar.datetime >= datetime.strptime(
                "2023-05-12 20:05:00", "%Y-%m-%d %H:%M:%S"
            ):
                a = 2

        # 当前仓位阶段
        current_phase = self.get_current_phase()

        # 下一持仓阶段
        next_phase = current_phase + 1

        # 初始化仓位
        if not self.position:
            # 成交价格
            trade_price = round_to(bar.close_price, self.symbol_price_tick)

            # 初始化持仓价格
            self.position_price = trade_price

            # 初始化持仓合约数量
            init_volume = self.unit_value / self.position_price
            init_volume = round_to(init_volume, self.symbol_min_volume)

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
            # 是否达到目标价位
            reduce_price_cross = False

            # 成交价格
            trade_price = round_to(self.ma_price, self.symbol_price_tick)

            # 判断是否趋势追踪平仓许可
            if not self.close_enable:
                if (self.direction == Direction.LONG and bar.low_price > self.ma_price) or (self.direction == Direction.SHORT and bar.high_price < self.ma_price):
                    self.close_enable = True

            if self.close_enable:
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

                if self.trending_step > 0:
                    # 组合策略取消趋势追踪
                    self.portfolio.update_trending(self, False)

                # 初始化趋势追踪等级
                self.trending_step = 0

                # 初始化趋势追踪组合
                self.current_trending_group = []

                # 平仓
                self.position_price = trade_price
                target_position_value = self.unit_value

                # 计算减仓的合约数量
                changed_volume = (target_position_value / self.position_price) - abs(
                    self.position
                )
                changed_volume = round_to(changed_volume, self.symbol_min_volume)

                # 目标仓位合约数量
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

                # 初始化仓位最大亏损
                self.max_loss_value = 0
                self.max_loss_rate = ""

                # 减仓操作后停止后续加仓判断
                return

        # 检查加仓
        if self.position_increase_price:
            # 成交价格
            trade_price = round_to(self.ma_price, self.symbol_price_tick)

            # 是否达到目标价位
            increase_price_cross = False
            if self.direction == Direction.LONG:
                if next_phase >= len(self.phase_position_values):
                    # ====== 趋势行情 ======
                    max_fall = ((self.bar.low_price / self.bar.open_price) - 1) * 100
                    close_fall = ((self.bar.close_price / self.bar.open_price) - 1) * 100
                    if max_fall <= -5 and close_fall > max_fall / 2.0:
                        increase_price_cross = True
                        trade_price = round_to(self.bar.close_price, self.symbol_price_tick)

                else:
                    # ====== 震荡行情 ======
                    if (self.ma_price <= self.position_increase_price
                        and bar.high_price > trade_price
                        and bar.low_price <= trade_price
                    ):
                        increase_price_cross = True

            if self.direction == Direction.SHORT:
                if next_phase >= len(self.phase_position_values):
                    # ====== 趋势行情 ======
                    max_rise = ((self.bar.high_price / self.bar.open_price) - 1) * 100
                    close_rise = ((self.bar.close_price / self.bar.open_price) - 1) * 100
                    if max_rise >= 5 and close_rise < max_rise / 2.0:
                        increase_price_cross = True
                        trade_price = round_to(self.bar.close_price, self.symbol_price_tick)
                
                else:
                    # ====== 震荡行情 ======
                    if (self.ma_price >= self.position_increase_price
                        and bar.low_price < trade_price
                        and bar.high_price >= trade_price
                    ):
                        increase_price_cross = True

            if increase_price_cross:
                """价格满足加仓条件"""

                # 加仓的合约数量
                changed_volume = 0

                if next_phase >= len(self.phase_position_values):
                    # ====== 趋势行情 ======

                    # 当前持仓价值
                    current_position_value = abs(self.position) * self.position_price

                    # 更新持仓价格
                    price_rate = 0.01
                    if self.direction == Direction.LONG:
                        target_positon_price = trade_price * (1 + price_rate)

                    elif self.direction == Direction.SHORT:
                        target_positon_price = trade_price * (1 - price_rate)

                    else:
                        exit("检查代码！")

                    # 计算加仓的合约数量
                    # current_position_value + changed_volume * trade_price = (abs(self.position) + changed_volume) * self.position_price
                    # current_position_value + changed_volume * trade_price = abs(self.position) * self.position_price + changed_volume * self.position_price
                    # changed_volume * (trade_price - self.position_price) = abs(self.position) * self.position_price - current_position_value
                    changed_volume = (
                        abs(self.position) * target_positon_price
                        - current_position_value
                    ) / (trade_price - target_positon_price)
                    changed_volume = round_to(changed_volume, self.symbol_min_volume)

                    # 更新持仓价格
                    self.position_price = target_positon_price

                    # 新的趋势策略信号
                    self.portfolio.update_trending(self, True)
                    self.trending_step += 1
                    self.current_trending_group.append({"datetime":bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                                        "max_loss_value":self.max_loss_value,
                                                        "max_loss_rate":self.max_loss_rate})
                    self.close_enable = False

                else:
                    # ====== 震荡行情 ======

                    # 目标持仓价值
                    target_position_value = self.phase_position_values[next_phase]

                    # 计算加仓的合约数量
                    changed_volume = (
                        (
                            target_position_value
                            - abs(self.position) * self.position_price
                        )
                    ) / trade_price
                    changed_volume = round_to(changed_volume, self.symbol_min_volume)

                    # 更新持仓价格
                    self.position_price = (
                        changed_volume * trade_price
                        + abs(self.position) * self.position_price
                    ) / (abs(self.position) + changed_volume)

                # 目标仓位合约数量
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

                # 加仓需要变更最大亏损比率，基于加仓后的持仓价值
                self.max_loss_rate = (
                    self.max_loss_value / (abs(self.position) * self.position_price)
                ) * 100
                self.max_loss_rate = round_to(self.max_loss_rate, 0.01)
                self.max_loss_rate = f"{self.max_loss_rate}%"

    def calculate_indicator(self):
        """计算入场指标"""

        # 当前仓位阶段
        current_phase = self.get_current_phase()

        # 下一趋势等级
        next_phase = current_phase + 1
        if next_phase >= len(self.phase_position_values):
            self.next_trending_step = self.trending_step + 1

        else:
            self.next_trending_step = 0

        # 均线价格
        self.ma_price = self.am.sma(self.ma_window)

        if self.position_price:
            # ====== 减仓价格 ======
            if self.direction == Direction.LONG:
                self.position_reduce_price = self.position_price * (1 + 0.01)

            elif self.direction == Direction.SHORT:
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

    def save_sync_data(self):
        status = {}
        for name in self.syncs:
            status[name] = self.__getattribute__(name)
        self.saved_sync_data = {
            "backtesting_status": status,
            "backtesting_to": self.bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
        }

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
        self.trending_update_list = []  # 趋势策略信号的更新先缓存在这里，在on_daily完成更新
        self.trending_history_dict = {}  # 缓存追踪过的趋势策略
        self.dt = None  # 当前回测时间
        self.trending_open = True

    def init(self, portfolioValue, symbolList, history_file: str = ""):
        self.portfolioValue = portfolioValue

        # 回测历史数据
        exchange = symbolList[0].split(".")[-1]
        history_data = self.load_backtesting_history_data(
            exchange=exchange, file_name=history_file
        )

        for symbol in symbolList:
            # 创建策略信号，并根据历史回测数据初始化
            pure_symbol = symbol[: symbol.index("USDT")]
            signal_key = f"MARTING_{exchange}_{pure_symbol}"

            long_signal_key = f"{signal_key}_{Direction.LONG.value}"
            long_history_data = history_data.get(long_signal_key, {})
            signal1 = MartingSignal(
                self, symbol, Direction.LONG, 9, 14, history_data=long_history_data
            )

            short_signal_key = f"{signal_key}_{Direction.SHORT.value}"
            short_history_data = history_data.get(short_signal_key, {})
            signal2 = MartingSignal(
                self, symbol, Direction.SHORT, 9, 14, history_data=short_history_data
            )

            l = self.signalDict[symbol]
            l.append(signal1)
            l.append(signal2)

            # 根据历史回测数据给策略组合初始化
            long_signal_position = 0
            if long_history_data:
                long_signal_position = long_history_data["backtesting_status"][
                    "position"
                ]
                long_signal_position_key = f"{symbol}_{Direction.LONG.value}"
                self.signalPosDict[long_signal_position_key] = long_signal_position

            short_signal_position = 0
            if short_history_data:
                short_signal_position = short_history_data["backtesting_status"][
                    "position"
                ]
                short_signal_position_key = f"{symbol}_{Direction.SHORT.value}"
                self.signalPosDict[short_signal_position_key] = short_signal_position

            self.posDict[symbol] = long_signal_position + short_signal_position

    def load_backtesting_history_data(self, exchange: str, file_name: str):
        history_data = {}

        if file_name:
            dir = os.path.dirname(os.path.realpath(__file__))
            file_path = Path(dir).joinpath(
                f"backtesting_history{DIR_SYMBOL}{exchange}{DIR_SYMBOL}{file_name}"
            )
            if file_path.exists():
                with open(file_path, mode="r", encoding="UTF-8") as f:
                    history_data = json.load(f)
        return history_data

    def onBar(self, bar):
        if not self.dt or self.dt != bar.datetime:
            self.dt = bar.datetime
            self.on_daily()

        for signal in self.signalDict[bar.symbol]:
            signal.on_bar(bar)

    def on_daily(self):
        # 更新当前的趋势信号
        for trending_update_data in self.trending_update_list:
            signal = trending_update_data["signal"]
            trending = trending_update_data["trending"]
            max_loss_value = trending_update_data["max_loss_value"]
            max_loss_rate = trending_update_data["max_loss_rate"]

            # 缓存趋势追踪记录
            signal_key = f"{signal.symbol}_{signal.direction.value}"

            # 趋势策略当前持仓均价
            position_price = signal.position_price

            # 趋势策略当前持仓价值
            position_value = abs(round_to(signal.position * signal.position_price, 1))

            data = {
                "datetime": self.dt,
                "signal": signal_key,
                "position_price": position_price,
                "position_value": position_value,
                "max_loss_value": max_loss_value,
                "max_loss_rate": max_loss_rate,
                "trending": trending,
            }
            signal_trending_list = self.trending_history_dict.get(signal_key, [])
            signal_trending_list.append(data)
            self.trending_history_dict[signal_key] = signal_trending_list

            # 清空趋势更新缓存字典
            self.trending_update_list = []

    def update_trending(self, signal, trending):
        # ====== 趋势策略信号的开仓/平仓都会调用这个方法，先缓存更新内容，在on_daily完成更新 ======
        max_loss_value = signal.max_loss_value
        max_loss_rate = signal.max_loss_rate
        trending_data = {
            "signal": signal,
            "trending": trending,
            "max_loss_value": max_loss_value,
            "max_loss_rate": max_loss_rate,
        }
        self.trending_update_list.append(trending_data)

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
        trade_data = {
            "datetime": signal.bar.datetime,
            "symbol": signal.symbol,
            "direction": direction,
            "offset": offset,
            "volume": volume,
            "price": price,
            "signal_position": round_to(signal.position, signal.symbol_min_volume),
            "signal_position_price": round_to(
                signal.position_price, signal.symbol_price_tick
            ),
            "signal_position_value": round_to(
                abs(signal.position) * signal.position_price, 1
            ),
            "max_loss_value": signal.max_loss_value,
            "max_loss_rate": signal.max_loss_rate,
        }
        signal_trades_list.append(trade_data)
        self.signalTradesDict[signal_key] = signal_trades_list

        # 向回测引擎中发单记录
        self.engine.sendOrder(signal.symbol, direction, offset, price, volume)
