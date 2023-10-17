# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange, Interval
from vnpy.trader.utility import ArrayManager
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
from vnpy.app.cta_strategy.base import DAILY_DB_NAME, DOMINANT_DB_NAME
from enum import Enum
from vnpy.trader.utility import round_to, floor_to, ceil_to
from vnpy.trader.utility import DIR_SYMBOL
import numpy as np
import os
from pathlib import Path
import json
from typing import Callable
from indicators.squeeze_momentum import SqueezeMomentum

UNIT_RATE = 0.1  # 初始开仓价值比率


class BarGenerator:
    def __init__(
        self,
        window: int = 0,
        on_window_bar: Callable = None,
        interval: Interval = Interval.MINUTE,
    ):
        self.bar: BarData = None
        self.hour_bar: BarData = None
        self.window_bar: BarData = None

        self.window: int = window
        self.on_window_bar: Callable = on_window_bar
        self.interval: Interval = interval
        self.interval_count: int = 0

    def update_bar(self, bar: BarData) -> None:
        """
        使用分钟Bar生成目标周期Bar
        """
        if self.interval == Interval.MINUTE:
            self.update_bar_minute_window(bar)
        else:
            self.update_bar_hour_window(bar)

    def update_bar_minute_window(self, bar: BarData) -> None:
        # If not inited, create window bar object
        if not self.window_bar:
            dt = bar.datetime.replace(second=0, microsecond=0)
            self.window_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
            )
        # Otherwise, update high/low price into window bar
        else:
            self.window_bar.high_price = max(self.window_bar.high_price, bar.high_price)
            self.window_bar.low_price = min(self.window_bar.low_price, bar.low_price)

        # Update close price/volume/turnover into window bar
        self.window_bar.close_price = bar.close_price
        self.window_bar.volume += bar.volume
        self.window_bar.turnover += bar.turnover
        self.window_bar.open_interest = bar.open_interest

        # Check if window bar completed
        if not (bar.datetime.minute + 1) % self.window:
            self.on_window_bar(self.window_bar)
            self.window_bar = None

    def update_bar_hour_window(self, bar: BarData) -> None:
        if not self.hour_bar:
            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)
            self.hour_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price,
                volume=bar.volume,
                turnover=bar.turnover,
                open_interest=bar.open_interest,
            )
            return

        finished_bar = None

        # 原始Bar数据为1分钟周期，59分为小时分界点
        # 原始Bar数据为5分钟周期，55分为小时分界点
        if bar.datetime.minute == 55:
            self.hour_bar.high_price = max(self.hour_bar.high_price, bar.high_price)
            self.hour_bar.low_price = min(self.hour_bar.low_price, bar.low_price)

            self.hour_bar.close_price = bar.close_price
            self.hour_bar.volume += bar.volume
            self.hour_bar.turnover += bar.turnover
            self.hour_bar.open_interest = bar.open_interest

            finished_bar = self.hour_bar
            self.hour_bar = None

        # If minute bar of new hour, then push existing window bar
        elif bar.datetime.hour != self.hour_bar.datetime.hour:
            finished_bar = self.hour_bar

            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)
            self.hour_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price,
                volume=bar.volume,
                turnover=bar.turnover,
                open_interest=bar.open_interest,
            )
        # Otherwise only update minute bar
        else:
            self.hour_bar.high_price = max(self.hour_bar.high_price, bar.high_price)
            self.hour_bar.low_price = min(self.hour_bar.low_price, bar.low_price)

            self.hour_bar.close_price = bar.close_price
            self.hour_bar.volume += bar.volume
            self.hour_bar.turnover += bar.turnover
            self.hour_bar.open_interest = bar.open_interest

        # Push finished window bar
        if finished_bar:
            self.on_hour_bar(finished_bar)

    def on_hour_bar(self, bar: BarData) -> None:
        if self.window == 1:
            self.on_window_bar(bar)

        else:
            if not self.window_bar:
                self.window_bar = BarData(
                    symbol=bar.symbol,
                    exchange=bar.exchange,
                    datetime=bar.datetime,
                    gateway_name=bar.gateway_name,
                    open_price=bar.open_price,
                    high_price=bar.high_price,
                    low_price=bar.low_price,
                )
            else:
                self.window_bar.high_price = max(
                    self.window_bar.high_price, bar.high_price
                )
                self.window_bar.low_price = min(
                    self.window_bar.low_price, bar.low_price
                )

            self.window_bar.close_price = bar.close_price
            self.window_bar.volume += bar.volume
            self.window_bar.turnover += bar.turnover
            self.window_bar.open_interest = bar.open_interest

            self.interval_count += 1
            if not self.interval_count % self.window:
                self.interval_count = 0
                self.on_window_bar(self.window_bar)
                self.window_bar = None


class MartingDCASignal(object):
    def __init__(
        self,
        portfolio,
        symbol,
        direction,
        ma_window,
        history_data: dict = {},
        params: dict = {},
    ):
        # 常量
        self.portfolio = portfolio  # 投资组合
        self.symbol = symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.unit_value = self.portfolio.portfolioValue * UNIT_RATE  # 单位持仓价值
        if self.symbol in params:
            params = params[self.symbol]
        self.reduce_rate = params.get("reduce_rate", 0.003)  # 盈利平仓比率
        self.continuous_increase_rate = params.get(
            "continuous_increase_rate", 0.005
        )  # 持续加仓比率
        self.trending_increase_rate = params.get(
            "trending_increase_rate", 0.04
        )  # 趋势加仓比率
        self.trending_open_loss_rate = params.get(
            "trending_open_loss_rate", 0.02
        )  # 趋势加仓时的持仓亏损比率

        self.symbol_min_volume = self.portfolio.engine.min_volume_dict[
            self.symbol
        ]  # 合约最小交易数量
        self.symbol_price_tick = self.portfolio.engine.priceTickDict[
            self.symbol
        ]  # 合约最小价格变动

        self.init_status = history_data.get("sync_status", {})  # 回测初始状态
        self.start_dt = None  # 回测开始时间
        backtesting_to = history_data.get("sync_dt", "")
        if backtesting_to:
            self.start_dt = datetime.strptime(
                backtesting_to, "%Y-%m-%d %H:%M:%S"
            ) + timedelta(minutes=5)

        if not self.symbol_min_volume or not self.symbol_price_tick:
            exit("检查代码！")

        if self.direction != Direction.LONG and self.direction != Direction.SHORT:
            exit("检查代码！")

        # 变量
        self.start = (
            False if self.init_status else True
        )  # 开始回测开关，当有初始状态时，回测Bar数据需要从start_dt开始
        self.bar: BarData = None  # 最新K线
        self.am = ArrayManager(self.ma_window)  # K线容器
        self.bm = BarGenerator(
            window=1, on_window_bar=self.on_hour_bar, interval=Interval.HOUR
        )  # K线生成器
        self.position = 0  # 持仓量
        self.position_price = 0  # 持仓均价
        self.position_reduce_price = 0  # 减仓价格
        self.position_increase_price = 0  # 加仓价格
        self.tag_price = 0  # 标记价格，根据此价格计算下一次加仓价格
        self.max_loss_value = 0  # 当前持仓最大亏损价值
        self.max_loss_rate = ""  # 当前持仓最大亏损比率
        self.ma_price = 0  # 均线价格
        self.trending_step = 0  # 追踪趋势的等级
        self.open_waitting = False  # 等待正在交易的反方向信号平仓才能开仓，且只能从初始仓位开始
        self.top_open_price = 0  # 趋势加仓价格
        self.current_trending_group = []

        # 对冲信号相关
        self.sm_indicator = SqueezeMomentum(
            bb_length=20, bb_factor=2, kc_length=20, kc_factor=1.5
        )

        # 初始化状态
        for name, value in self.init_status.items():
            self.__setattr__(name, value)

        # 初始化历史回测的仓位状态
        self.init_status_close = False if self.position else True

        # 保存到backtesting_history.json的变量
        self.syncs = [
            "position",
            "position_price",
            "position_reduce_price",
            "position_increase_price",
            "tag_price",
            "max_loss_value",
            "max_loss_rate",
            "ma_price",
            "trending_step",
            "open_waitting",
            "current_trending_group",
        ]
        self.saved_sync_data = {}

    def on_bar(self, bar):
        if not bar.check_valid():
            raise ("Bar数据校验不通过！！")
        self.bar = bar
        self.am.update_bar(bar)
        self.bm.update_bar(bar)
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

    def on_hour_bar(self, bar: BarData):
        self.sm_indicator.update_bar(bar)
        sm_signal = self.sm_indicator.generate_signal()
        if sm_signal == self.direction:
            print(f"{bar.datetime}\t{sm_signal}")

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
        if (
            "AAVE" in self.symbol
            and self.direction == Direction.SHORT
            and bar.datetime
            >= datetime.strptime("2023-08-20 19:25:00", "%Y-%m-%d %H:%M:%S")
        ):
            a = 2

        # 获取反方向信号，判断开仓等待
        oppsite_signal = self.portfolio.get_oppsite_signal(self)
        oppsite_signal_key = f"{oppsite_signal.symbol}_{oppsite_signal.direction.value}"
        oppsite_signal_pos = self.portfolio.signalPosDict.get(oppsite_signal_key, 0)
        if abs(oppsite_signal_pos) > 0:
            self.open_waitting = True

        # 检查减仓
        if self.position_reduce_price:
            # 成交价格
            trade_price = round_to(self.ma_price, self.symbol_price_tick)

            # 是否达到目标价位
            reduce_price_cross = False
            if self.direction == Direction.LONG:
                if (
                    self.ma_price >= self.position_reduce_price
                    and bar.low_price <= self.ma_price
                    and bar.high_price >= self.ma_price
                ):
                    reduce_price_cross = True
                    trade_price = floor_to(self.ma_price, self.symbol_price_tick)

            if self.direction == Direction.SHORT:
                if (
                    self.ma_price <= self.position_reduce_price
                    and bar.high_price >= self.ma_price
                    and bar.low_price <= self.ma_price
                ):
                    reduce_price_cross = True
                    trade_price = ceil_to(self.ma_price, self.symbol_price_tick)

            if reduce_price_cross:
                """满足减仓条件"""

                # 在变量更新前进行组合策略更新，已获取仓位变更前的状态数据
                self.portfolio.update_trending(self, False)

                # 组合策略检查最高等级
                self.portfolio.check_top_step(self, False)

                # 成交数量
                trade_volume = abs(self.position)

                # 变量更新
                self.current_trending_group.append(
                    {
                        "action": "CLOSE",
                        "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "trending_step": self.trending_step,
                        "trade_price": trade_price,
                        "position_reduce_price": self.position_reduce_price,
                        "position_increase_price": self.position_increase_price,
                        "tag_price": self.tag_price,
                        "ma_price": self.ma_price,
                        "max_loss_value": self.max_loss_value,
                        "max_loss_rate": self.max_loss_rate,
                    }
                )
                self.position = 0
                self.position_price = 0
                self.tag_price = trade_price
                self.trending_step = 0

                if not self.open_waitting:
                    if self.init_status_close:
                        # 变量更新后发出订单，已获取仓位变更后的状态数据
                        if self.direction == Direction.LONG:
                            self.portfolio.newSignal(
                                self,
                                Direction.SHORT,
                                Offset.CLOSE,
                                trade_price,
                                trade_volume,
                            )

                        elif self.direction == Direction.SHORT:
                            self.portfolio.newSignal(
                                self,
                                Direction.LONG,
                                Offset.CLOSE,
                                trade_price,
                                trade_volume,
                            )

                else:
                    # 正在交易的反方向信号趋势加仓
                    oppsite_signal.top_open_price = trade_price

                    # 判断开仓等待
                    signal_key = f"{self.symbol}_{self.direction.value}"
                    signal_pos = self.portfolio.signalPosDict.get(signal_key, 0)
                    if signal_pos:
                        exit("开仓等待信号有仓位，检查代码！")

                # 初始化历史回测的仓位状态更新
                self.init_status_close = True

                # 取消开仓等待
                self.open_waitting = False
                if not oppsite_signal.trending_step:
                    oppsite_signal.open_waitting = False

                # 更新持仓最大亏损
                self.max_loss_value = 0
                self.max_loss_rate = ""

                # 减仓操作后停止后续加仓判断
                return

        # 检查加仓
        if self.position_increase_price:
            # 成交价格
            trade_price = round_to(self.ma_price, self.symbol_price_tick)

            # 正在交易的信号趋势加仓判断
            increase_price_cross = False
            if not self.open_waitting and self.top_open_price and self.position_price:
                # 基于目标价格的亏损比率
                loss_rate = 0
                if self.direction == Direction.LONG:
                    loss_rate = (self.top_open_price / self.position_price) - 1

                elif self.direction == Direction.SHORT:
                    loss_rate = 1 - (self.top_open_price / self.position_price)

                # 亏损是否达到目标值
                if loss_rate <= (self.trending_increase_rate * -1):
                    increase_price_cross = True
                    trade_price = self.top_open_price

            # 普通加仓判断
            if not increase_price_cross:
                if self.direction == Direction.LONG:
                    if (
                        self.ma_price <= self.position_increase_price
                        and bar.high_price >= self.ma_price
                        and bar.low_price <= self.ma_price
                    ):
                        increase_price_cross = True
                        trade_price = ceil_to(self.ma_price, self.symbol_price_tick)

                if self.direction == Direction.SHORT:
                    if (
                        self.ma_price >= self.position_increase_price
                        and bar.low_price <= self.ma_price
                        and bar.high_price >= self.ma_price
                    ):
                        increase_price_cross = True
                        trade_price = floor_to(self.ma_price, self.symbol_price_tick)

            if increase_price_cross:
                """满足加仓条件"""

                # 加仓的合约数量
                trade_volume = 0

                # 当前持仓价值、目标持仓价值
                current_position_value = abs(self.position) * self.position_price

                # 基于成交价格的亏损比率
                loss_rate = 0
                if self.position_price:
                    if self.direction == Direction.LONG:
                        loss_rate = (trade_price / self.position_price) - 1

                    elif self.direction == Direction.SHORT:
                        loss_rate = 1 - (trade_price / self.position_price)

                if loss_rate > (self.trending_increase_rate * -1):
                    """普通加仓"""

                    # 目标持仓价值【定额加仓】
                    target_position_value = current_position_value + self.unit_value

                    # 计算加仓的合约数量
                    trade_volume = (
                        (target_position_value - current_position_value)
                    ) / trade_price
                    trade_volume = round_to(trade_volume, self.symbol_min_volume)

                    # 加仓数量检查
                    if trade_volume <= 0:
                        exit("加仓数量错误，检查代码！")

                elif self.open_waitting or self.top_open_price:
                    """根据持仓价格百分比加仓"""

                    # 计算加仓数量
                    if self.direction == Direction.LONG:
                        target_positon_price = trade_price * (
                            1 + self.trending_open_loss_rate
                        )

                    elif self.direction == Direction.SHORT:
                        target_positon_price = trade_price * (
                            1 - self.trending_open_loss_rate
                        )

                    trade_volume = (
                        abs(self.position) * target_positon_price
                        - current_position_value
                    ) / (trade_price - target_positon_price)
                    trade_volume = round_to(trade_volume, self.symbol_min_volume)

                    # 加仓数量检查
                    if trade_volume <= 0:
                        exit("加仓数量错误，检查代码！")

                if not self.open_waitting:
                    # 控制单个交易信号持仓价值
                    # target_position_value = (abs(trade_volume) * trade_price) + current_position_value
                    # if target_position_value >= self.portfolio.portfolioValue * 0.3:
                    #     top_cross = self.portfolio.check_top_step(self, True)
                    #     if not top_cross:
                    #         trade_volume = 0

                    # 控制组合的持仓信号总数量
                    trading_signal_count = 0
                    signal_key = f"{self.symbol}_{self.direction.value}"
                    signal_trading = False
                    for key_, pos in self.portfolio.signalPosDict.items():
                        if abs(pos) > 0:
                            trading_signal_count += 1
                            if signal_key == key_:
                                signal_trading = True
                                break
                    if not signal_trading and trading_signal_count >= 5:
                        trade_volume = 0

                    # 控制组合的持仓总价值
                    # total_positon_value = 0
                    # for _, l in self.portfolio.signalDict.items():
                    #     for signal in l:
                    #         if not signal.open_waitting:
                    #             total_positon_value += abs(signal.position) * signal.position_price
                    # total_positon_value_after = total_positon_value + abs(trade_volume) * trade_price
                    # if total_positon_value_after >= self.portfolio.portfolioValue * 25:
                    #     trade_volume = 0

                if trade_volume > 0:
                    # 在变量更新前进行组合策略更新，已获取仓位变更前的状态数据
                    self.portfolio.update_trending(self, True)

                    # 变量更新
                    current_status = {
                        "action": "OPEN",
                        "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "trending_step": self.trending_step,
                        "trade_price": trade_price,
                        "position_reduce_price": self.position_reduce_price,
                        "position_increase_price": self.position_increase_price,
                        "tag_price": self.tag_price,
                        "ma_price": self.ma_price,
                        "max_loss_value": self.max_loss_value,
                        "max_loss_rate": self.max_loss_rate,
                    }
                    if self.trending_step == 0:
                        self.current_trending_group = [current_status]
                    else:
                        self.current_trending_group.append(current_status)

                    if self.direction == Direction.LONG:
                        self.position = abs(self.position) + trade_volume

                    elif self.direction == Direction.SHORT:
                        self.position = (abs(self.position) + trade_volume) * -1

                    self.position_price = (
                        (trade_volume * trade_price) + current_position_value
                    ) / abs(self.position)
                    self.tag_price = trade_price
                    self.trending_step += 1

                    if not self.open_waitting:
                        if self.init_status_close:
                            # 变量更新后发出订单，已获取仓位变更后的状态数据
                            if self.direction == Direction.LONG:
                                self.portfolio.newSignal(
                                    self,
                                    Direction.LONG,
                                    Offset.OPEN,
                                    trade_price,
                                    trade_volume,
                                )

                            elif self.direction == Direction.SHORT:
                                self.portfolio.newSignal(
                                    self,
                                    Direction.SHORT,
                                    Offset.OPEN,
                                    trade_price,
                                    trade_volume,
                                )

                    # 更新持仓最大亏损
                    self.max_loss_value = 0
                    self.max_loss_rate = ""
                    self.calculate_max_loss()

    def calculate_indicator(self):
        """计算入场指标"""

        # 均线价格
        self.ma_price = self.am.sma(self.ma_window)

        # 标记价格
        self.tag_price = self.tag_price if self.tag_price else self.ma_price
        if self.direction == Direction.LONG:
            self.tag_price = max(self.tag_price, self.ma_price)

        elif self.direction == Direction.SHORT:
            self.tag_price = min(self.tag_price, self.ma_price)

        # 减仓价格
        if self.direction == Direction.LONG:
            self.position_reduce_price = self.position_price * (1 + self.reduce_rate)

        elif self.direction == Direction.SHORT:
            self.position_reduce_price = self.position_price * (1 - self.reduce_rate)

        # 加仓价格
        if self.direction == Direction.LONG:
            self.position_increase_price = self.tag_price * (
                1 - self.continuous_increase_rate
            )

        elif self.direction == Direction.SHORT:
            self.position_increase_price = self.tag_price * (
                1 + self.continuous_increase_rate
            )

        # 趋势加仓价格初始化
        self.top_open_price = 0

    def save_sync_data(self):
        status = {}
        for name in self.syncs:
            status[name] = self.__getattribute__(name)
        self.saved_sync_data = {
            "sync_status": status,
            "sync_dt": self.bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
        }


class MartingDCAPortfolio(object):
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
        self.top_step_signal = None
        self.trending_open = True

    def init(
        self, portfolioValue, symbolList, history_file: str = "", params: dict = {}
    ):
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
            signal1 = MartingDCASignal(
                self,
                symbol,
                Direction.LONG,
                9,
                history_data=long_history_data,
                params=params,
            )

            short_signal_key = f"{signal_key}_{Direction.SHORT.value}"
            short_history_data = history_data.get(short_signal_key, {})
            signal2 = MartingDCASignal(
                self,
                symbol,
                Direction.SHORT,
                9,
                history_data=short_history_data,
                params=params,
            )

            l = self.signalDict[symbol]
            l.append(signal1)
            l.append(signal2)

    def load_backtesting_history_data(self, exchange: str, file_name: str):
        history_data = {}

        if file_name:
            dir = os.path.dirname(os.path.realpath(__file__))
            file_path = Path(dir).joinpath(
                f"backtesting_history{DIR_SYMBOL}INVERSE{DIR_SYMBOL}{exchange}{DIR_SYMBOL}{file_name}"
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
            trade_price = trending_update_data["trade_price"]
            last_position_price = trending_update_data["last_position_price"]
            last_max_loss_value = trending_update_data["last_max_loss_value"]
            last_max_loss_rate = trending_update_data["last_max_loss_rate"]

            # 缓存趋势追踪记录
            signal_key = f"{signal.symbol}_{signal.direction.value}"

            # 趋势策略当前持仓价值
            position_value = abs(
                round_to(abs(signal.position) * signal.position_price, 1)
            )

            # 平仓盈亏
            close_pnl = 0
            if not trending:
                direction_v = 1 if signal.direction == Direction.LONG else -1
                close_pnl = (
                    ((trade_price / last_position_price) - 1) * 100 * direction_v
                )
                close_pnl = round_to(close_pnl, 0.01)
                close_pnl = f"{close_pnl}%"

            data = {
                "datetime": self.dt,
                "signal": signal_key,
                "position_price": signal.position_price,
                "position_value": position_value,
                "close_pnl": close_pnl,
                "max_loss_value": last_max_loss_value,
                "max_loss_rate": last_max_loss_rate,
                "trending": trending,
            }
            # "tag_price": signal.tag_price,
            signal_trending_list = self.trending_history_dict.get(signal_key, [])
            signal_trending_list.append(data)
            self.trending_history_dict[signal_key] = signal_trending_list

            # 清空趋势更新缓存字典
            self.trending_update_list = []

    def update_trending(self, signal, trending):
        # ====== 趋势策略信号的开仓/平仓都会调用这个方法，先缓存更新内容，在on_daily完成更新 ======
        trending_data = {
            "signal": signal,
            "trending": trending,
            "trade_price": round_to(signal.ma_price, signal.symbol_price_tick),
            "last_position_price": signal.position_price,
            "last_max_loss_value": signal.max_loss_value,
            "last_max_loss_rate": signal.max_loss_rate,
        }
        self.trending_update_list.append(trending_data)

    def check_top_step(self, signal, top: bool):
        if top:
            if self.top_step_signal and signal != self.top_step_signal:
                return False

            else:
                self.top_step_signal = signal
                return True

        else:
            if signal == self.top_step_signal:
                self.top_step_signal = None
                return True

            else:
                return False

    def get_oppsite_signal(self, signal):
        l = self.signalDict[signal.symbol]
        for target in l:
            if target != signal:
                return target
        return None

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
