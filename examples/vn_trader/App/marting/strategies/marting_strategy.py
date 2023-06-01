# encoding: UTF-8

"""
使用马丁式加仓的趋势追踪策略
"""

from vnpy.trader.constant import Direction, Offset
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.utility import ArrayManager
from vnpy.app.cta_strategy.base import *
from datetime import datetime, timedelta
from vnpy.trader.constant import Interval
import csv
import os
from vnpy.trader.object import BarData
from vnpy.trader.utility import round_to, floor_to, ceil_to
import numpy as np
from threading import Thread
from utilities.BarGenerator import BarGenerator

class MartingStrategy(CtaTemplate):
    """马丁策略"""

    className = "MartingStrategy"
    author = "loe"

    # 策略参数
    interval_window = 5  # 数据的时间周期5分钟
    ma_window = 9  # 均线参数
    rsi_window = 14  # RSI参数

    # 参数列表，保存了参数的名称
    parameters = [
        "strategy_name",
        "vt_symbol",
        "direction",
        "ma_window",
        "rsi_window",
        "unit_value",
    ]

    # 变量列表，保存了变量的名称
    variables = [
        "is_backtesting",
        "symbol_price_tick",
        "symbol_min_volume",
        "position_price",
        "ma_price",
        "position_reduce_price",
        "position_increase_price",
        "max_loss_value",
        "max_loss_rate",
        "trending_step",
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = ["backtesting_to", "backtesting_status"]

    def __init__(self, ctaEngine, martingPortfolio, setting):
        # 组合管理引擎
        self.portfolio = martingPortfolio

        # 回测相关
        self.backtesting = None
        self.backtesting_from = datetime.strptime(
            "2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        )
        self.backtesting_to = None
        self.backtesting_status = {}
        self.is_backtesting = False

        # 策略变量
        self.direction: Direction = Direction.NET  # 交易方向
        self.unit_value = self.portfolio.portfolioValue * 0.5 * 0.01  # 最小持仓价值
        self.symbol_min_volume: float = 0.0
        self.symbol_price_tick: float = 0.0
        self.bar: BarData = None  # 最新K线
        self.position_price = 0  # 持仓均价
        self.ma_price = 0  # 均线价格
        self.position_reduce_price = 0  # 减仓价格
        self.position_increase_price = 0  # 加仓价格
        self.max_loss_value = 0  # 当前持仓最大亏损价值
        self.max_loss_rate = ""  # 当前持仓最大亏损比率
        self.rsi_array = []  # 指定周期内的RSI列表
        self.trending_step = 0  # 追踪趋势的等级
        self.window_bar_generator = BarGenerator(
            window=self.interval_window,
            on_window_bar=self.on_bar,
            interval=Interval.MINUTE,
        )  # 5分钟Bar生成工具
        self.minute_bar_generator = BarGenerator(
            on_bar=self.window_bar_generator.update_bar
        )  # 1分钟Bar生成工具
        self.window_bar_list = []  # 基于实时Tick数据生成的周期Bar数据列表
        self.calculate_phase_positions()  # 马丁格尔仓位管理

        # 完成setting.json参数的配置
        super(MartingStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

        # 对setting.json特殊字段处理
        self.direction = (
            Direction.LONG
            if self.direction == "多"
            else (Direction.SHORT if self.direction == "空" else Direction.NET)
        )

    def calculate_phase_positions(self):
        self.phase_position_values = []
        total_phase_count = 3
        for i in range(total_phase_count):
            phase_position = self.unit_value * (2 ** (i + 1) - 1)
            self.phase_position_values.append(phase_position)

    def on_init(self):
        self.write_log(f"{self.strategy_name}\t策略初始化")

    def on_start(self):
        # 交易合约缺失
        contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
        if not contract:
            return False

        # 交易方向设置错误
        if self.direction != Direction.LONG and self.direction != Direction.SHORT:
            return False

        # 设置必要的合约相关参数
        self.symbol_min_volume = contract.min_volume
        self.symbol_price_tick = contract.pricetick

        # 回测数据
        self.start_backtesting()
        return True

    def start_backtesting(self):
        self.write_log(f"开启回测线程")
        self.is_backtesting = True
        thread = Thread(target=self.backtesting_marting)
        thread.start()

    def backtesting_marting(self):
        backtestint_start = (
            self.backtesting_to + timedelta(minutes=self.interval_window)
            if self.backtesting_to
            else None
        )

        self.backtesting = MartingBacktesting(
            vt_symbol=self.vt_symbol,
            direction=self.direction,
            ma_window=self.ma_window,
            rsi_window=self.rsi_window,
            symbol_min_volume=self.symbol_min_volume,
            symbol_price_tick=self.symbol_price_tick,
            init_status=self.backtesting_status,
            start_dt=backtestint_start,
        )

        # 载入历史数据获取回测参数
        if self.backtesting_to:
            start_dt = self.backtesting_to - timedelta(days=1)

        elif self.backtesting_from:
            start_dt = self.backtesting_from

        else:
            raise ("检查代码！")

        backtesting_data = self.cta_engine.load_bar(
            vt_symbol=self.vt_symbol,
            start_dt=start_dt,
            interval=Interval.MINUTE,
            window=5,
            callback=None,
        )

        # 剔除最后一个Bar数据，保证数据的准确性
        backtesting_data = backtesting_data[0:-1]
        for bar in backtesting_data:
            self.backtesting.on_bar(bar)

        # 回测完成保存回测状态
        status = {}
        for name in self.backtesting.syncs:
            status[name] = self.backtesting.__getattribute__(name)
        self.backtesting_status = status
        self.backtesting_to = backtesting_data[-1].datetime

        # 结束回测
        self.is_backtesting = False
        self.write_log(f"回测结束：{self.backtesting_to}")

        # 策略状态更新
        self.put_timer_event()

    def on_stop(self):
        self.write_log(f"{self.strategy_name}\t策略停止")

    def on_tick(self, tick):
        if not self.trading:
            return

        # 给分钟Bar生成器推送数据
        self.minute_bar_generator.update_tick(tick=tick)

        # 策略状态更新
        self.put_timer_event()

    def on_bar(self, bar):
        """基于实时Tick数据生成的周期Bar数据推送"""
        # 保存到列表
        self.window_bar_list.append(bar)
        if len(self.window_bar_list) > 10:
            self.window_bar_list.pop(0)

        # 打开组合引擎数据下载开关
        self.portfolio.download_enable = True

        # 策略状态更新
        self.put_timer_event()

    def on_order(self, order):
        """委托推送"""
        pass

    def on_trade(self, trade):
        """成交推送"""
        # 邮件提醒
        super(MartingStrategy, self).on_trade(trade)

class MartingBacktesting(object):
    def __init__(
        self,
        vt_symbol: str,
        direction: Direction,
        ma_window: int,
        rsi_window: int,
        symbol_min_volume: float,
        symbol_price_tick: float,
        init_status: dict,
        start_dt: datetime,
    ):
        # 常量
        self.unit_value = 1000000 * 0.5 * 0.01  # 最小持仓价值
        self.vt_symbol = vt_symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.rsi_window = rsi_window  # RSI参数
        self.symbol_min_volume = symbol_min_volume  # 合约最小交易数量
        self.symbol_price_tick = symbol_price_tick  # 合约最小价格变动
        self.init_status = init_status  # 回测初始状态
        self.start_dt = start_dt  # 回测开始时间
        if not self.symbol_min_volume or not self.symbol_price_tick:
            raise ("检查代码！")

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
        self.rsi_array = []
        self.trending_step = 0  # 追踪趋势的等级
        self.calculate_phase_positions()  # 马丁格尔倍数仓位管理

        # 同步保存到数据库的变量
        self.syncs = [
            "position",
            "position_price",
            "position_reduce_price",
            "position_increase_price",
            "max_loss_value",
            "max_loss_rate",
            "ma_price",
            "rsi_array",
            "trending_step",
        ]

        # 初始化状态
        for name, value in self.init_status.items():
            self.__setattr__(name, value)

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
                raise ("回测有初始状态，但没有开始时间！")

            if bar.datetime < self.start_dt:
                # 未达到开始时间
                return

            elif bar.datetime == self.start_dt:
                # 开始回测
                self.start = True

            else:
                # 开始回测时间的Bar数据缺失
                raise ("开始回测时间的Bar数据缺失！")

        self.calculate_max_loss()
        self.generate_signal(bar)
        self.calculate_indicator()

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
            raise ("检查代码！")

    def generate_signal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # fake
        if self.vt_symbol == "BTCUSDT.BYBIT" and self.direction == Direction.LONG:
            if self.bar.datetime >= datetime.strptime(
                "2022-01-12 20:05:00", "%Y-%m-%d %H:%M:%S"
            ):
                a = 2

        # 当前仓位阶段
        current_phase = self.get_current_phase()

        # 初始化仓位
        if not self.position:
            # 成交价格
            trade_price = round_to(bar.close_price, self.symbol_price_tick)

            # 初始化持仓价格
            self.position_price = trade_price

            # 初始化持仓合约数量
            init_volume = self.unit_value / self.position_price
            init_volume = round_to(init_volume, self.symbol_min_volume)

            # 当前持仓数量更新
            if self.direction == Direction.LONG:
                self.position = init_volume

            elif self.direction == Direction.SHORT:
                self.position = init_volume * -1

            else:
                raise ("检查代码！")

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

                # 初始化趋势追踪等级
                self.trending_step = 0

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
                    # 当前持仓数量更新
                    if self.direction == Direction.LONG:
                        self.position = target_position

                    elif self.direction == Direction.SHORT:
                        self.position = target_position * -1

                    else:
                        raise ("检查代码！")

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
                # 根据RSI判断是否超卖
                rsi_cross = False
                for rsi in self.rsi_array:
                    if rsi <= 25:
                        rsi_cross = True
                        break

                if (
                    rsi_cross
                    and self.ma_price <= self.position_increase_price
                    and bar.high_price > trade_price
                    and bar.low_price <= trade_price
                ):
                    increase_price_cross = True

            if self.direction == Direction.SHORT:
                # 根据RSI判断是否超买
                rsi_cross = False
                for rsi in self.rsi_array:
                    if rsi >= 75:
                        rsi_cross = True
                        break

                if (
                    rsi_cross
                    and self.ma_price >= self.position_increase_price
                    and bar.low_price < trade_price
                    and bar.high_price >= trade_price
                ):
                    increase_price_cross = True

            if increase_price_cross:
                """价格满足加仓条件"""

                # 加仓的合约数量
                changed_volume = 0

                # 下一持仓阶段
                next_phase = current_phase + 1

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
                        raise ("检查代码！")

                    # 计算加仓的合约数量
                    # current_position_value + changed_volume * trade_price = (abs(self.position) + changed_volume) * self.position_price
                    # current_position_value + changed_volume * trade_price = abs(self.position) * self.position_price + changed_volume * self.position_price
                    # changed_volume * (trade_price - self.position_price) = abs(self.position) * self.position_price - current_position_value
                    changed_volume = (
                        abs(self.position) * target_positon_price
                        - current_position_value
                    ) / (trade_price - target_positon_price)

                    # 更新持仓价格
                    self.position_price = target_positon_price

                    # 新的趋势策略信号
                    self.trending_step += 1

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
                    # 当前持仓数量更新
                    if self.direction == Direction.LONG:
                        self.position = target_position

                    elif self.direction == Direction.SHORT:
                        self.position = target_position * -1

                    else:
                        raise ("检查代码！")

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

        # 均线价格
        self.ma_price = self.am.sma(self.ma_window)

        # RSI指标
        self.rsi_array = []
        rsi_result = self.am.rsi(self.rsi_window, True)
        for rsi in rsi_result:
            if not np.isnan(rsi):
                self.rsi_array.append(rsi)

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
