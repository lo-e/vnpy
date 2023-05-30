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


class MartingStrategy(CtaTemplate):
    """马丁策略"""

    className = "MartingStrategy"
    author = "loe"

    # 策略参数
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
    syncs = ["pos", "backtesting_to"]

    def __init__(self, ctaEngine, martingPortfolio, setting):
        self.portfolio = martingPortfolio
        self.backtesting = None
        self.backtesting_from = datetime.strptime(
            "2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        )
        self.backtesting_to = None

        self.direction: Direction = Direction.NET  # 交易方向
        self.unit_value = self.portfolio.portfolioValue * 0.5 * 0.01  # 最小持仓价值
        self.symbol_min_volume: float = 0.0
        self.symbol_price_tick: float = 0.0
        self.am = ArrayManager(max(self.ma_window, self.rsi_window + 12))  # K线容器
        self.bar: BarData = None  # 最新K线
        self.position_price = 0  # 持仓均价
        self.ma_price = 0  # 均线价格
        self.position_reduce_price = 0  # 减仓价格
        self.position_increase_price = 0  # 加仓价格
        self.max_loss_value = 0  # 当前持仓最大亏损价值
        self.max_loss_rate = ""  # 当前持仓最大亏损比率
        self.rsi_array = []  # 指定周期内的RSI列表
        self.trending_step = 0  # 追踪趋势的等级
        self.calculate_phase_positions()  # 马丁格尔倍数仓位管理

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

    def backtesting_marting(self):
        if not self.backtesting:
            self.backtesting = MartingBacktesting(
                portfolio_value=self.portfolio.portfolioValue,
                vt_symbol=self.vt_symbol,
                direction=self.direction,
                ma_window=self.ma_window,
                rsi_window=self.rsi_window,
                symbol_min_volume=self.symbol_min_volume,
                symbol_price_tick=self.symbol_price_tick,
            )

        # 载入历史数据获取回测参数
        if self.backtesting_to:
            start_dt = self.backtesting_to + timedelta(minutes=5)

        elif self.backtesting_from:
            start_dt = self.backtesting_from

        else:
            exit("检查代码！")

        backtesting_data = self.cta_engine.load_bar(
            vt_symbol=self.vt_symbol,
            start_dt=start_dt,
            interval=Interval.MINUTE,
            window=5,
            callback=None,
        )
        
        for bar in backtesting_data:
            self.backtesting.on_bar(bar)

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
        self.backtesting_marting()

        return True

    def on_stop(self):
        self.write_log(f"{self.strategy_name}\t策略停止")

    def on_tick(self, tick):
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
                current_multiplier = self.calMultiplier(
                    tick.last_price, direction=Direction.LONG
                )

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
                    if self.portfolio.newSignal(
                        self.vt_symbol, Direction.LONG, Offset.OPEN
                    ):
                        unitChange += 1

            if tick.last_price >= self.longEntry2 and self.virtualUnit < 2:
                action = True
                current_multiplier = self.calMultiplier(
                    tick.last_price, direction=Direction.LONG
                )

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(
                        self.vt_symbol, Direction.LONG, Offset.OPEN
                    ):
                        unitChange += 1

            if tick.last_price >= self.longEntry3 and self.virtualUnit < 3:
                action = True
                current_multiplier = self.calMultiplier(
                    tick.last_price, direction=Direction.LONG
                )

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(
                        self.vt_symbol, Direction.LONG, Offset.OPEN
                    ):
                        unitChange += 1

            if tick.last_price >= self.longEntry4 and self.virtualUnit < 4:
                action = True
                current_multiplier = self.calMultiplier(
                    tick.last_price, direction=Direction.LONG
                )

                self.open(tick.last_price, 1)

                self.longStop = tick.last_price - 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(
                        self.vt_symbol, Direction.LONG, Offset.OPEN
                    ):
                        unitChange += 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.buy(
                        self.bestLimitOrderPrice(tick, Direction.LONG, multi=200),
                        current_multiplier * abs(unitChange),
                    )

                self.put_timer_event()
                return

            # 止损平仓
            if self.virtualUnit > 0:
                longExit = max(self.longStop, self.exitDown)
                if tick.last_price <= longExit:
                    self.close(tick.last_price)
                    self.portfolio.newSignal(
                        self.vt_symbol, Direction.SHORT, Offset.CLOSE
                    )
                    if self.pos > 0:
                        self.sell(
                            self.bestLimitOrderPrice(tick, Direction.SHORT, multi=200),
                            abs(self.pos),
                        )
                    # 平仓后更新最新指标
                    self.updateIndicator()
                    self.hasClose = True

                self.put_timer_event()
                return

        if self.virtualUnit <= 0:
            # 空头开仓加仓
            if tick.last_price <= self.shortEntry1 and self.virtualUnit > -1:
                action = True
                current_multiplier = self.calMultiplier(
                    tick.last_price, direction=Direction.SHORT
                )

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(
                        self.vt_symbol, Direction.SHORT, Offset.OPEN
                    ):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry2 and self.virtualUnit > -2:
                action = True
                current_multiplier = self.calMultiplier(
                    tick.last_price, direction=Direction.SHORT
                )

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(
                        self.vt_symbol, Direction.SHORT, Offset.OPEN
                    ):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry3 and self.virtualUnit > -3:
                action = True
                current_multiplier = self.calMultiplier(
                    tick.last_price, direction=Direction.SHORT
                )

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(
                        self.vt_symbol, Direction.SHORT, Offset.OPEN
                    ):
                        unitChange -= 1

            if tick.last_price <= self.shortEntry4 and self.virtualUnit > -4:
                action = True
                current_multiplier = self.calMultiplier(
                    tick.last_price, direction=Direction.SHORT
                )

                self.open(tick.last_price, -1)

                self.shortStop = tick.last_price + 2 * self.atrVolatility

                preCheck = True
                if current_multiplier <= 0:
                    preCheck = False

                # if self.lastPnl > 0:
                #     preCheck = False

                if preCheck:
                    if self.portfolio.newSignal(
                        self.vt_symbol, Direction.SHORT, Offset.OPEN
                    ):
                        unitChange -= 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.short(
                        self.bestLimitOrderPrice(tick, Direction.SHORT, multi=200),
                        current_multiplier * abs(unitChange),
                    )

                self.put_timer_event()
                return

            # 止损平仓
            if self.virtualUnit < 0:
                shortExit = min(self.shortStop, self.exitUp)
                if tick.last_price >= shortExit:
                    self.close(tick.last_price)
                    self.portfolio.newSignal(
                        self.vt_symbol, Direction.LONG, Offset.CLOSE
                    )
                    if self.pos < 0:
                        self.cover(
                            self.bestLimitOrderPrice(tick, Direction.LONG, multi=200),
                            abs(self.pos),
                        )
                    # 平仓后更新最新指标
                    self.updateIndicator()
                    self.hasClose = True

                self.put_timer_event()
                return

        self.put_timer_event()

    def on_bar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        return

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
    def calMultiplier(self, price, direction: Direction):
        multiplier = 0
        riskValue = self.portfolio.portfolioValue * 0.01
        if self.atrVolatility:
            if direction == Direction.LONG:
                multiplier = (
                    riskValue
                    * (price * (price - 2 * self.atrVolatility))
                    / self.atrVolatility
                )
            elif direction == Direction.SHORT:
                multiplier = (
                    riskValue
                    * (price * (price + 2 * self.atrVolatility))
                    / self.atrVolatility
                )

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
        cost = self.virtualUnit * self.entry  # 计算之前的开仓成本
        cost += change * price  # 加上新仓位的成本
        self.virtualUnit += change  # 更新信号持仓
        self.entry = cost / self.virtualUnit  # 计算新的平均开仓成本

    # 信号平仓
    def close(self, price):
        self.lastPnl = (price - self.entry) * self.virtualUnit

        self.virtualUnit = 0
        self.unit = 0
        self.entry = 0
        self.multiplierList = []


class MartingBacktesting(object):
    def __init__(
        self,
        portfolio_value,
        vt_symbol,
        direction,
        ma_window,
        rsi_window,
        symbol_min_volume,
        symbol_price_tick,
    ):
        # 常量
        self.vt_symbol = vt_symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.rsi_window = rsi_window  # RSI参数
        self.unit_value = portfolio_value * 0.5 * 0.01  # 最小持仓价值
        self.symbol_min_volume = symbol_min_volume  # 合约最小交易数量
        self.symbol_price_tick = symbol_price_tick  # 合约最小价格变动
        if not self.symbol_min_volume or not self.symbol_price_tick:
            exit("检查代码！")

        # 变量
        self.inited = False  # 是否完成初始建仓
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
        self.calculate_phase_positions()  # 马丁格尔倍数仓位管理
        self.trending_step = 0  # 追踪趋势的等级

    def on_bar(self, bar):
        if not bar.check_valid():
            raise ("Bar数据校验不通过！！")
        self.bar = bar
        self.am.update_bar(bar)
        if not self.am.inited:
            return

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
        current_phase_position_value = (
            abs(self.position) * self.position_price
        )
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

            # 完成初始建仓
            self.inited = True

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
            # fake
            if self.symbol == "SANDUSDT.BYBIT" and self.direction == Direction.LONG:
                if self.bar.datetime >= datetime.strptime(
                    "2023-05-24 23:00:00", "%Y-%m-%d %H:%M:%S"
                ):
                    a = 2

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
                        exit("检查代码！")

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

    def newSignal(self, direction, offset, price, volume):
        self.portfolio.newSignal(self, direction, offset, price, volume)
