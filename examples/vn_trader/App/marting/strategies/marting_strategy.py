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
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import round_to, floor_to, ceil_to
import numpy as np
from threading import Thread
from utilities.BarGenerator import BarGenerator
from App.marting.martingPortfolio import BAR_DOWNLOAD_GENERATE_COMPLETE
from vnpy.event import Event
from copy import copy


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
        "init_value_rate",
        "bottom_step",
        "top_step",
    ]

    # 变量列表，保存了变量的名称
    variables = [
        "strategy_name",
        "init_value_rate",
        "bottom_step",
        "top_step",
        "symbol_price_tick",
        "symbol_min_volume",
        "backtesting_to",
        "strategy_to",
        "tick_dt",
        "tick_trade_enable",
        "latest_price",
        "position_value",
        "position_price",
        "position_close_price",
        "position_increase_price",
        "current_pnl_rate",
        "trending_step",
        "target_volume",
        "strategy_position_price",
        "strategy_position_reduce_price",
        "strategy_position_increase_price",
        "strategy_max_loss_value",
        "strategy_max_loss_rate",
        "strategy_current_pnl_rate",
        "strategy_trending_step",
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
        "backtesting_to",
        "backtesting_status",
        "position_value",
        "position_price",
        "position_close_price",
        "position_increase_price",
        "trending_step",
    ]

    # 监控列表
    monitors = ["tick_trade_enable"]

    def __init__(self, ctaEngine, martingPortfolio, setting):
        # 组合管理引擎
        self.portfolio = martingPortfolio

        # 回测相关
        self.backtesting = None
        self.backtesting_from = datetime.strptime(
            "2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        )
        self.backtesting_to: datetime = None
        self.backtesting_status = {}
        self.is_backtesting = False
        self.backtesting_wait = 100
        self.strategy_to: datetime = None
        self.strategy_status = {}

        # 回测结果相关
        self.strategy_position_price = 0  # 持仓均价
        self.strategy_position_reduce_price = 0  # 平仓价格
        self.strategy_position_increase_price = 0  # 加仓价格
        self.strategy_max_loss_value = 0  # 最大亏损价值
        self.strategy_max_loss_rate = ""  # 最大亏损比率
        self.strategy_current_pnl_rate = ""  # 当前亏损比率【基于Tick数据实时计算】
        self.strategy_trending_step = 0  # 趋势追踪等级

        # 策略参数
        self.init_value_rate = 0  # 初始持仓价值占组合价值比率
        self.bottom_step = 0  # 趋势追踪最低等级
        self.top_step = 0  # 趋势追踪最高等级

        # 策略变量
        self.tick: TickData = None
        self.direction: Direction = Direction.NET  # 交易方向
        self.symbol_min_volume: float = 0.0
        self.symbol_price_tick: float = 0.0
        self.bar: BarData = None  # 最新K线
        self.position_value = 0  # 持仓价值
        self.position_price = 0  # 持仓均价
        self.position_close_price = 0  # 平仓价格
        self.position_increase_price = 0 # 加仓价格
        self.current_pnl_rate = ""  # 当前持仓亏损比率【基于Tick数据实时计算】
        self.trending_step = 0  # 趋势追踪等级
        self.tick_dt = None # 最新的tick时间
        self.tick_trade_enable = False  # Tick数据时间在回测后的指定范围内允许交易
        self.latest_price = 0 # 最新的tick价格
        self.target_volume = -1  # 目标持仓
        self.window_bar_list = []  # 基于实时Tick数据生成的周期Bar数据列表
        self.monitor_dict = {} # 最新的同步数据，用于检查是否更新，如更新及时同步数据库

        self.window_bar_generator = BarGenerator(
            window=self.interval_window,
            on_window_bar=self.on_bar,
            interval=Interval.MINUTE,
        )  # 5分钟Bar生成工具
        self.minute_bar_generator = BarGenerator(
            on_bar=self.window_bar_generator.update_bar
        )  # 1分钟Bar生成工具

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

        # 监听事件
        self.cta_engine.event_engine.register(
            BAR_DOWNLOAD_GENERATE_COMPLETE, self.portfolio_download_generate_complete
        )

    def on_init(self):
        # 回测历史记录如果数据库没有记录，从json文件中获取
        if not self.backtesting_status or not self.backtesting_to:
            backtesting_history = self.portfolio.strategys_backtesting_history.get(
                self.strategy_name, {}
            )
            if backtesting_history:
                self.backtesting_status = backtesting_history["backtesting_status"]
                self.backtesting_to = datetime.strptime(
                    backtesting_history["backtesting_to"], "%Y-%m-%d %H:%M:%S"
                )

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

    def portfolio_download_generate_complete(self, event: Event):  # 回测数据
        if self.inited:
            self.start_backtesting()

    def start_backtesting(self):
        # 策略组合统一管理回测线程
        self.portfolio.backtesting_queue.put(self.strategy_name)

    def backtesting_marting(self):
        if self.is_backtesting or self.backtesting_wait <= 5:
            return
        self.is_backtesting = True

        try:
            backtestint_start = (
                self.backtesting_to + timedelta(minutes=self.interval_window)
                if self.backtesting_to
                else None
            )

            self.backtesting = MartingBacktesting(
                strategy=self,
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
                data_from = self.backtesting_to - timedelta(hours=5)

            elif self.backtesting_from:
                data_from = self.backtesting_from

            else:
                self.raise_error("开始回测时backtesting_to和backtesting_from缺失")

            backtesting_data = self.cta_engine.load_bar(
                vt_symbol=self.vt_symbol,
                data_from=data_from,
                interval=Interval.MINUTE,
                window=5,
                callback=None,
            )
            if len(backtesting_data) < 50:
                self.raise_error("回测数据缺失！")

            # 剔除最后一个Bar数据，保证数据的准确性
            backtesting_data = backtesting_data[0:-1]

            # 开始历史数据回测
            for bar in backtesting_data:
                self.backtesting.on_bar(bar)

            # 历史数据回测完成保存回测状态
            status = {}
            for name in self.backtesting.syncs:
                status[name] = self.backtesting.__getattribute__(name)
            self.backtesting_status = status
            self.backtesting_to = backtesting_data[-1].datetime
            self.strategy_status = status
            self.strategy_to = backtesting_data[-1].datetime

            # 实时数据
            strategy_data = []
            last_bar = backtesting_data[-1]
            next_bar_dt = last_bar.datetime + timedelta(minutes=self.interval_window)
            for i in range(len(self.window_bar_list)):
                bar = self.window_bar_list[i]
                if bar.datetime == next_bar_dt:
                    strategy_data = self.window_bar_list[i:]
                    break

            # 开始实时数据回测
            for bar in strategy_data:
                self.backtesting.on_bar(bar)

            # 实时数据回测完成保存策略状态
            if strategy_data:
                status = {}
                for name in self.backtesting.syncs:
                    status[name] = self.backtesting.__getattribute__(name)
                self.strategy_status = status
                self.strategy_to = strategy_data[-1].datetime

            # 回测结果
            self.strategy_position_price = self.strategy_status["position_price"]
            self.strategy_position_reduce_price = self.strategy_status[
                "position_reduce_price"
            ]
            self.strategy_position_increase_price = self.strategy_status[
                "position_increase_price"
            ]
            self.strategy_max_loss_value = self.strategy_status["max_loss_value"]
            self.strategy_max_loss_rate = self.strategy_status["max_loss_rate"]
            self.strategy_trending_step = self.strategy_status["trending_step"]

        except:
            pass
        
        # 结束回测
        self.is_backtesting = False
        self.backtesting_wait = 0
        print(f"回测结束 b_to：{self.backtesting_to}\ts_to：{self.strategy_to}")
        self.put_timer_event()

    def on_timer(self):
        # 回测等待
        self.backtesting_wait += 1

        # 周期首尾分钟，手动update_tick
        dt = datetime.now()
        if (not (dt.minute + 1) % self.interval_window) and self.tick and (self.tick.datetime.minute != dt.minute):
            manual_tick = copy(self.tick)
            manual_tick.datetime = dt
            self.on_tick(manual_tick)
            
        if (not dt.minute % self.interval_window) and self.tick and (self.tick.datetime.minute != dt.minute):
            manual_tick = copy(self.tick)
            manual_tick.datetime = dt
            self.on_tick(manual_tick)

        # 检查同步数据
        self.put_sync_event()
        super().on_timer()

    def put_sync_event(self):
        updated = False
        monitor_dict = {}

        # 当前同步数据
        for key in self.syncs:
            value = self.__getattribute__(key)
            monitor_dict[key] = value
        
        for key in self.monitors:
            value = self.__getattribute__(key)
            monitor_dict[key] = value

        # 比较是否有更新
        for key, value in monitor_dict.items():
            if key in self.monitor_dict:
                last_value = self.monitor_dict[key]
                if value != last_value:
                    updated = True
                    break
                
        self.monitor_dict = monitor_dict
        if updated:
            self.put_timer_event()

    def on_tick(self, tick):
        if not self.trading:
            return

        # 去除时区，避免不必要的麻烦
        tick.datetime = tick.datetime.replace(tzinfo=None)

        # 给分钟Bar生成器推送数据
        if (self.tick_dt and tick.datetime >= self.tick_dt) or not self.tick_dt:
            self.minute_bar_generator.update_tick(tick=tick)

        # 第一个五分钟周期起始，下载数据
        if (not self.window_bar_list) and (not tick.datetime.minute % self.interval_window):
            self.portfolio.download_initing()
        
        # 更新tick相关变量
        self.tick = copy(tick)
        self.tick_dt = max(self.tick_dt, tick.datetime) if self.tick_dt else tick.datetime
        self.latest_price = tick.last_price

        # 计算当前回测盈亏比率
        if self.strategy_position_price:
            if self.direction == Direction.LONG:
                direction_value = 1
            else:
                direction_value = -1
            self.strategy_current_pnl_rate = (
                ((tick.last_price / self.strategy_position_price) - 1)
                * 100
                * direction_value
            )
            self.strategy_current_pnl_rate = round_to(
                self.strategy_current_pnl_rate, 0.01
            )
            self.strategy_current_pnl_rate = f"{self.strategy_current_pnl_rate}%"
        
        else:
            self.strategy_current_pnl_rate = ""

        # 计算当前持仓盈亏比率
        if self.position_price:
            if self.direction == Direction.LONG:
                direction_value = 1
            else:
                direction_value = -1
            self.current_pnl_rate = (
                ((tick.last_price / self.position_price) - 1)
                * 100
                * direction_value
            )
            self.current_pnl_rate = round_to(
                self.current_pnl_rate, 0.01
            )
            self.current_pnl_rate = f"{self.current_pnl_rate}%"

        else:
            self.current_pnl_rate = ""

        # 当前策略状态更新至最新时满足交易条件
        if self.strategy_to and self.strategy_to + timedelta(
            minutes=self.interval_window
        ) <= tick.datetime <= self.strategy_to + timedelta(
            minutes=self.interval_window * 3
        ):
            # Tick允许交易
            self.tick_trade_enable = True

            # 有正在执行的开平仓操作，不进行后续判断
            if self.target_volume >= 0:
                return

            # 策略成交价格
            strategy_ma_price = self.strategy_status["ma_price"]
            trade_price = round_to(strategy_ma_price, self.symbol_price_tick)
            if not strategy_ma_price:
                self.raise_error(f"均线价格异常")

            # 邮件通知内容
            email_msg = ""

            if self.pos:
                # ====== 检查平仓 ======
                if not self.position_close_price:
                    self.raise_error(f"平仓价格异常")
                
                # 平仓价格
                target_close_price = self.position_close_price

                # 选择盈利最大化平仓价格
                strategy_reduce_price = self.strategy_status["position_reduce_price"]
                strategy_trending_step = self.strategy_status["trending_step"]
                if self.trending_step == strategy_trending_step:
                    if self.direction == Direction.LONG:
                        target_close_price = max(self.position_close_price, strategy_reduce_price)
                    
                    if self.direction == Direction.SHORT:
                        target_close_price = min(self.position_close_price, strategy_reduce_price)

                # 是否达到目标价位
                if self.direction == Direction.LONG:
                    if (
                        strategy_ma_price >= target_close_price
                        and tick.last_price < trade_price
                        and tick.last_price > trade_price - self.symbol_price_tick * 5
                    ):
                        self.target_volume = 0

                if self.direction == Direction.SHORT:
                    if (
                        strategy_ma_price <= target_close_price
                        and tick.last_price > trade_price
                        and tick.last_price < trade_price + self.symbol_price_tick * 5
                    ):
                        self.target_volume = 0

                if self.target_volume == 0:
                    # 邮件提醒
                    position_value = abs(self.pos) * self.position_price
                    email_msg += f"\n平仓：当前趋势追踪等级{self.trending_step} 持仓价值{position_value}"

                    # 重置趋势追踪等级
                    self.trending_step = 0

                    # 策略组合更新
                    self.portfolio.update_trending_top()

            if self.target_volume < 0:
                # ====== 检查建仓加仓 ======
                strategy_trending_step = self.strategy_status["trending_step"]
                strategy_next_trending_step = self.strategy_status["next_trending_step"]
                strategy_rsi_array = self.strategy_status["rsi_array"]
                strategy_position_increase_price = self.strategy_status[
                    "position_increase_price"
                ]

                if not strategy_position_increase_price:
                    self.raise_error(f"建仓加仓价格异常")

                # 下一实盘趋势追踪等级
                next_trending_step = 0

                # 回测下一趋势追踪等级满足指定条件
                if (
                    self.bottom_step <= strategy_next_trending_step <= self.top_step
                    and strategy_next_trending_step > self.trending_step
                ): 
                    # 策略组合最多只能有一个趋势追踪最高等级
                    trending_top_cross = True
                    if strategy_next_trending_step == self.top_step and self.portfolio.trending_top:
                        trending_top_cross = False

                    if trending_top_cross:
                        # 是否达到目标价位
                        if self.direction == Direction.LONG:
                            # 根据RSI判断是否超卖
                            rsi_cross = False
                            for rsi in strategy_rsi_array:
                                if rsi <= 25:
                                    rsi_cross = True
                                    break

                            if (
                                rsi_cross
                                and strategy_ma_price <= strategy_position_increase_price
                                and tick.last_price > trade_price
                                and tick.last_price < trade_price + self.symbol_price_tick * 5
                            ):
                                next_trending_step = strategy_next_trending_step

                        elif self.direction == Direction.SHORT:
                            # 根据RSI判断是否超买
                            rsi_cross = False
                            for rsi in strategy_rsi_array:
                                if rsi >= 75:
                                    rsi_cross = True
                                    break

                            if (
                                rsi_cross
                                and strategy_ma_price >= strategy_position_increase_price
                                and tick.last_price < trade_price
                                and tick.last_price > trade_price - self.symbol_price_tick * 5
                            ):
                                next_trending_step = strategy_next_trending_step

                        else:
                            self.raise_error("on_tick中发现direction不正确")

                        if next_trending_step:
                            # 邮件提醒
                            email_msg += f"\n加仓【下一趋势等级】：当前{self.trending_step} 即将：{next_trending_step}"

                # 回测当前趋势追踪等级比当前实盘的高
                if not next_trending_step:
                    tick_price_cross = False
                    if (
                        self.bottom_step <= strategy_trending_step <= self.top_step
                        and strategy_trending_step > self.trending_step
                    ):
                        # 策略组合最多只能有一个趋势追踪最高等级
                        trending_top_cross = True
                        if strategy_trending_step == self.top_step and self.portfolio.trending_top:
                            trending_top_cross = False

                        if trending_top_cross:
                            # 判断当前回测持仓盈亏是否满足指定条件
                            strategy_position_price = self.strategy_status["position_price"]
                            tick_price_cross = False
                            if (
                                self.direction == Direction.LONG
                                and tick.last_price < strategy_position_price * (1 - 0.01)
                            ):
                                tick_price_cross = True

                            elif (
                                self.direction == Direction.SHORT
                                and tick.last_price > strategy_position_price * (1 + 0.01)
                            ):
                                tick_price_cross = True

                            if tick_price_cross:
                                next_trending_step = strategy_trending_step

                                # 邮件提醒
                                email_msg += f"\n加仓【当前趋势等级】：当前{self.trending_step} 即将：{next_trending_step}"

                # 回测趋势追踪等级与实盘不匹配，以实盘加仓标准再次判断
                if not next_trending_step:
                    target_trending_step = self.trending_step + 1
                    if strategy_trending_step != self.trending_step and self.bottom_step <= target_trending_step <= self.top_step and self.position_increase_price:
                        # 策略组合最多只能有一个趋势追踪最高等级
                        trending_top_cross = True
                        if target_trending_step == self.top_step and self.portfolio.trending_top:
                            trending_top_cross = False

                        if trending_top_cross:
                            # 是否达到目标价位
                            if self.direction == Direction.LONG:
                                # 根据RSI判断是否超卖
                                rsi_cross = False
                                for rsi in strategy_rsi_array:
                                    if rsi <= 25:
                                        rsi_cross = True
                                        break

                                if (
                                    rsi_cross
                                    and strategy_ma_price <= self.position_increase_price
                                    and tick.last_price > trade_price
                                    and tick.last_price < trade_price + self.symbol_price_tick * 5
                                ):
                                    next_trending_step = target_trending_step

                            elif self.direction == Direction.SHORT:
                                # 根据RSI判断是否超买
                                rsi_cross = False
                                for rsi in strategy_rsi_array:
                                    if rsi >= 75:
                                        rsi_cross = True
                                        break

                                if (
                                    rsi_cross
                                    and strategy_ma_price >= self.position_increase_price
                                    and tick.last_price < trade_price
                                    and tick.last_price > trade_price - self.symbol_price_tick * 5
                                ):
                                    next_trending_step = target_trending_step

                            else:
                                self.raise_error("on_tick中发现direction不正确")

                            if next_trending_step:
                                # 邮件提醒
                                email_msg = f"\n加仓【实盘下一趋势等级】：当前{self.trending_step} 即将：{next_trending_step}"

                if next_trending_step:
                    # 当前持仓价值
                    current_position_value = abs(self.pos) * self.position_price

                    # 加仓的数量
                    changed_volume = 0

                    if next_trending_step == self.bottom_step:
                        # 初始建仓
                        target_value = (
                            self.portfolio.portfolioValue * self.init_value_rate
                        )
                        changed_volume = (
                            target_value - current_position_value
                        ) / trade_price
                        changed_volume = round_to(
                            changed_volume, self.symbol_min_volume
                        )

                    else:
                        # 加仓
                        if self.pos:
                            # 目标持仓价格
                            price_rate = 0.01
                            if self.direction == Direction.LONG:
                                target_positon_price = trade_price * (1 + price_rate)

                            elif self.direction == Direction.SHORT:
                                target_positon_price = trade_price * (1 - price_rate)

                            else:
                                self.raise_error("on_tick中发现direction不正确")

                            changed_volume1 = (
                                abs(self.pos) * target_positon_price
                                - current_position_value
                            ) / (trade_price - target_positon_price)

                            target_value = (
                                self.portfolio.portfolioValue * self.init_value_rate
                            ) * (10 ** (next_trending_step - self.bottom_step))
                            changed_volume2 = (
                                target_value - current_position_value
                            ) / trade_price

                            # 加仓数量选择最优
                            changed_volume = max(changed_volume1, changed_volume2)
                            changed_volume = round_to(
                                changed_volume, self.symbol_min_volume
                            )

                        else:
                            target_value = (
                                self.portfolio.portfolioValue * self.init_value_rate
                            ) * (10 ** (next_trending_step - self.bottom_step))
                            changed_volume = target_value / trade_price
                            changed_volume = round_to(
                                changed_volume, self.symbol_min_volume
                            )

                    # 加仓后的目标持仓数量
                    self.target_volume = (
                        changed_volume + abs(self.pos) if changed_volume > 0 else -1
                    )

                    # 邮件提醒
                    email_msg += f"\n持仓变化：{changed_volume} 目标持仓：{self.target_volume}"

                    # 确定趋势追踪等级
                    self.trending_step = next_trending_step

                    # 策略组合更新
                    self.portfolio.update_trending_top()

            # 邮件通知
            if email_msg:
                self.send_email(content=email_msg)

            # 有正在执行的开平仓操作，立即发出订单
            if self.target_volume >= 0:
                self.check_order()
        else:
            # Tick不允许交易
            self.tick_trade_enable = False

    def on_bar(self, bar):
        """基于实时Tick数据生成的周期Bar数据推送"""
        # 保存到列表
        self.window_bar_list.append(bar)
        if len(self.window_bar_list) > 20:
            self.window_bar_list.pop(0)

        # 回测数据
        self.start_backtesting()

    def check_order(self):
        """根据目标仓位发出订单"""
        if not self.trading or not self.tick:
            return
        
        # 先取消现有的活动订单
        self.cancel_all()

        # 发出订单
        if self.target_volume >= 0:
            # 建仓加仓
            changed_volume = self.target_volume - abs(self.pos)
            changed_volume = round_to(changed_volume, self.symbol_min_volume)
            if self.direction == Direction.LONG:
                if changed_volume > 0:
                    # 加仓
                    super().buy(
                        self.tick.last_price + self.symbol_price_tick * 20,
                        abs(changed_volume),
                    )

                elif changed_volume < 0:
                    # 平仓
                    super().sell(
                        self.tick.last_price - self.symbol_price_tick * 20,
                        abs(changed_volume),
                    )

            elif self.direction == Direction.SHORT:
                if changed_volume > 0:
                    # 加仓
                    super().short(
                        self.tick.last_price - self.symbol_price_tick * 20,
                        abs(changed_volume),
                    )

                elif changed_volume < 0:
                    # 平仓
                    super().cover(
                        self.tick.last_price + self.symbol_price_tick * 20,
                        abs(changed_volume),
                    )

    def on_order(self, order):
        """委托推送"""
        pass

    def on_trade(self, trade):
        """成交推送"""
        # 持仓精度自动修正
        self.pos = round_to(self.pos, self.symbol_min_volume)

        # 检查目标持仓是否执行完成
        sub = abs(abs(self.pos) - self.target_volume)
        if sub < self.symbol_min_volume:
            self.target_volume = -1

        if self.pos:
            trade_price = trade.price
            trade_volume = trade.volume
            is_open = False
            if self.direction == Direction.LONG:
                if trade.direction == Direction.LONG:
                    # 加仓后的持仓价值
                    self.position_value += trade_price * trade_volume
                    is_open = True

                else:
                    # 平仓后的持仓价值
                    self.position_value = self.position_price * abs(self.pos)

            elif self.direction == Direction.SHORT:
                if trade.direction == Direction.SHORT:
                    # 加仓后的持仓价值
                    self.position_value += trade_price * trade_volume
                    is_open = True

                else:
                    # 平仓后的持仓价值
                    self.position_value = self.position_price * abs(self.pos)

            if is_open:
                # 持仓均价
                self.position_price = self.position_value / abs(self.pos)

                # 平仓、加仓价格
                if self.direction == Direction.LONG:
                    self.position_close_price = self.position_price * (1 + 0.01)
                    self.position_increase_price = self.position_price * (1 - 0.08)

                elif self.direction == Direction.SHORT:
                    self.position_close_price = self.position_price * (1 - 0.01)
                    self.position_increase_price = self.position_price * (1 + 0.08)

        else:
            # 重置持仓价值、持仓均价、平仓价格
            self.position_value = 0
            self.position_price = 0
            self.position_close_price = 0
            self.position_increase_price = 0

        # 邮件提醒
        super(MartingStrategy, self).on_trade(trade)

    def on_stop(self):
        self.write_log(f"{self.strategy_name}\t策略停止")

    def raise_error(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\t{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"马丁策略{self.strategy_name}")

class MartingBacktesting(object):
    def __init__(
        self,
        strategy: MartingStrategy,
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
        self.strategy = strategy  # 实盘策略
        self.vt_symbol = vt_symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.rsi_window = rsi_window  # RSI参数
        self.symbol_min_volume = symbol_min_volume  # 合约最小交易数量
        self.symbol_price_tick = symbol_price_tick  # 合约最小价格变动
        self.init_status = init_status  # 回测初始状态
        self.start_dt = start_dt  # 回测开始时间
        if not self.symbol_min_volume or not self.symbol_price_tick:
            self.strategy.raise_error("马丁回测symbol_min_volume和symbol_price_tick缺失")

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
        self.trending_step = 0  # 趋势追踪等级
        self.next_trending_step = 0  # 下一个趋势追踪等级
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
            "next_trending_step",
        ]

        # 初始化状态
        for name, value in self.init_status.items():
            self.__setattr__(name, value)

    def on_bar(self, bar):
        if not bar.check_valid():
            self.strategy.raise_error("Bar数据校验不通过！！")
        self.bar = bar
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # 检查是否可以开始回测
        if not self.start:
            if not self.start_dt:
                self.strategy.raise_error("回测有初始状态，但没有开始时间！")

            if bar.datetime < self.start_dt:
                # 未达到开始时间
                return

            elif bar.datetime == self.start_dt:
                # 开始回测
                self.start = True

            else:
                # 开始回测时间的Bar数据缺失
                self.strategy.raise_error("开始回测时间的Bar数据缺失！")

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
            self.strategy.raise_error("马丁回测direction不正确")

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
                self.strategy.raise_error("马丁回测direction不正确")

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
                        self.strategy.raise_error("马丁回测direction不正确")

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
                        self.strategy.raise_error("马丁回测direction不正确")

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
                        self.strategy.raise_error("马丁回测direction不正确")

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
