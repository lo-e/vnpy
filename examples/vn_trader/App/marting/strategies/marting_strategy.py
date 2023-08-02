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

UNIT_RATE = 0.1 # 初始开仓价值比率
REDUCE_RATE = 0.005 # 盈利平仓比率
CONTINUOUS_INCREASE_RATE = 0.01 # 持续加仓比率
TRENDING_INCREASE_RATE = 0.04 # 趋势加仓比率
TRENDING_OPEN_LOSS_RATE = 0.02 # 趋势加仓时的持仓亏损比率
TOP_STEP = 6

class MartingStrategy(CtaTemplate):
    """马丁策略"""

    className = "MartingStrategy"
    author = "loe"

    # 策略参数
    interval_window = 5  # 数据的时间周期5分钟
    ma_window = 9  # 均线参数
    top_step = TOP_STEP

    # 参数列表，保存了参数的名称
    parameters = [
        "strategy_name",
        "vt_symbol",
        "direction",
    ]

    # 变量列表，保存了变量的名称
    variables = [
        "strategy_name",
        "symbol_price_tick",
        "symbol_min_volume",
        "tag_price",
        "tag_price_dt",
        "ma_price",
        "bar_dt",
        "tick_dt",
        "tick_trade_enable",
        "latest_price",
        "position_value",
        "position_price",
        "position",
        "position_close_price",
        "position_increase_price",
        "current_pnl_rate",
        "trending_step",
        "target_volume",
        "open_waitting",
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
        "position_value",
        "position_price",
        "position",
        "position_close_price",
        "position_increase_price",
        "trending_step",
        "tag_price",
        "tag_price_dt",
    ]

    # 监控列表
    monitors = ["tick_trade_enable"]

    def __init__(self, ctaEngine, martingPortfolio, setting):
        # 组合管理引擎
        self.portfolio = martingPortfolio

        # 策略参数、变量
        self.direction: Direction = Direction.NET  # 交易方向
        self.tick: TickData = None
        self.symbol_min_volume: float = 0.0
        self.symbol_price_tick: float = 0.0
        self.bar: BarData = None  # 最新K线
        self.position_value = 0  # 持仓价值
        self.position_price = 0  # 持仓均价
        self.position = 0 # 虚拟持仓
        self.position_close_price = 0  # 平仓价格
        self.position_increase_price = 0 # 加仓价格
        self.current_pnl_rate = ""  # 当前持仓亏损比率【基于Tick数据实时计算】
        self.trending_step = 0  # 趋势追踪等级
        self.ma_price = 0 # 均线
        self.bar_dt = None # 最新的bar时间
        self.tick_dt = None # 最新的tick时间
        self.tick_trade_enable = False  # Tick数据时间在回测后的指定范围内允许交易
        self.latest_price = 0 # 最新的tick价格
        self.tag_price = 0 # 标记价格
        self.tag_price_dt = None # 标记时间
        self.target_volume = -1  # 目标持仓
        self.open_waitting = False # 实盘交易等待
        self.top_open_immediate = False # 实盘交易高等级加仓
        self.window_bar_list = []  # 基于实时Tick数据生成的周期Bar数据列表
        self.monitor_dict = {} # 最新的同步和监控的变量数据，用于检查是否更新，如更新及时同步数据库和刷新UI
        self.open_email_suspend = False # 加仓超限email发送暂停
        self.is_backtesting = False # 是否正在回测
        self.backtesting_wait = 100 # 回测缓冲时间
        self.strategy_event_wait = 0 # 策略事件缓冲时间

        self.am = ArrayManager(self.ma_window)  # K线容器
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
        if self.direction == "多":
            self.direction = Direction.LONG
            
        elif self.direction == "空":
            self.direction = Direction.SHORT

        # 监听事件
        self.cta_engine.event_engine.register(
            BAR_DOWNLOAD_GENERATE_COMPLETE, self.portfolio_download_generate_complete
        )

    def on_init(self):
        # 回测数据
        self.start_backtesting()
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

        # 载入历史数据
        data_from = datetime.now() - timedelta(days=2)
        backtesting_data = self.cta_engine.load_bar(
            vt_symbol=self.vt_symbol,
            data_from=data_from,
            interval=Interval.MINUTE,
            window=5,
            callback=None,
        )

        # 剔除最后一个Bar数据，保证数据的准确性
        backtesting_data = backtesting_data[0:-1]
        if len(backtesting_data):
            # 添加实时Bar数据
            live_data = []
            last_bar = backtesting_data[-1]
            next_bar_dt = last_bar.datetime + timedelta(minutes=self.interval_window)
            for i in range(len(self.window_bar_list)):
                bar = self.window_bar_list[i]
                if bar.datetime == next_bar_dt:
                    live_data = self.window_bar_list[i:]
                    break
            backtesting_data += live_data

        # 开始数据回测
        self.am = ArrayManager(self.ma_window)
        self.ma_price = 0
        self.bar_dt = None
        for bar in backtesting_data:
            if not bar.check_valid():
                raise ("Bar数据校验不通过！！")
            self.am.update_bar(bar)

            if self.am.inited:
                # 均线价格
                self.ma_price = self.am.sma(self.ma_window)
                self.bar_dt = bar.datetime

                # 标记价格
                if self.tag_price and bar.datetime > self.tag_price_dt:
                    if self.direction == Direction.LONG:
                        self.tag_price = max(self.tag_price, self.ma_price)
                    
                    elif self.direction == Direction.SHORT:
                        self.tag_price = min(self.tag_price, self.ma_price)
                    self.tag_price_dt = bar.datetime

        # 更新指标
        self.calculate_indicator()

        # 结束回测
        self.is_backtesting = False
        self.backtesting_wait = 0
        self.strategy_event_wait = 0
        self.put_timer_event()

    def calculate_indicator(self):
        """计算入场指标"""

        if not self.am.inited:
            return
        
        # 初始化标记价格
        if not self.tag_price:
            self.tag_price = self.ma_price
            self.tag_price_dt = self.bar_dt

        # 减仓价格
        if self.direction == Direction.LONG:
            self.position_close_price = self.position_price * (1 + REDUCE_RATE)

        elif self.direction == Direction.SHORT:
            self.position_close_price = self.position_price * (1 - REDUCE_RATE)

        # 加仓价格
        if self.trending_step + 1 < TOP_STEP:
            if self.direction == Direction.LONG:
                self.position_increase_price = self.tag_price * (1 - CONTINUOUS_INCREASE_RATE)

            elif self.direction == Direction.SHORT:
                self.position_increase_price = self.tag_price * (1 + CONTINUOUS_INCREASE_RATE)
        
        else:
            if self.direction == Direction.LONG:
                self.position_increase_price = self.position_price * (1 - TRENDING_INCREASE_RATE)

            elif self.direction == Direction.SHORT:
                self.position_increase_price = self.position_price * (1 + TRENDING_INCREASE_RATE)

    def on_timer(self):
        # 回测缓冲
        self.backtesting_wait += 1

        # 策略事件缓冲
        self.strategy_event_wait += 1

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

            # 新周期开始，取消未成交的所有订单
            self.cancel_all()
            self.target_volume = -1

        # 策略事件发出判断
        if self.strategy_event_wait >= 5:
            self.strategy_event_wait = 0
            self.put_timer_event()

        else:
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
        if self.bar_dt and self.bar_dt + timedelta(
            minutes=self.interval_window
        ) <= tick.datetime <= self.bar_dt + timedelta(
            minutes=self.interval_window * 3
        ):
            # Tick允许交易
            self.tick_trade_enable = True

            # 生成交易信号
            self.generate_signal(tick=tick)
        else:
            # Tick不允许交易
            self.tick_trade_enable = False
        
        # 信号判断结束后取消立即开仓交易
        self.top_open_immediate = False

    # 生成交易信号
    def generate_signal(self, tick):
        # 有正在执行的开平仓操作，不进行后续判断
        if self.target_volume >= 0:
            return

        # 均线判断
        if not self.ma_price:
            return

        # 邮件通知内容
        email_msg = ""

        # 获取反方向信号
        oppsite_strategy = self.get_oppsite_strategy()
        if oppsite_strategy.pos:
            self.open_waitting = True

        if self.position_close_price:
            """ 检查平仓 """

            # 是否达到目标价位
            if self.direction == Direction.LONG:
                if (
                    self.ma_price >= self.position_close_price
                    and tick.last_price <= self.ma_price
                    and tick.last_price > self.ma_price - self.symbol_price_tick * 5
                ):
                    self.target_volume = 0

            if self.direction == Direction.SHORT:
                if (
                    self.ma_price <= self.position_close_price
                    and tick.last_price >= self.ma_price
                    and tick.last_price < self.ma_price + self.symbol_price_tick * 5
                ):
                    self.target_volume = 0

        next_trending_step = 0
        if self.target_volume < 0 and self.position_increase_price:
            """ 检查加仓 """

            # 普通加仓判断
            if self.trending_step + 1 < TOP_STEP or self.open_waitting:
                if self.direction == Direction.LONG:
                    if (
                        self.ma_price <= self.position_increase_price
                        and tick.last_price >= self.ma_price
                        and tick.last_price < self.ma_price + self.symbol_price_tick * 5
                    ):
                        next_trending_step = self.trending_step + 1

                elif self.direction == Direction.SHORT:
                    if (
                        self.ma_price >= self.position_increase_price
                        and tick.last_price <= self.ma_price
                        and tick.last_price > self.ma_price - self.symbol_price_tick * 5
                    ):
                        next_trending_step = self.trending_step + 1
            
            # 趋势加仓判断
            if (self.trending_step + 1 >= TOP_STEP) and (not self.open_waitting) and (self.top_open_immediate):
                if self.direction == Direction.LONG:
                    if (
                        tick.last_price <= self.position_increase_price
                    ):
                        next_trending_step = self.trending_step + 1

                if self.direction == Direction.SHORT:
                    if (
                        tick.last_price >= self.position_increase_price
                    ):
                        next_trending_step = self.trending_step + 1

            if next_trending_step:

                # 当前持仓价值
                current_position_value = abs(self.position) * self.position_price

                # 加仓的数量
                changed_volume = 0

                if self.trending_step + 1 < TOP_STEP:
                    """ 固定倍数加仓 """

                    # 目标持仓价值
                    target_position_value = current_position_value * 2 if current_position_value else self.portfolio.portfolioValue * UNIT_RATE
                    trade_value = target_position_value - current_position_value
                    trade_value = max(trade_value, tick.last_price * self.symbol_min_volume, 5.1)

                    # 计算加仓的合约数量
                    changed_volume = trade_value / tick.last_price
                    changed_volume = ceil_to(changed_volume, self.symbol_min_volume)

                else:
                    """ 根据持仓价格百分比加仓 """

                    # 目标持仓价格
                    if self.direction == Direction.LONG:
                        target_positon_price = tick.last_price * (1 + TRENDING_OPEN_LOSS_RATE)

                    elif self.direction == Direction.SHORT:
                        target_positon_price = tick.last_price * (1 - TRENDING_OPEN_LOSS_RATE)

                    changed_volume = (
                        abs(self.position) * target_positon_price
                        - current_position_value
                    ) / (tick.last_price - target_positon_price)
                    changed_volume = ceil_to(
                        changed_volume, self.symbol_min_volume
                    )
                
                # 针对币安开仓金额不得低于5U，不满足则不开仓
                changed_volume = changed_volume if changed_volume * tick.last_price > 5 else 0

                # 加仓后的目标持仓数量
                self.target_volume = (
                    changed_volume + abs(self.position) if changed_volume > 0 else -1
                )

        # 有正在执行的开平仓操作，立即发出订单
        if self.target_volume >= 0:
            if self.target_volume > 0:

                """ 组合持仓限制判断 """
                changed_volume = self.target_volume - abs(self.position)
                open_value = changed_volume * tick.last_price
                open_cross = self.portfolio.check_open_cross(strategy=self, open_value=open_value)
                if open_cross:
                    # 变量更新
                    if self.open_waitting:
                        if self.direction == Direction.LONG:
                            self.position = abs(self.position) + changed_volume

                        elif self.direction == Direction.SHORT:
                            self.position = (abs(self.position) + changed_volume) * -1

                        self.position_price = ((changed_volume * tick.last_price) + current_position_value) / abs(self.position)
                        self.position_value = self.position_price * abs(self.position)

                    else:
                        oppsite_strategy.open_waitting = True
                        # 邮件通知
                        email_msg = f"\n马丁策略加仓：\n当前等级{self.trending_step}\n加仓后等级：{next_trending_step}"
                        self.send_email(content=email_msg)

                    self.tag_price = self.ma_price
                    self.tag_price_dt = self.bar_dt
                    self.trending_step = next_trending_step

                    # 更新策略组合
                    self.portfolio.update_trending_top()
                    
                    # 取消加仓邮件暂停
                    self.open_email_suspend = False

                    # 更新指标
                    self.calculate_indicator()

                    # 提交订单
                    self.check_order()

                else:
                    if not self.open_email_suspend:
                        self.open_email_suspend = True
                        email_msg += f"\n\n加仓不通过【组合持仓价值超过限制】\n策略名称：{self.strategy_name}\n策略持仓价值：{self.position_value}\n加仓价值：{open_value}\n组合持仓价值：{self.portfolio.total_strategy_value}"
                        self.send_email(content=email_msg)

                        # 取消正在进行的所有订单
                        self.cancel_all()
                    self.target_volume = -1
            
            else:
                """ 平仓 """
                
                # 变量更新
                if self.open_waitting:
                    oppsite_strategy.top_open_immediate = True
                    self.position = 0
                    self.position_price = 0
                    self.position_value = 0

                else:
                    email_msg += f"\n马丁策略平仓：\n当前趋势追踪等级{self.trending_step}\n平仓价值{position_value}"
                    self.send_email(content=email_msg)

                self.tag_price = self.ma_price
                self.tag_price_dt = self.bar_dt
                self.trending_step = 0
                self.open_waitting = False
                if not oppsite_strategy.trending_step:
                    oppsite_strategy.open_waitting = False

                # 更新策略组合
                self.portfolio.update_trending_top()

                # 更新指标
                self.calculate_indicator()

                # 提交订单
                self.check_order()

    def on_bar(self, bar):
        """基于实时Tick数据生成的周期Bar数据推送"""
        # 保存到列表
        self.window_bar_list.append(bar)
        if len(self.window_bar_list) > 50:
            self.window_bar_list.pop(0)

        # 回测数据
        self.start_backtesting()

    def check_order(self, price:float=0):
        """根据目标仓位发出订单"""
        if not self.trading or not self.tick:
            return
        
        # 先取消现有的活动订单
        self.cancel_all()

        # 发出订单
        if self.target_volume >= 0:
            changed_volume = self.target_volume - abs(self.pos)
            changed_volume = round_to(changed_volume, self.symbol_min_volume)
            if self.direction == Direction.LONG:
                if changed_volume > 0:
                    if not self.open_waitting:
                        # 加仓
                        if not price:
                            price = self.tick.last_price + self.symbol_price_tick * 20

                        super().buy(
                            price,
                            abs(changed_volume),
                        )

                elif changed_volume < 0:
                    # 平仓
                    if not price:
                        price = self.tick.last_price - self.symbol_price_tick * 20

                    super().sell(
                        price,
                        abs(changed_volume),
                    )

            elif self.direction == Direction.SHORT:
                if changed_volume > 0:
                    if not self.open_waitting:
                        # 加仓
                        if not price:
                            price = self.tick.last_price - self.symbol_price_tick * 20

                        super().short(
                            price,
                            abs(changed_volume),
                        )

                elif changed_volume < 0:
                    # 平仓
                    if not price:
                        price = self.tick.last_price + self.symbol_price_tick * 20

                    super().cover(
                        price,
                        abs(changed_volume),
                    )

        # 开仓等待状态下自动完成仓位
        if self.open_waitting:
            self.target_volume = -1

    def on_trade(self, trade):
        """成交推送"""
        # 持仓精度自动修正
        self.pos = round_to(self.pos, self.symbol_min_volume)
        self.position = self.pos

        # 检查目标持仓是否执行完成
        sub = abs(abs(self.pos) - self.target_volume)
        if sub < self.symbol_min_volume:
            self.target_volume = -1

        if self.pos:
            trade_price = trade.price
            trade_volume = trade.volume
            is_open = False
            if self.direction == Direction.LONG and trade.direction == Direction.LONG:
                is_open = True

            elif self.direction == Direction.SHORT and trade.direction == Direction.SHORT:
                is_open = True

            if is_open:
                # 加仓后的持仓价值
                self.position_value += trade_price * trade_volume

                # 持仓均价
                self.position_price = self.position_value / abs(self.pos)
            
            else:
                # 平仓后的持仓价值
                self.position_value = self.position_price * abs(self.pos)

        else:
            # 重置持仓价值、持仓均价
            self.position_value = 0
            self.position_price = 0
        
        # 更新指标
        self.calculate_indicator()

        # 邮件提醒
        super(MartingStrategy, self).on_trade(trade)

    def raise_error(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\t{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"马丁策略{self.strategy_name}")
    
    def get_oppsite_strategy(self):
        oppsite_strategy_name = ""
        if self.direction == Direction.LONG:
            oppsite_strategy_name = self.strategy_name.replace("多", "空")

        elif self.direction == Direction.SHORT:
            oppsite_strategy_name = self.strategy_name.replace("空", "多")

        oppsite_strategy = self.cta_engine.strategies.get(oppsite_strategy_name, None)
        return oppsite_strategy