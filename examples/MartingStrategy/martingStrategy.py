# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange
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

UNIT_RATE = 0.1 # 初始开仓价值比率
REDUCE_RATE = 0.005 # 盈利平仓比率
CONTINUOUS_INCREASE_RATE = 0.01 # 持续加仓比率
TRENDING_INCREASE_RATE = 0.04 # 趋势加仓比率
TRENDING_OPEN_LOSS_RATE = 0.02 # 趋势加仓时的持仓亏损比率
TOP_STEP = 3

class MartingSignal(object):
    def __init__(
        self,
        portfolio,
        symbol,
        direction,
        ma_window,
        history_data: dict = {},
    ):
        # 常量
        self.portfolio = portfolio  # 投资组合
        self.symbol = symbol  # 合约代码
        self.direction = direction  # 交易方向
        self.ma_window = ma_window  # 均线参数
        self.unit_value = self.portfolio.portfolioValue * UNIT_RATE  # 单位持仓价值
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

        if self.direction != Direction.LONG and self.direction != Direction.SHORT:
            exit("检查代码！")

        # 变量
        self.start = (
            False if self.init_status else True
        )  # 开始回测开关，当有初始状态时，回测Bar数据需要从start_dt开始
        self.bar: BarData = None  # 最新K线
        self.am = ArrayManager(self.ma_window)  # K线容器
        self.position = 0  # 持仓量
        self.position_price = 0  # 持仓均价
        self.position_reduce_price = 0  # 减仓价格
        self.position_increase_price = 0  # 加仓价格
        self.tag_price = 0 # 标记价格，根据此价格计算下一次加仓价格
        self.max_loss_value = 0  # 当前持仓最大亏损价值
        self.max_loss_rate = ""  # 当前持仓最大亏损比率
        self.ma_price = 0  # 均线价格
        self.trending_step = 0  # 追踪趋势的等级
        self.open_waitting = False # 等待正在交易的反方向信号平仓才能开仓，且只能从初始仓位开始
        self.top_open_price = 0 # 趋势加仓价格
        self.current_trending_group = []

        # 初始化状态
        for name, value in self.init_status.items():
            self.__setattr__(name, value)

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
            "current_trending_group",
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
        if "DOT" in self.symbol and self.direction == Direction.LONG and bar.datetime >= datetime.strptime("2023-02-22 23:40:00", "%Y-%m-%d %H:%M:%S"):
            a = 2

        # 获取反方向信号
        oppsite_signal = self.portfolio.get_oppsite_signal(self)

        # 检查减仓
        if self.position_reduce_price:
            # 成交价格
            trade_price = round_to(self.ma_price, self.symbol_price_tick)

            # 是否达到目标价位
            reduce_price_cross = False
            if self.direction == Direction.LONG:
                if (
                    self.ma_price >= self.position_reduce_price
                    and bar.low_price <= trade_price
                    and bar.high_price >= trade_price
                ):
                    reduce_price_cross = True

            if self.direction == Direction.SHORT:
                if (
                    self.ma_price <= self.position_reduce_price
                    and bar.high_price >= trade_price
                    and bar.low_price <= trade_price
                ):
                    reduce_price_cross = True

            if reduce_price_cross:
                """ 满足减仓条件 """

                # 在变量更新前进行组合策略更新，已获取仓位变更前的状态数据
                self.portfolio.update_trending(self, False)

                # 组合策略检查最高等级
                self.portfolio.check_top_step(self, False)

                # 成交数量
                trade_volume = abs(self.position)

                # 变量更新
                self.position = 0
                self.position_price = 0
                self.tag_price = trade_price
                self.trending_step = 0
                self.current_trending_group = []

                if not self.open_waitting:
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

                # 取消开仓等待
                self.open_waitting = False

                # 更新持仓最大亏损
                self.max_loss_value = 0
                self.max_loss_rate = ""

                # 减仓操作后停止后续加仓判断
                return

        # 检查加仓
        if self.position_increase_price:
            # 判断开仓等待
            oppsite_signal_key = f"{oppsite_signal.symbol}_{oppsite_signal.direction.value}"
            oppsite_signal_pos = self.portfolio.signalPosDict.get(oppsite_signal_key, 0)
            if abs(oppsite_signal_pos) > 0:
                self.open_waitting = True

            # 成交价格
            trade_price = round_to(self.ma_price, self.symbol_price_tick)

            # 是否达到目标价位
            increase_price_cross = False

            if self.trending_step + 1 < TOP_STEP or self.open_waitting:
                if self.direction == Direction.LONG:
                    if (
                        self.ma_price <= self.position_increase_price
                        and bar.high_price >= trade_price
                        and bar.low_price <= trade_price
                    ):
                        increase_price_cross = True

                if self.direction == Direction.SHORT:
                    if (
                        self.ma_price >= self.position_increase_price
                        and bar.low_price <= trade_price
                        and bar.high_price >= trade_price
                    ):
                        increase_price_cross = True

            # 趋势加仓判断
            if (self.trending_step + 1 >= TOP_STEP) and (not self.open_waitting) and (self.top_open_price):
                if self.direction == Direction.LONG:
                    if (
                        self.top_open_price <= self.position_increase_price
                    ):
                        increase_price_cross = True
                        trade_price = self.top_open_price

                if self.direction == Direction.SHORT:
                    if (
                        self.top_open_price >= self.position_increase_price
                    ):
                        increase_price_cross = True
                        trade_price = self.top_open_price

            if increase_price_cross:
                """ 满足加仓条件 """

                # 加仓的合约数量
                trade_volume = 0

                # 当前持仓价值、目标持仓价值
                current_position_value = abs(self.position) * self.position_price
                
                if self.trending_step + 1 < TOP_STEP:
                    """ 固定倍数加仓 """

                    # 目标持仓价值
                    target_position_value = current_position_value * 2 if current_position_value else self.unit_value

                    # 计算加仓的合约数量
                    trade_volume = ((target_position_value - current_position_value)) / trade_price
                    trade_volume = round_to(trade_volume, self.symbol_min_volume)

                    # 加仓数量检查
                    if trade_volume <= 0:
                        exit("加仓数量错误，检查代码！")

                else:
                    """ 根据持仓价格百分比加仓 """

                    # 计算加仓数量
                    if self.direction == Direction.LONG:
                        target_positon_price = trade_price * (1 + TRENDING_OPEN_LOSS_RATE)

                    elif self.direction == Direction.SHORT:
                        target_positon_price = trade_price * (1 - TRENDING_OPEN_LOSS_RATE)

                    trade_volume = (
                        abs(self.position) * target_positon_price
                        - current_position_value
                    ) / (trade_price - target_positon_price)
                    trade_volume = round_to(trade_volume, self.symbol_min_volume)
                    
                    # 加仓数量检查
                    if trade_volume <= 0:
                        exit("加仓数量错误，检查代码！")

                    # 控制组合总持仓价值
                    if not self.open_waitting:
                        # 目标持仓价值
                        target_position_value = (abs(self.position) + trade_volume) * target_positon_price
                        if target_position_value >= self.portfolio.portfolioValue:
                            # 检查最高等级
                            top_cross = self.portfolio.check_top_step(self, True)
                            if not top_cross:
                                trade_volume = 0

                if trade_volume > 0:
                    # 在变量更新前进行组合策略更新，已获取仓位变更前的状态数据
                    self.portfolio.update_trending(self, True)

                    # 变量更新
                    if self.direction == Direction.LONG:
                        self.position = abs(self.position) + trade_volume

                    elif self.direction == Direction.SHORT:
                        self.position = (abs(self.position) + trade_volume) * -1

                    self.position_price = ((trade_volume * trade_price) + current_position_value) / abs(self.position)
                    self.tag_price = trade_price
                    self.trending_step += 1
                    self.current_trending_group.append({"datetime":bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                                        "trending_step":self.trending_step,
                                                        "max_loss_value":self.max_loss_value,
                                                        "max_loss_rate":self.max_loss_rate})
                    
                    if not self.open_waitting:
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
            self.position_reduce_price = self.position_price * (1 + REDUCE_RATE)

        elif self.direction == Direction.SHORT:
            self.position_reduce_price = self.position_price * (1 - REDUCE_RATE)

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

        # 趋势加仓价格初始化
        self.top_open_price = 0
        
    def save_sync_data(self):
        status = {}
        for name in self.syncs:
            status[name] = self.__getattribute__(name)
        self.saved_sync_data = {
            "backtesting_status": status,
            "backtesting_to": self.bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
        }


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
        self.top_step_signal = None
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
                self, symbol, Direction.LONG, 9, history_data=long_history_data
            )

            short_signal_key = f"{signal_key}_{Direction.SHORT.value}"
            short_history_data = history_data.get(short_signal_key, {})
            signal2 = MartingSignal(
                self, symbol, Direction.SHORT, 9, history_data=short_history_data
            )

            l = self.signalDict[symbol]
            l.append(signal1)
            l.append(signal2)

            # # 根据历史回测数据给策略组合持仓初始化
            # long_signal_position = 0
            # if long_history_data:
            #     long_signal_position = long_history_data["backtesting_status"][
            #         "position"
            #     ]
            #     long_signal_position_key = f"{symbol}_{Direction.LONG.value}"
            #     self.signalPosDict[long_signal_position_key] = long_signal_position

            # short_signal_position = 0
            # if short_history_data:
            #     short_signal_position = short_history_data["backtesting_status"][
            #         "position"
            #     ]
            #     short_signal_position_key = f"{symbol}_{Direction.SHORT.value}"
            #     self.signalPosDict[short_signal_position_key] = short_signal_position

            # self.posDict[symbol] = long_signal_position + short_signal_position

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
            position_value = abs(round_to(abs(signal.position) * signal.position_price, 1))

            # 平仓盈亏
            close_pnl = 0
            if not trending:
                direction_v = 1 if signal.direction == Direction.LONG else -1
                close_pnl = ((trade_price / last_position_price) - 1) * 100 * direction_v
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
