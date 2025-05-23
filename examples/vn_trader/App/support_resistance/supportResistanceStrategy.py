# encoding: UTF-8

"""
跟单策略
"""

from vnpy.trader.constant import Direction, Offset
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.utility import ArrayManager
from vnpy.app.cta_strategy.base import *
from datetime import datetime, timedelta
from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, TickData, PositionData
from vnpy.trader.utility import round_to, floor_to, ceil_to, load_json_path
import numpy as np
from threading import Thread
from utilities.BarGenerator import BarGenerator
from vnpy.event import Event
from copy import copy
import os
from pathlib import Path
from vnpy.trader.constant import Exchange
from vnpy.trader.object import SubscribeRequest
import time
from time import sleep
from enum import Enum
from decimal import Decimal
from queue import Empty, Queue
from vnpy.trader.utility import DIR_SYMBOL
import pandas as pd
from pymongo import MongoClient
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME

class SupportResistanceStrategy(CtaTemplate):
    """ 压力支撑策略 """

    className = "SupportResistanceStrategy"
    author = "loe"

    # 参数列表，保存了参数的名称
    parameters = [
        "strategy_name",
        "vt_symbol",
        "exchange",
        "exchange_user",
        "direction",
        "up_price",
        "down_price",
        "completed"
    ]

    # 变量列表，保存了变量的名称
    variables = [
        "entry",
        "target_pos",
        "open_value",
        "open_price",
        "high_price",
        "low_price",
        "profit_half"
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
        "entry",
        "target_pos",
        "open_value",
        "open_price",
        "high_price",
        "low_price",
        "profit_half"
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.portfolio # 投资组合管理
        self.exchange: Exchange = Exchange.NONE
        self.exchange_user:str = ""
        self.direction: Direction = Direction.NET
        self.up_price: float = 0
        self.down_price: float = 0
        self.completed: bool = False
        
        # 完成setting.json参数的配置
        super(SupportResistanceStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

        # 交易所配置判断
        if self.exchange == "OKX":
            self.exchange = Exchange.OKX
        
        elif self.exchange == "BINANCE":
            self.exchange = Exchange.BINANCE
        
        elif self.exchange == "BYBIT":
            self.exchange = Exchange.BYBIT

        else:
            raise(f"交易所配置错误：{self.exchange}")

        # 交易方向配置判断
        if self.direction == "LONG":
            self.direction = Direction.LONG
        
        elif self.direction == "SHORT":
            self.direction = Direction.SHORT

        else:
            raise(f"交易方向配置错误：{self.direction}")
        
        # 是否执行完成
        if self.completed == 0:
            self.completed = False

        else:
            self.completed = True

        self.entry = False                          # 已开仓
        self.target_pos = 0                         # 目标持仓
        self.target_pos_check_ts = 0                # 仓位检查时间戳
        self.open_value = 0                         # 持仓价值
        self.open_price = 0                         # 持仓均价
        self.high_price = 0                         # 持仓后最高价
        self.low_price = 0                          # 持仓后最低价
        self.profit_half = False                    # 目标盈利过半

    def on_init(self):
        # 交易所成功连接判断
        gateway = self.cta_engine.main_engine.get_gateway(gateway_name=self.exchange.value, account_name=self.exchange_user)
        if not gateway:
            raise(f"支撑压力策略交易所未连接：{self.exchange}@{self.exchange_user}")

    def on_tick(self, tick: TickData):
        if not self.trading or self.completed:
            return
        
        self.tick = copy(tick)
        target_pos_updated = False
        if not self.entry:
            self.entry = True
            
            # 开仓
            if self.down_price < tick.last_price < self.up_price:
                self.target_pos = self.portfolio.portfolioValue / tick.last_price
                target_pos_updated = True

        # 平仓
        if tick.last_price >= self.up_price or tick.last_price <= self.down_price:
            self.target_pos = 0
            target_pos_updated = True

        if self.open_price:
            if self.direction == Direction.LONG:
                # 多头开仓后最高价
                self.high_price = max(self.high_price, tick.last_price)

                # 多头盈利目标过半
                if self.tick.last_price >= self.open_price + abs(self.up_price - self.open_price) * 0.5:
                    self.profit_half = True

                # 多头过半止盈
                if self.profit_half and tick.last_price <= self.high_price - abs(self.high_price - self.open_price) * 0.5:
                    self.target_pos = 0
                    target_pos_updated = True

            elif self.direction == Direction.SHORT:
                # 空头开仓后最低价
                self.low_price = min(self.low_price, tick.last_price) if self.low_price else tick.last_price
            
                # 空头盈利目标过半
                if self.tick.last_price <= self.open_price - abs(self.open_price - self.down_price) * 0.5:
                    self.profit_half = True

                # 空头过半止盈
                if self.profit_half and tick.last_price >= self.low_price + abs(self.open_price - self.low_price) * 0.5:
                    self.target_pos = 0
                    target_pos_updated = True

        if target_pos_updated:
            self.target_pos_check_ts = time.time() - 10
            if not self.target_pos_checking:
                self.target_pos_checking = True
                Thread(target=self.check_target_pos).start()
        
    def check_target_pos(self):
        self.target_pos_checking = True
        cancel_ts = 0
        while True:
            try:
                if self.tick and self.target_pos != self.pos and time.time() >= self.target_pos_check_ts + 3:
                    self.target_pos_check_ts = time.time()

                    # 撮合交易
                    if self.direction == "LONG":
                        if self.target_pos < 0 or self.pos < 0:
                            msg = f"仓位异常\n\n合约 {self.vt_symbol}\n方向 {self.direction.value}\n目标 {self.target_pos}\n当前 {self.pos}"
                            self.send_ding_talk(msg)
                            break

                        gap = self.target_pos - self.pos
                        if gap > 0:
                            # 多头开仓
                            trade_price = self.tick.last_price * 1.005
                            self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(gap))
                        
                        elif gap < 0:
                            # 多头平仓
                            trade_price = self.tick.last_price * 0.995
                            self.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(gap))

                    if self.direction == "SHORT":
                        if self.target_pos > 0 or self.pos > 0:
                            msg = f"仓位异常\n\n合约 {self.vt_symbol}\n方向 {self.direction.value}\n目标 {self.target_pos}\n当前 {self.pos}"
                            self.send_ding_talk(msg)
                            break

                        gap = abs(self.target_pos) - abs(self.pos)
                        if gap > 0:
                            # 空头开仓
                            trade_price = self.tick.last_price * 0.995
                            self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(gap))
                        
                        elif gap < 0:
                            # 空头平仓
                            trade_price = self.tick.last_price * 1.005
                            self.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(gap))

                elif self.target_pos == self.pos and time.time() >= self.target_pos_check_ts + 3:
                    if time.time() >= cancel_ts + 3:
                        cancel_ts = time.time()
                        self.cancel_all()

                    if time.time() >= self.target_pos_check_ts + 60:
                        break

            except Exception as e:
                msg = f"核查目标仓位出错\n\n合约 {self.vt_symbol}\n方向 {self.direction.value}\n目标 {self.target_pos}\n当前 {self.pos}\n{e}"
                self.send_ding_talk(msg)
                break
        
        self.target_pos_checking = False

    def send_order(self, direction, offset, price, volume):
        # 撤回历史订单
        self.cancel_all()

        # 精度处理
        contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)
        if not price or not volume:
            return

        # 币安开仓有最低价值限制，判断是否满足
        if offset == Offset.OPEN and self.exchange == Exchange.BINANCE:
            oms_engine = self.cta_engine.main_engine.engines["oms"]
            tick = oms_engine.ticks.get(self.vt_symbol, None)
            if tick:
                value_cross = True
                order_value = tick.last_price * volume
                if "BTC" in self.vt_symbol and order_value <= 100:
                    value_cross = False

                if "ETH" in self.vt_symbol and order_value <= 20:
                    value_cross = False
                
                if "BCH" in self.vt_symbol and order_value <= 20:
                    value_cross = False

                if "ETC" in self.vt_symbol and order_value <= 20:
                    value_cross = False

                if "LINK" in self.vt_symbol and order_value <= 20:
                    value_cross = False

                if "LTC" in self.vt_symbol and order_value <= 20:
                    value_cross = False

                if order_value <= 5:
                    value_cross = False
                
                if not value_cross:
                    self.send_ding_talk(f"开仓订单价值未满足要求\n合约：{self.vt_symbol}\n价格：{tick.last_price}\n数量：{volume}\n价值：{order_value}")
                    return
        
        # 平仓订单数量处理
        # if offset != Offset.OPEN:
        #     volume = min(volume, abs(self.pos))
        
        # 发出订单
        super().send_order(direction, offset, price, volume)

    def on_trade(self, trade):
        try:
            # 持仓精度自动修正
            contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
            if contract:
                self.pos = round_to(self.pos, contract.min_volume)

            trade_price = trade.price
            trade_volume = trade.volume
            if trade.offset == Offset.OPEN:
                # 开仓价值
                self.open_value += trade_price * trade_volume

                # 开仓均价
                self.open_price = self.open_value / abs(self.pos)

            else:
                # 开仓价值
                self.open_value = self.open_price * abs(self.pos)

            if not self.pos:
                # 重置
                self.open_value = 0
                self.open_price = 0
        
        except Exception as e:
            msg = f"成交处理出错\n\n{e}"
            self.send_ding_talk(msg)
        
        # 邮件提醒
        super().on_trade(trade)

        # 同步数据
        self.put_timer_event()

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"{self.strategy_name}")
