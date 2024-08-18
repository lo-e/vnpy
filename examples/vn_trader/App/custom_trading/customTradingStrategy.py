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

class CustomTradingStrategy(CtaTemplate):
    """ 自主交易策略 """

    className = "CustomTradingStrategy"
    author = "loe"

    # 参数列表，保存了参数的名称
    parameters = [
        "strategy_name",
        "vt_symbol",
        "exchange",
        "exchange_user",
        "direction",
        "long_window",
        "short_window",
        "max_loss_count",
        "profit_rate",
        "loss_rate",
        "stop_profit_price",
        "stop_price_up",
        "stop_price_down"
    ]

    # 变量列表，保存了变量的名称
    variables = [
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.copytradePortfolio # 投资组合管理
        # self.exchange = Exchange.NONE # 交易所
        # self.exchange_user = "" # 交易所用户名

        # 完成setting.json参数的配置
        super(CustomTradingStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

        # 交易所配置判断
        if self.exchange == "OKX":
            self.exchange = Exchange.OKX
        
        elif self.exchange == "BINANCE":
            self.exchange = Exchange.BINANCE
        
        else:
            exit(f"交易所配置错误：{self.exchange}")

        # 交易方向配置判断
        if self.direction == "LONG":
            self.direction = Direction.LONG
        
        elif self.direction == "SHORT":
            self.direction = Direction.SHORT

        else:
            exit(f"交易方向配置错误：{self.direction}")

    def on_init(self):
        # 交易所成功连接判断
        gateway = self.cta_engine.main_engine.get_gateway(gateway_name=self.exchange.value, account_name=self.exchange_user)
        if not gateway:
            exit(f"自主交易策略交易所未连接：{self.exchange}@{self.exchange_user}")

    def on_start(self):
        pass

    def on_tick(self, tick: TickData):
        if not self.trading:
            return

    def send_symbol_order(self, symbol, direction, offset, price, volume, stop=False):
        contract = self.cta_engine.main_engine.get_contract(symbol)
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)
        if not price or not volume:
            return

        # 币安开仓有最低价值限制，判断是否满足
        if offset == Offset.OPEN and self.exchange == Exchange.BINANCE:
            value_cross = True
            order_value = price * volume
            if "BTC" in symbol and order_value <= 100:
                value_cross = False

            if "ETH" in symbol and order_value <= 20:
                value_cross = False
            
            if "BCH" in symbol and order_value <= 20:
                value_cross = False

            if "ETC" in symbol and order_value <= 20:
                value_cross = False

            if "LINK" in symbol and order_value <= 20:
                value_cross = False

            if "LTC" in symbol and order_value <= 20:
                value_cross = False

            if order_value <= 5:
                value_cross = False
            
            if not value_cross:
                self.send_ding_talk(f"开仓订单价值未满足要求\n合约：{symbol}\n价格：{tick.last_price}\n数量：{volume}\n价值：{order_value}")
                return
        
        super().send_symbol_order(symbol, direction, offset, price, volume, stop)

    def on_trade(self, trade):
        super().on_trade(trade)
    
    def on_timer(self):
        self.put_event()
        super().on_timer()

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"{self.strategy_name}")
