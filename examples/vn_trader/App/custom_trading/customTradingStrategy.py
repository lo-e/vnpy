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
        "virtual_pos",
        "direction",
        "indicator_inited",
        "bar_loading",
        "bar_lack",
        "bar_loaded_dt",
        "long_up",
        "long_down",
        "short_up",
        "short_down",
        "pos_open_price",
        "stop_loss_price",
        "profit_stop",
        "open_stop",
        "loss_count",
        "max_loss_count"
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.customTradingPortfolio # 投资组合管理
        
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

        self.bar_loading = False                                                                            # 正在加载bar数据
        self.bar_lack = True                                                                                # bar缺失
        self.bar_loaded_dt = None                                                                           # 已完成导入的bar截止时间

        self.virtual_pos = 0                                                                                # 虚拟持仓
        self.pos_open_price = 0                                                                             # 开仓价格
        self.stop_loss_price = 0                                                                            # 持仓止损价格
        
        self.long_up = 0                                                                                    # 止损最高价
        self.long_down = 0                                                                                  # 止损最低价
        self.short_up = 0                                                                                   # 开仓向上突破价
        self.short_down = 0                                                                                 # 开仓向下突破价

        self.indicator_inited = False                                                                       # 指标初始化状态
        self.profit_stop = False                                                                            # 止盈状态
        self.open_stop = False                                                                              # 停止开新的仓位
        self.loss_count = 0                                                                                 # 止损次数
        self.bar = None                                                                                     # 当前最新bar
        self.am = ArrayManager(self.long_window)                                                            # K线容器
        self.bar_generator = BarGenerator(on_bar=None, window=5, on_window_bar=self.on_window_bar)          # bar生成工具

    def on_init(self):
        # 交易所成功连接判断
        gateway = self.cta_engine.main_engine.get_gateway(gateway_name=self.exchange.value, account_name=self.exchange_user)
        # if not gateway:
            # exit(f"自主交易策略交易所未连接：{self.exchange}@{self.exchange_user}")

    def on_start(self):
        pass

    def load_bar_data(self):
        # 起始bar时间
        data_from = (datetime.now() - timedelta(minutes=self.long_window * 5 * 2)).replace(second=0, microsecond=0)

        # 最后bar时间
        data_to = datetime.now().replace(second=0, microsecond=0)
        while (data_to.minute + 1) % 5:
            data_to = data_to + timedelta(minutes=1)
        data_to = data_to - timedelta(minutes=5)
        if data_to == self.bar_loaded_dt:
            # 该周期bar数据已经导入完成
            return

        # 重新初始化bar工具
        self.am = ArrayManager(self.long_window)
        self.bar_generator = BarGenerator(on_bar=None, window=5, on_window_bar=self.on_window_bar) 

        # 更新状态（正在加载bar数据）
        self.bar_loading = True

        # 数据库加载bar数据
        mc = MongoClient()
        db = mc[MINUTE_DB_NAME]
        collection = db[self.vt_symbol]
        flt = {'datetime':{'$gte':data_from,
                           '$lte':data_to}} 
        cursor = collection.find(flt).sort('datetime')
        next_bar_dt = data_from
        for d in cursor:
            exchange = Exchange.NONE
            bar = BarData(gateway_name = '', symbol = '', exchange = exchange, datetime = None, endDatetime = None)
            bar.__dict__ = d

            if bar.datetime != next_bar_dt:
                # bar数据缺失
                self.bar_lack = True
                self.bar_loading = False
                return
            
            next_bar_dt = bar.datetime + timedelta(minutes=1)
            self.on_bar(bar)

        if next_bar_dt - timedelta(minutes=1) != data_to:
            # bar数据缺失
            self.bar_lack = True
            self.bar_loading = False
            return

        # 更新状态（bar数据加载完毕）
        self.bar_lack = False
        self.bar_loaded_dt = data_to
        self.bar_loading = False

        # 计算指标
        self.calculate_indicator()

    def on_bar(self, bar):
        self.bar_generator.update_bar(bar)

    def on_window_bar(self, bar):
        self.am.update_bar(bar)
        self.bar = bar

    def calculate_indicator(self):
        if not self.am.inited:
            return
        
        # 止损价格
        self.long_up, self.long_down = self.am.donchian(self.long_window)
        if not self.indicator_inited:
            if self.direction == Direction.LONG:
                if self.long_down == self.bar.low_price:
                    self.indicator_inited = True
            
            else:
                if self.long_up == self.bar.high_price:
                    self.indicator_inited = True

        # 突破开仓价格
        self.short_up, self.short_down = self.am.donchian(self.short_window)

    def on_tick(self, tick: TickData):
        if not self.trading or self.profit_stop or self.loss_count >= self.max_loss_count:
            return
        
        # 判断是否价格突破开仓上限/下限，然后停止开新仓位
        if tick.last_price >= self.stop_price_up or tick.last_price <= self.stop_price_down:
            self.open_stop = True
        
        if not self.virtual_pos:
            # 停止开新的仓位判断
            if self.open_stop or self.bar_loading or self.bar_lack or not self.indicator_inited:
                return
            
            if self.direction == Direction.LONG:
                if tick.last_price >= self.short_up:
                    # 多头开仓
                    pos_open_price = tick.last_price
                    trade_price = tick.last_price * 1.005
                    lever = self.loss_rate / abs(((self.long_down / pos_open_price) - 1))
                    if ((self.stop_profit_price / pos_open_price) - 1) * lever >= self.profit_rate:
                        value = self.portfolio.portfolioValue * lever
                        volume = value / pos_open_price
                        self.send_order(Direction.LONG, Offset.OPEN, trade_price, volume)
                        self.pos_open_price = pos_open_price
                        self.stop_loss_price = self.long_down
                        return
            
            else:
                if tick.last_price <= self.short_down:
                    # 空头开仓
                    pos_open_price = tick.last_price
                    trade_price = tick.last_price * 0.995
                    lever = self.loss_rate / abs(((self.long_up / pos_open_price) - 1))
                    if (1 - (self.stop_profit_price / pos_open_price)) * lever >= self.profit_rate:
                        value = self.portfolio.portfolioValue * lever
                        volume = value / pos_open_price
                        self.send_order(Direction.SHORT, Offset.OPEN, trade_price, volume)
                        self.pos_open_price = pos_open_price
                        self.stop_loss_price = self.long_up
                        return

        else:
            if self.direction == Direction.LONG:
                if tick.last_price >= self.stop_profit_price:
                    # 多头止盈
                    trade_price = tick.last_price * 0.995
                    self.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(self.virtual_pos))
                    self.pos_open_price = 0
                    self.stop_loss_price = 0
                    self.profit_stop = True
                    return
                
                if tick.last_price <= self.stop_loss_price:
                    # 多头止损
                    trade_price = tick.last_price * 0.995
                    self.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(self.virtual_pos))
                    self.pos_open_price = 0
                    self.stop_loss_price = 0
                    self.loss_count += 1
                    return
                
                if tick.last_price >= self.short_up:
                    # 停止加仓判断
                    if self.bar_loading or self.bar_lack:
                        return
            
                    # 多头加仓
                    if ((self.long_down / self.pos_open_price) - 1) >= abs((self.stop_loss_price / self.pos_open_price) - 1):
                        pos_open_price = tick.last_price
                        trade_price = tick.last_price * 1.005
                        if 1 / abs(((self.long_down / pos_open_price) - 1)) >= 1.5 / abs(((self.stop_loss_price / self.pos_open_price) - 1)):
                            # 满足多头加仓条件
                            value = self.portfolio.portfolioValue * self.loss_rate / abs(((self.long_down / pos_open_price) - 1))
                            volume = value / pos_open_price
                            add_volume = volume - abs(self.pos)
                            if add_volume > 0:
                                self.send_order(Direction.LONG, Offset.OPEN, trade_price, add_volume)
                                self.pos_open_price = pos_open_price
                                self.stop_loss_price = self.long_down
                                self.max_loss_count += 1
                                return

            else:
                if tick.last_price <= self.stop_profit_price:
                    # 空头止盈
                    trade_price = tick.last_price * 1.005
                    self.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(self.virtual_pos))
                    self.pos_open_price = 0
                    self.stop_loss_price = 0
                    self.profit_stop = True
                    return
                
                if tick.last_price >= self.stop_loss_price:
                    # 空头止损
                    trade_price = tick.last_price * 1.005
                    self.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(self.virtual_pos))
                    self.pos_open_price = 0
                    self.stop_loss_price = 0
                    self.loss_count += 1
                    return
                
                if tick.last_price <= self.short_down:
                    # 停止加仓判断
                    if self.bar_loading or self.bar_lack:
                        return
                    
                    # 空头加仓
                    if ((self.long_up / self.pos_open_price) - 1) * -1 >= abs((self.stop_loss_price / self.pos_open_price) - 1):
                        pos_open_price = tick.last_price
                        trade_price = tick.last_price * 0.995
                        if 1 / abs(((self.long_up / pos_open_price) - 1)) >= 1.5 / abs(((self.stop_loss_price / self.pos_open_price) - 1)):
                            # 满足空头加仓条件
                            value = self.portfolio.portfolioValue * self.loss_rate / abs(((self.long_up / pos_open_price) - 1))
                            volume = value / pos_open_price
                            add_volume = volume - abs(self.pos)
                            if add_volume > 0:
                                self.send_order(Direction.SHORT, Offset.OPEN, trade_price, add_volume)
                                self.pos_open_price = pos_open_price
                                self.stop_loss_price = self.long_up
                                self.max_loss_count += 1
                                return

    def send_order(self, direction, offset, price, volume):
        # 撤回历史订单
        self.cancel_all()

        # 精度处理
        contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)
        if not price or not volume:
            return
        
        # 当前虚拟持仓
        if direction == Direction.LONG:
            self.virtual_pos += volume

        else:
            self.virtual_pos -= volume

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
        if offset != Offset.OPEN:
            volume = min(volume, self.pos)
        
        # 发出订单
        super().send_order(direction, offset, price, volume)

    def on_trade(self, trade):
        super().on_trade(trade)
    
    def on_timer(self):
        # 导入bar数据
        if not self.bar_loading:
            self.load_bar_data()

        # 策略更新事件
        self.put_event()
        super().on_timer()

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"{self.strategy_name}")
