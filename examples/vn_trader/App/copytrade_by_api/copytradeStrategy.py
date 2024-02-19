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

class CopytradeStrategy(CtaTemplate):
    """ 跟单交易策略 """

    className = "CopytradeStrategy"
    author = "loe"

    # 参数列表，保存了参数的名称
    parameters = [
        "strategy_name",
        "exchange",
        "exchange_user",
        "trade_capital",
        "trade_stop_loss"
    ]

    # 变量列表，保存了变量的名称
    variables = [
        "symbol_pos_dict",
        "target_symbol_pos_dict",
        "symbol_absolute_pos_dict",
        "position_pnl",
        "position_pnl_rate"
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
        "symbol_pos_dict",
        "target_symbol_pos_dict",
        "symbol_absolute_pos_dict"
    ]

    def __init__(self, ctaEngine, setting):
        self.exchange = Exchange.NONE # 交易所
        self.exchange_user = "" # 交易所用户名

        self.symbol_pos_dict = {} # 合约净持仓
        self.target_symbol_pos_dict = {} #  合约目标净持仓
        self.symbol_absolute_pos_dict = {} # 合约双向持仓数据

        self.target_symbol_pos_cache = {} # 跟单持仓缓存
        self.wait_tick_symbols = set() # 等待行情数据的合约集合
        self.position_pnl = 0 # 持仓盈亏
        self.position_pnl_rate = "" # 持仓盈亏占比（相对投资组合总资金）

        self.check_trader_position_updated_queue = Queue() # 检查带单交易员仓位更新
        self.check_position_queue = Queue() # 检查跟单仓位队列
        self.trade_assets_setting = setting["trade_assets"] # 跟单交易资金设置

        self.portfolio = ctaEngine.copytradePortfolio # 投资组合管理

        # 交易总资金、总止损
        self.trade_capital = 0
        stop_loss = 0
        for trader_code, assets in self.trade_assets_setting.items():
            copy_data = self.portfolio.copy_setting[trader_code]
            copy_stop_loss = copy_data["stop_loss"]
            self.trade_capital += assets
            stop_loss += assets * copy_stop_loss
        self.trade_stop_loss = stop_loss / self.trade_capital if self.trade_capital else -1

        # 完成setting.json参数的配置
        super(CopytradeStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

    def on_init(self):
        # 投资组合管理缺失判断
        if not self.portfolio:
            exit(f"投资组合管理缺失")

        # 交易所配置判断
        if self.exchange == "OKX":
            self.exchange = Exchange.OKX
        
        elif self.exchange == "BINANCE":
            self.exchange = Exchange.BINANCE
        
        else:
            exit(f"跟单交易策略交易所配置错误：{self.exchange}")

        # 交易所成功连接判断
        gateway = self.cta_engine.main_engine.get_gateway(gateway_name=self.exchange.value, account_name=self.exchange_user)
        if not gateway:
            exit(f"跟单交易策略交易所未连接：{self.exchange}@{self.exchange_user}")
        
        # 订阅合约
        subscribe_vt_symbols = set()
        for pure_symbol in self.portfolio.symbols:
            vt_symbol = ""
            if self.exchange == Exchange.OKX:
                vt_symbol = f"{pure_symbol}-USDT-SWAP.{self.exchange.value}"
            
            elif self.exchange == Exchange.BINANCE:
                if pure_symbol in ["PEPE", "SHIB", "XEC", "LUNC", "FLOKI", "BONK", "SATS"]:
                    vt_symbol = f"1000{pure_symbol}USDT.{self.exchange.value}"
                    
                else:
                    vt_symbol = f"{pure_symbol}USDT.{self.exchange.value}"
                
            if vt_symbol:
                subscribe_vt_symbols.add(vt_symbol)

        subscribe_vt_symbols = subscribe_vt_symbols.union(set(self.symbol_pos_dict.keys()))
        for vt_symbol in subscribe_vt_symbols:
            contract = self.cta_engine.main_engine.get_contract(vt_symbol)
            if contract:
                req = SubscribeRequest(
                    symbol=contract.symbol, exchange=contract.exchange
                )
                self.cta_engine.main_engine.subscribe(req, contract.gateway_name)
            else:
                self.write_log(f"行情订阅失败，找不到合约{vt_symbol}")

        # 开启新线程检查带单员带单更新后的目标持仓
        t = Thread(target=self.check_trader_position_updated)
        t.start()

        # 开启新线程检查目标持仓
        t = Thread(target=self.check_target_pos)
        t.start()

        # 开启新线程等待行情数据
        t = Thread(target=self.wait_symbol_tick)
        t.start()

        # 开启新线程统计当前盈亏
        t = Thread(target=self.calculate_pnl)
        t.start()

    # 检查带单员带单更新
    def check_trader_position_updated(self):
        while True:
            if (not self.trading) or (not self.portfolio.trader_position_inited):
                sleep(1)
                continue
            
            try:
                event = self.check_trader_position_updated_queue.get(block=True, timeout=1)

                # 合约的目标仓位
                target_symbol_pos_dict = {}

                for trader, setting in self.portfolio.copy_setting.items():
                    copy_value = setting.get("copy_assets", 0)
                    trade_value = self.trade_assets_setting.get(trader, 0)
                    if not copy_value or not trade_value:
                        continue

                    for symbol, pos_data in self.portfolio.trader_position_dict[trader].items():
                        long_data = pos_data.get("long", {})
                        long_volume = long_data.get("volume", 0)

                        short_data = pos_data.get("short", {})
                        short_volume = short_data.get("volume", 0)
                        
                        # 计算目标持仓
                        pos = long_volume - short_volume
                        target_pos = pos * trade_value / copy_value

                        # 转换合约
                        pure_symbol = symbol.split("-")[0]

                        vt_symbol = ""
                        if self.exchange == Exchange.OKX:
                            vt_symbol = f"{pure_symbol}-USDT-SWAP.{self.exchange.value}"
                        
                        elif self.exchange == Exchange.BINANCE:
                            if pure_symbol in ["PEPE", "SHIB", "XEC", "LUNC", "FLOKI", "BONK", "SATS"]:
                                vt_symbol = f"1000{pure_symbol}USDT.{self.exchange.value}"
                                target_pos = target_pos / 1000
                                
                            else:
                                vt_symbol = f"{pure_symbol}USDT.{self.exchange.value}"

                        # 持仓统计
                        if vt_symbol:
                            target_symbol_pos_dict[vt_symbol] = target_symbol_pos_dict.get(vt_symbol, 0) + target_pos

                # 仓位精度处理
                for vt_symbol, pos in target_symbol_pos_dict.items():
                    contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                    if contract:
                        target_symbol_pos_dict[vt_symbol] = round_to(pos, contract.min_volume)

                # 历史持仓数据填补
                for symbol in self.target_symbol_pos_cache.keys():
                    if symbol not in target_symbol_pos_dict:
                        target_symbol_pos_dict[symbol] = 0

                # 导入持仓检查队列
                if (not event) or (self.target_symbol_pos_cache != target_symbol_pos_dict):
                    self.target_symbol_pos_cache = target_symbol_pos_dict
                    self.check_position_queue.put(target_symbol_pos_dict)
                    
                    # 发送钉钉通知
                    msg = f"跟单仓位更新\n{datetime.now()}\n"
                    for vt_symbol, pos in target_symbol_pos_dict.items():
                        msg += f"\n{vt_symbol}：{pos}"
                    msg += "\n\n------------\n"

                    for trader_name, symbol_pos_dict in self.portfolio.trader_name_position_dict.items():
                        if symbol_pos_dict:
                            msg += f"\n【{trader_name}】"

                        for symbol, pos_data in symbol_pos_dict.items():
                            msg += f"\n{symbol}"

                            long_data = pos_data.get("long", {})
                            long_volume = long_data.get("volume", 0)
                            long_price = long_data.get("price", 0)

                            short_data = pos_data.get("short", {})
                            short_volume = short_data.get("volume", 0)
                            short_price = short_data.get("price", 0)

                            if long_volume:
                                msg += f"\nlong {long_volume}@{long_price}\n"
                            
                            if short_volume:
                                msg += f"\nshort {short_volume}@{short_price}\n"

                    msg += "\n"
                    self.send_ding_talk(msg)
            
            except Empty:
                pass

            except Exception as e:
                print(f"check_trader_position_updated 报错：{e}")

    # 检查目标仓位
    def check_target_pos(self):
        while True:
            if not self.trading:
                sleep(1)
                continue
            
            try:
                checking_data = self.check_position_queue.get(block=True, timeout=1)

                oms_engine = self.cta_engine.main_engine.engines["oms"]
                for vt_symbol, checking_pos in checking_data.items():
                    contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                    if not contract:
                        self.send_ding_talk(f"交易合约{vt_symbol}不存在")
                        return

                    # 检查合约目标持仓是否发生变化
                    checking_pos = round_to(checking_pos, contract.min_volume)
                    target_pos = self.target_symbol_pos_dict.get(vt_symbol, 0)
                    if target_pos != checking_pos:
                        # 获取合约最新行情数据
                        tick = oms_engine.ticks.get(vt_symbol, None)
                        if not tick:
                            self.send_ding_talk(f"交易合约{vt_symbol}行情数据缺失")

                            # 订阅合约行情
                            req = SubscribeRequest(
                                symbol=contract.symbol, exchange=contract.exchange
                            )
                            self.cta_engine.main_engine.subscribe(req, contract.gateway_name)
                            
                            # 行情数据监控
                            self.wait_tick_symbols.add(vt_symbol)

                        else:
                            # 合约目标持仓更新
                            target_pos = checking_pos
                            if target_pos:
                                self.target_symbol_pos_dict[vt_symbol] = target_pos
                            else:
                                self.target_symbol_pos_dict.pop(vt_symbol)

                            # 取消该合约正在进行中的订单
                            active_orders = oms_engine.get_all_active_orders(vt_symbol)
                            strategy_orderids = self.cta_engine.strategy_orderid_map[self.strategy_name]
                            for order in active_orders:
                                if order.vt_orderid in strategy_orderids:
                                    self.cancel_order(order.vt_orderid)

                            # 发出订单
                            # long_open_price = max(tick.last_price + contract.pricetick*100, tick.last_price * 1.0005)
                            # short_open_price = min(tick.last_price - contract.pricetick*100, tick.last_price * 0.9995)
                            long_open_price = tick.last_price * 1.0005
                            short_open_price = tick.last_price * 0.9995
                            long_close_price = tick.last_price * 1.01
                            short_close_price = tick.last_price * 0.99
                            current_pos = self.symbol_pos_dict.get(vt_symbol, 0)
                            if target_pos > 0:
                                if current_pos < 0:
                                    # 先平空
                                    self.send_symbol_order(vt_symbol, Direction.LONG, Offset.CLOSE, long_close_price, abs(current_pos))

                                    # 再开多
                                    self.send_symbol_order(vt_symbol, Direction.LONG, Offset.OPEN, long_open_price, abs(target_pos))
                                
                                elif current_pos == 0:
                                    # 开多
                                    self.send_symbol_order(vt_symbol, Direction.LONG, Offset.OPEN, long_open_price, abs(target_pos))
                                
                                else:
                                    if target_pos > current_pos:
                                        # 开多（加仓）
                                        volume = target_pos - current_pos
                                        self.send_symbol_order(vt_symbol, Direction.LONG, Offset.OPEN, long_open_price, abs(volume))
                                    
                                    elif target_pos < current_pos:
                                        # 平多（减仓）
                                        volume = target_pos - current_pos
                                        self.send_symbol_order(vt_symbol, Direction.SHORT, Offset.CLOSE, short_close_price, abs(volume))

                            elif target_pos == 0:
                                if current_pos > 0:
                                    # 平多
                                    self.send_symbol_order(vt_symbol, Direction.SHORT, Offset.CLOSE, short_close_price, abs(current_pos))

                                elif current_pos < 0:
                                    # 平空
                                    self.send_symbol_order(vt_symbol, Direction.LONG, Offset.CLOSE, long_close_price, abs(current_pos))

                            else:
                                if current_pos > 0:
                                    # 先平多
                                    self.send_symbol_order(vt_symbol, Direction.SHORT, Offset.CLOSE, short_close_price, abs(current_pos))

                                    # 再开空
                                    self.send_symbol_order(vt_symbol, Direction.SHORT, Offset.OPEN, short_open_price, abs(target_pos))
                                
                                elif current_pos == 0:
                                    # 开空
                                    self.send_symbol_order(vt_symbol, Direction.SHORT, Offset.OPEN, short_open_price, abs(target_pos))
                                
                                else:
                                    if target_pos < current_pos:
                                        # 开空（加仓）
                                        volume = target_pos - current_pos
                                        self.send_symbol_order(vt_symbol, Direction.SHORT, Offset.OPEN, short_open_price, abs(volume))
                                    
                                    elif target_pos > current_pos:
                                        # 平空（减仓）
                                        volume = target_pos - current_pos
                                        self.send_symbol_order(vt_symbol, Direction.LONG, Offset.CLOSE, long_close_price, abs(volume))

            except:
                pass

    def wait_symbol_tick(self):
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        while True:
            try:
                for vt_symbol in list(self.wait_tick_symbols):
                    tick = oms_engine.ticks.get(vt_symbol, None)
                    if tick:
                        self.check_trader_position_updated_queue.put(None)
                        self.wait_tick_symbols.remove(vt_symbol)
            except Exception as e:
                pass
            sleep(0.1)

    def calculate_pnl(self):
        while True:
            try:
                oms_engine = self.cta_engine.main_engine.engines["oms"]

                # 计算策略跟单盈亏
                copy_pnl = 0
                for vt_symbol in list(self.symbol_absolute_pos_dict.keys()):
                    pos_data = self.symbol_absolute_pos_dict[vt_symbol]
                    tick = oms_engine.ticks.get(vt_symbol, None)
                    if tick:
                        long_data = pos_data.get("long", {})
                        long_volume = abs(long_data.get("volume", 0))
                        long_price = long_data.get("price", 0)
                        if long_volume and long_price and tick.last_price:
                            long_pnl = long_volume * (tick.last_price - long_price)
                            copy_pnl += long_pnl

                        short_data = pos_data.get("short", {})
                        short_volume = abs(short_data.get("volume", 0))
                        short_price = short_data.get("price", 0)
                        if short_volume and short_price and tick.last_price:
                            short_pnl = short_volume * (short_price - tick.last_price)
                            copy_pnl += short_pnl
                
                self.position_pnl = round(copy_pnl, 2)
                position_pnl_rate = copy_pnl / self.trade_capital
                if self.trading and position_pnl_rate <= self.trade_stop_loss:
                    # ====== 止损平仓 ======

                    # 取消所有正在进行中的订单
                    self.cancel_all()

                    # 发出平仓订单
                    for vt_symbol, current_pos in self.symbol_pos_dict.items():
                        tick = oms_engine.ticks.get(vt_symbol, None)
                        if tick:
                            long_close_price = tick.last_price * 1.01
                            short_close_price = tick.last_price * 0.99
                            if current_pos > 0:
                                # 平多
                                self.send_symbol_order(vt_symbol, Direction.SHORT, Offset.CLOSE, short_close_price, abs(current_pos))

                            elif current_pos < 0:
                                # 平空
                                self.send_symbol_order(vt_symbol, Direction.LONG, Offset.CLOSE, long_close_price, abs(current_pos))
                    
                    # 停止策略，发出通知
                    self.trading = False
                    msg = f"\n投资组合当前亏损：{position_pnl_rate}\n最大亏损限制：{self.trade_stop_loss}\n已强制清仓，停止策略"
                    self.send_ding_talk(msg)
                        
                self.position_pnl_rate = f"{round(position_pnl_rate * 100, 2)}%"
                
            except Exception as e:
                print(f"calculate_pnl 报错：{e}")

            sleep(1)

    def send_symbol_order(self, symbol, direction, offset, price, volume, stop=False):
        contract = self.cta_engine.main_engine.get_contract(symbol)
        volume = round_to(abs(volume), contract.min_volume)
        if not volume:
            return

        # 币安开仓有最低价值限制，判断是否满足
        if offset == Offset.OPEN and self.exchange == Exchange.BINANCE:
            oms_engine = self.cta_engine.main_engine.engines["oms"]
            tick = oms_engine.ticks.get(symbol, None)
            if tick:
                value_cross = True
                order_value = tick.last_price * volume
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

                if order_value <= 5:
                    value_cross = False
                
                if not value_cross:
                    self.send_ding_talk(f"开仓订单价值未满足要求\n合约：{symbol}\n价格：{tick.last_price}\n数量：{volume}\n价值：{order_value}")
                    return
        
        super().send_symbol_order(symbol, direction, offset, price, volume, stop)

    def on_copy_trader(self):
        # 带单员带单更新
        self.check_trader_position_updated_queue.put(True)

    def on_trade(self, trade):
        """成交推送"""
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
