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
from App.marting.martingPortfolio import BAR_DOWNLOAD_GENERATE_COMPLETE
from vnpy.event import Event
from copy import copy
from vnpy.trader.event import EVENT_MAINENGINE_POSITION_UPDATED
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
        "portfolio_value",
        "stop_loss"
    ]

    # 变量列表，保存了变量的名称
    variables = [
        "symbol_pos_dict",
        "target_symbol_pos_dict",
        "symbol_absolute_pos_dict",
        "trader_name_position_dict",
        "trader_position_inited",
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
        self.symbol_pos_dict = {} # 合约净持仓
        self.target_symbol_pos_dict = {} #  合约目标净持仓
        self.symbol_absolute_pos_dict = {} # 合约双向持仓数据

        self.trader_position_dict = {} # 带单交易员带单数据
        self.trader_name_position_dict = {} # 带单交易员带单数据
        self.trader_position_inited = False # 带单交易员带单数据初始化

        self.copy_position_cache = {} # 跟单持仓缓存
        self.trader_position_cache = {} # 带单员带单持仓缓存

        self.wait_tick_symbols = set() # 等待行情数据的合约集合
        self.position_pnl = 0 # 持仓盈亏
        self.position_pnl_rate = "" # 持仓盈亏占比（相对投资组合总资金）

        self.check_position_queue = Queue()
        self.check_trader_position_updated_queue = Queue()

        # 导入跟单设置
        self.copy_setting = setting.get("copy_setting", {})

        # 带单员带单监控
        self.trader_setting = setting.get("trader_setting", {})

        # 投资组合设置
        portfolio_setting = setting.get("portfolio", {})
        self.portfolio_value = portfolio_setting.get("capital", 1000000000)
        self.stop_loss = portfolio_setting.get("stop_loss", 1)

        # 默认合约列表
        self.default_vt_symbols = setting.get("vt_symbols", [])

        # 完成setting.json参数的配置
        super(CopytradeStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

    def on_init(self):
        # 订阅交易所仓位更新
        self.cta_engine.event_engine.register(EVENT_MAINENGINE_POSITION_UPDATED, self.on_mainengine_position_updated)

        # 订阅合约
        subscribe_vt_symbols = set(self.default_vt_symbols + list(self.symbol_pos_dict.keys()))
        for vt_symbol in subscribe_vt_symbols:
            contract = self.cta_engine.main_engine.get_contract(vt_symbol)
            if contract:
                req = SubscribeRequest(
                    symbol=contract.symbol, exchange=contract.exchange
                )
                self.cta_engine.main_engine.subscribe(req, contract.gateway_name)
            else:
                self.write_log(f"行情订阅失败，找不到合约{vt_symbol}")

        # 开启新线程检查目标持仓
        t = Thread(target=self.check_target_pos)
        t.start()

        # 开启新线程检查带单员带单更新后的目标持仓
        t = Thread(target=self.check_trader_position_updated)
        t.start()

        # 开启新线程等待行情数据
        t = Thread(target=self.wait_symbol_tick)
        t.start()

        # 开启新线程统计当前盈亏
        t = Thread(target=self.calculate_pnl)
        t.start()

        # 开启新线程获取交易员的当前带单
        for trader, setting in self.trader_setting.items():
            start = setting.get("start", False)
            if start:
                t = Thread(target=self.fetch_copytrade_data, args=(trader,))
                t.start()

    def on_start(self):
        self.trading = True
        self.on_mainengine_position_updated(event=None)
        self.check_trader_position_updated_queue.put(None)

    # 跟单持仓更新
    def on_mainengine_position_updated(self, event):
        # 合约的目标仓位
        target_symbol_pos_dict = {}

        # 获取所有跟单账号持仓
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        for vt_positionid, position in oms_engine.positions.items():
            exchange: Exchange = position.exchange
            exchange_user: str = position.exchange_user
            target_setting = self.copy_setting.get(exchange.value, {}).get(exchange_user, {})
            copy_assets = target_setting.get("copy_assets", 0)
            copy_rate = target_setting.get("copy_rate", 0)
            copy_value = copy_assets * copy_rate
            trade_value = target_setting.get("trade_value", 0)
            start = target_setting.get("start", False)
            if copy_value and trade_value and start:
                # 计算目标持仓
                target_pos = abs(position.volume * trade_value / copy_value)
                if position.direction == Direction.SHORT:
                    target_pos = target_pos * -1

                # 转换合约
                pure_symbol = position.symbol.split("-")[0]
                if pure_symbol in ["PEPE", "SHIB", "XEC", "LUNC", "FLOKI", "BONK", "SATS"]:
                    binance_symbol = f"1000{pure_symbol}USDT.BINANCE"
                    target_pos = target_pos / 1000
                    
                else:
                    binance_symbol = f"{pure_symbol}USDT.BINANCE"

                # 持仓统计
                target_symbol_pos_dict[binance_symbol] = target_symbol_pos_dict.get(binance_symbol, 0) + target_pos
            # print(f"{datetime.now()}\t{vt_positionid}\t{position.volume}\t{position.price}")
        
        # 仓位精度处理
        for vt_symbol, pos in target_symbol_pos_dict.items():
            contract = self.cta_engine.main_engine.get_contract(vt_symbol)
            if contract:
                target_symbol_pos_dict[vt_symbol] = round_to(pos, contract.min_volume)

        # 导入持仓检查队列
        if self.trading and self.copy_position_cache != target_symbol_pos_dict:
            self.copy_position_cache = target_symbol_pos_dict
            self.check_position_queue.put(target_symbol_pos_dict)
        # print(f"\n")

    # 带单员带单更新
    def check_trader_position_updated(self):
        while True:
            try:
                __ = self.check_trader_position_updated_queue.get(block=True, timeout=1)

                # 合约的目标仓位
                target_symbol_pos_dict = {}

                inited = True
                for trader, setting in self.trader_setting.items():
                    copy_assets = setting.get("copy_assets", 0)
                    copy_rate = setting.get("copy_rate", 0)
                    copy_value = copy_assets * copy_rate
                    trade_value = setting.get("trade_value", 0)
                    start = setting.get("start", False)
                    if not copy_value or not trade_value or not start:
                        continue

                    if trader in self.trader_position_dict:
                        for symbol, pos in self.trader_position_dict[trader].items():
                            # 计算目标持仓
                            target_pos = pos * trade_value / copy_value

                            # 转换合约
                            pure_symbol = symbol.split("-")[0]
                            if pure_symbol in ["PEPE", "SHIB", "XEC", "LUNC", "FLOKI", "BONK", "SATS"]:
                                binance_symbol = f"1000{pure_symbol}USDT.BINANCE"
                                target_pos = target_pos / 1000
                                
                            else:
                                binance_symbol = f"{pure_symbol}USDT.BINANCE"

                            # 持仓统计
                            target_symbol_pos_dict[binance_symbol] = target_symbol_pos_dict.get(binance_symbol, 0) + target_pos
                    
                    else:
                        # 未完全获取所有带单员带单数据
                        inited = False

                # 仓位精度处理
                for vt_symbol, pos in target_symbol_pos_dict.items():
                    contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                    if contract:
                        target_symbol_pos_dict[vt_symbol] = round_to(pos, contract.min_volume)

                # 历史持仓数据填补
                for symbol in self.trader_position_cache.keys():
                    if symbol not in target_symbol_pos_dict:
                        target_symbol_pos_dict[symbol] = 0

                # 数据初始化判断
                self.trader_position_inited = inited
                if self.trader_position_inited:
                    # 导入持仓检查队列
                    if self.trading and self.trader_position_cache != target_symbol_pos_dict:
                        self.trader_position_cache = target_symbol_pos_dict
                        # self.check_position_queue.put(target_symbol_pos_dict)
                        
                        # 发送钉钉通知
                        msg = f"带单员带单更新\n\n时间：{datetime.now()}\n"
                        for trader_name, pos_data in self.trader_name_position_dict.items():
                            msg += f"\n{trader_name}：{pos_data}\n"
                        msg += "\n"
                        self.send_ding_talk(msg)
            
            except:
                pass

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
                            for order in active_orders:
                                self.cancel_order(order.vt_orderid)

                            # 发出订单
                            long_open_price = tick.last_price + contract.pricetick*100
                            short_open_price = tick.last_price - contract.pricetick*100
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

                self.put_timer_event()

            except:
                pass

    def wait_symbol_tick(self):
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        while True:
            try:
                for vt_symbol in list(self.wait_tick_symbols):
                    tick = oms_engine.ticks.get(vt_symbol, None)
                    if tick:
                        self.on_mainengine_position_updated(event=None)
                        self.wait_tick_symbols.remove(vt_symbol)
            except Exception as e:
                pass
            sleep(0.1)

    def calculate_pnl(self):
        while True:
            pnl = 0
            try:
                oms_engine = self.cta_engine.main_engine.engines["oms"]
                for vt_symbol in list(self.symbol_absolute_pos_dict.keys()):
                    pos_data = self.symbol_absolute_pos_dict[vt_symbol]
                    tick = oms_engine.ticks.get(vt_symbol, None)
                    if tick:
                        long_data = pos_data.get("long", {})
                        long_volume = abs(long_data.get("volume", 0))
                        long_price = long_data.get("price", 0)
                        if long_volume and long_price and tick.last_price:
                            long_pnl = long_volume * (tick.last_price - long_price)
                            pnl += long_pnl

                        short_data = pos_data.get("short", {})
                        short_volume = abs(short_data.get("volume", 0))
                        short_price = short_data.get("price", 0)
                        if short_volume and short_price and tick.last_price:
                            short_pnl = short_volume * (short_price - tick.last_price)
                            pnl += short_pnl
                
                self.position_pnl = round(pnl, 2)
                self.position_pnl_rate = f"{round(pnl / self.portfolio_value * 100, 2)}%"
                self.put_timer_event()
                
            except Exception as e:
                pass
            sleep(1)

    def send_symbol_order(self, symbol, direction, offset, price, volume, stop=False):
        contract = self.cta_engine.main_engine.get_contract(symbol)
        volume = round_to(abs(volume), contract.min_volume)
        if not volume:
            return

        # 币安开仓有最低价值限制，判断是否满足
        if offset == Offset.OPEN:
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

    def fetch_copytrade_data(self, trader):
        setting = self.trader_setting.get(trader, {})
        trader_name = setting.get("trader", "")
        error_notice_time = 0
        while True:
            try:
                gateway = self.cta_engine.main_engine.get_default_gateway("OKX")
                if gateway:
                    # 查询交易员排行榜
                    # rank_data = gateway.rest_api.query_copytrader_rank()

                    # 查询交易员当前带单
                    """
                    从小有个百万梦 '540D011FDACCB47A'
                    墙头草 'D5E7A8430A35CA84'
                    Alvinnn111 '9B28742D954561AE'
                    """
                    trader_position_data = gateway.rest_api.query_copytrade(trader)
                    if isinstance(trader_position_data, list):
                        net_pos_dict_real = {}
                        net_pos_dict_copy = {}
                        for d in trader_position_data:
                            symbol = d["instId"]
                            pure_symbol = symbol.split("-")[0]
                            symbol_list = setting.get("symbol_list", [])
                            if symbol_list and pure_symbol not in symbol_list:
                                continue

                            contract = self.cta_engine.main_engine.get_contract(f"{symbol}.OKX")
                            if contract:
                                subPos = float(d["subPos"])
                                pos = abs(contract.min_volume * subPos)

                                posSide = d["posSide"]
                                if posSide == "short":
                                    pos = pos * -1
                                
                                # 计算带单员实际净持仓
                                symbol_net_pos_real = net_pos_dict_real.get(symbol, 0) + pos
                                net_pos_dict_real[symbol] = round_to(symbol_net_pos_real, contract.min_volume)

                                # 根据跟单比例计算净持仓
                                pos = pos * setting.get("copy_rate", 1)
                                pos = floor_to(pos, contract.min_volume)
                                symbol_net_pos = net_pos_dict_copy.get(symbol, 0) + pos
                                net_pos_dict_copy[symbol] = round_to(symbol_net_pos, contract.min_volume)
                                # print(f"{symbol}\t{posSide}\t{pos}")

                        # 带单交易员带单数据更新
                        if (trader not in self.trader_position_dict) or self.trader_position_dict[trader] != net_pos_dict_copy:
                            self.trader_position_dict[trader] = net_pos_dict_copy
                            self.trader_name_position_dict[trader_name] = net_pos_dict_copy
                            self.check_trader_position_updated_queue.put(None)
                        print(f"{datetime.now()}\t带单员：{trader_name}\t开单数量：{len(trader_position_data)}\t实际净持仓：{net_pos_dict_real}\t跟单净持仓：{net_pos_dict_copy}\n")
                    
                    else:
                        error_notice_gap = int(time.time()) - error_notice_time
                        if error_notice_gap >= 60*10:
                            error_notice_time = int(time.time())
                            msg = f"！获取（{trader_name}）带单数据类型异常！\n{trader_position_data}"
                            self.send_ding_talk(msg)

            except Exception as e:
                error_notice_gap = int(time.time()) - error_notice_time
                if error_notice_gap >= 60*10:
                    error_notice_time = int(time.time())
                    msg = f"！获取（{trader_name}）带单报错！\n{e}"
                    self.send_ding_talk(msg)

            sleep(0.01)

    def on_trade(self, trade):
        """成交推送"""
        super().on_trade(trade)
        self.put_timer_event()
    
    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"{self.strategy_name}")
