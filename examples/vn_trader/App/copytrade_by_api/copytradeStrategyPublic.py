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
from vnpy.trader.event import EVENT_MAINENGINE_POSITION_UPDATED
from vnpy.trader.utility import DIR_SYMBOL
import pandas as pd

class CopytradeStrategyPublic(CtaTemplate):
    """ 跟单交易策略 """

    className = "CopytradeStrategyPublic"
    author = "loe"

    # 参数列表，保存了参数的名称
    parameters = [
        "strategy_name",
        "exchange",
        "exchange_user",
        "type",
        "trade_capital",
        "trade_stop_loss"
    ]

    # 变量列表，保存了变量的名称
    variables = [
        "trader_symbol_pos_dict",
        "target_trader_symbol_pos_dict",
        "symbol_absolute_pos_dict",
        "position_pnl",
        "position_pnl_rate"
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
        "trader_symbol_pos_dict",
        "target_trader_symbol_pos_dict",
        "symbol_absolute_pos_dict",
        "target_trader_symbol_absolute_pos_dict"
    ]

    def __init__(self, ctaEngine, setting):
        self.exchange = Exchange.NONE # 交易所
        self.exchange_user = "" # 交易所用户名

        self.trader_symbol_pos_dict = {} # 交易员分类的合约净持仓
        self.target_trader_symbol_pos_dict = {} #  交易员分类的目标合约净持仓
        self.symbol_absolute_pos_dict = {} # 合约双向持仓数据
        self.target_trader_symbol_absolute_pos_dict = {} # 根据交易员分类的合约目标双向持仓数据
        self.orderid_trader_dict = {} # 根据交易员分类的订单id

        self.wait_tick_symbols = set() # 等待行情数据的合约集合
        self.position_pnl = 0 # 持仓盈亏
        self.position_pnl_rate = "" # 持仓盈亏占比（相对投资组合总资金）

        self.check_trader_position_updated_queue = Queue() # 检查带单交易员仓位更新
        self.check_position_queue = Queue() # 检查跟单仓位队列
        self.trade_assets_setting = setting["trade_assets"] # 跟单交易资金设置

        self.portfolio = ctaEngine.copytradePortfolio # 投资组合管理
        self.next_check_real_strategy_position_dt = None # 下一次检查对比交易所持仓和策略持仓的时间
        self.trader_pnl_data_dict = {} # 根据交易员分类的pnl数据

        # 交易总资金、总止损
        self.trade_capital = 0
        stop_loss = 0
        for trader_code, assets in self.trade_assets_setting.items():
            copy_data = self.portfolio.copy_setting[trader_code]
            copy_stop_loss = copy_data["stop_loss"]
            self.trade_capital += assets
            stop_loss += assets * copy_stop_loss
        self.trade_stop_loss = stop_loss / self.trade_capital if self.trade_capital else -1

        # 文件获取根据交易员分类的pnl数据
        for trader, assets in self.trade_assets_setting.items():
            trader_setting = self.portfolio.copy_setting.get(trader, {})
            trader_name = trader_setting.get("trader", "")
            if not trader_name:
                continue

            strategy_name = setting["strategy_name"]
            if not strategy_name:
                continue

            # 文件路径
            dir = os.getcwd()
            dir_path = Path(dir).joinpath(f"BaiduSyncdisk{DIR_SYMBOL}PNL_{strategy_name}{DIR_SYMBOL}")
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            file_path = dir_path.joinpath(f"{trader_name}.csv")

            # 获取文件数据
            pnl_data_list = []
            if os.path.exists(file_path):
                csv_data = pd.read_csv(file_path)
                for _, row in csv_data.iterrows():
                    row_dict = dict(row)
                    pnl_data_list.append(row_dict)
            
            self.trader_pnl_data_dict[trader_name] = pnl_data_list

        # 完成setting.json参数的配置
        super(CopytradeStrategyPublic, self).__init__(
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

        for _, symbol_pos_data in self.trader_symbol_pos_dict.items():
            for vt_symbol, _ in symbol_pos_data.items():
                subscribe_vt_symbols = subscribe_vt_symbols.add(vt_symbol)

        for vt_symbol in subscribe_vt_symbols:
            contract = self.cta_engine.main_engine.get_contract(vt_symbol)
            if contract:
                req = SubscribeRequest(
                    symbol=contract.symbol, exchange=contract.exchange
                )
                self.cta_engine.main_engine.subscribe(req, contract.gateway_name)
            else:
                self.write_log(f"行情订阅失败，找不到合约{vt_symbol}")
                
        # 订阅交易所仓位更新
        self.cta_engine.event_engine.register(EVENT_MAINENGINE_POSITION_UPDATED, self.on_mainengine_position_updated)

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

    def on_start(self):
        # 一分钟后开始检查对比交易所持仓和策略持仓
        self.next_check_real_strategy_position_dt = datetime.now() + timedelta(minutes=1)

    def on_mainengine_position_updated(self, event):
        """ fake """
        return
    
        if not self.trading:
            return
        
        if self.next_check_real_strategy_position_dt and datetime.now() < self.next_check_real_strategy_position_dt:
            return

        # 钉钉消息标题
        ding_talk_msg = f"交易所持仓与策略持仓不一致"
        ding_talk_action = False

        # 获取所有跟单账号持仓
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        for _, position in oms_engine.positions.items():
            exchange: Exchange = position.exchange
            exchange_user: str = position.exchange_user
            if self.exchange == exchange and self.exchange_user == exchange_user:
                # 真实持仓
                real_pos = position.volume

                # 精度处理
                contract = self.cta_engine.main_engine.get_contract(position.vt_symbol)
                if contract:
                    real_pos = round_to(real_pos, contract.min_volume)

                absolute_pos_data = self.symbol_absolute_pos_dict.get(position.vt_symbol, {})
                if position.direction == Direction.LONG:
                    long_data = absolute_pos_data.get("long", {})
                    long_volume = long_data.get("volume", 0)
                    if abs(long_volume) != abs(real_pos):
                        ding_talk_action = True
                        ding_talk_msg += f"\n\n{position.vt_symbol}\n{position.direction.value} {real_pos}&{long_volume}"

                elif position.direction == Direction.SHORT:
                    short_data = absolute_pos_data.get("short", {})
                    short_volume = short_data.get("volume", 0)
                    if abs(short_volume) != abs(real_pos):
                        ding_talk_action = True
                        ding_talk_msg += f"\n\n{position.vt_symbol}\n{position.direction.value} {real_pos}&{short_volume}"
        
        # 发送钉钉通知
        if ding_talk_action:
            self.next_check_real_strategy_position_dt = datetime.now() + timedelta(hours=1)
            self.send_ding_talk(ding_talk_msg)

    # 检查带单员带单更新
    def check_trader_position_updated(self):
        while True:
            if (not self.trading) or (not self.portfolio.trader_position_inited):
                sleep(1)
                continue
            
            try:
                event = self.check_trader_position_updated_queue.get(block=True, timeout=1)

                # 合约的目标仓位
                target_trader_symbol_pos_dict = {}

                for trader, setting in self.portfolio.copy_setting.items():
                    start = setting.get("start", False)
                    if not start:
                        continue

                    copy_value = setting.get("copy_assets", 0)
                    trade_value = self.trade_assets_setting.get(trader, 0)
                    if not copy_value or not trade_value:
                        continue

                    for symbol, pos_data in self.portfolio.trader_position_dict[trader].items():
                        long_data = pos_data.get("long", {})
                        long_volume = long_data.get("volume", 0)
                        long_price = long_data.get("price", 0)
                        long_lever = long_data.get("lever", 0)

                        short_data = pos_data.get("short", {})
                        short_volume = short_data.get("volume", 0)
                        short_price = short_data.get("price", 0)
                        short_lever = short_data.get("lever", 0)

                        if long_lever or short_lever:
                            # 计算目标持仓
                            long_volume = long_volume * trade_value / copy_value
                            short_volume = short_volume * trade_value / copy_value

                            # 转换合约
                            pure_symbol = symbol.split("-")[0]

                            # 墙头草跟单山寨币仓位加倍
                            if (trader == "D5E7A8430A35CA84") and (pure_symbol not in ["BTC", "ETH", "XRP"]):
                                long_volume *= 2
                                short_volume *= 2

                            vt_symbol = ""
                            if self.exchange == Exchange.OKX:
                                vt_symbol = f"{pure_symbol}-USDT-SWAP.{self.exchange.value}"
                            
                            elif self.exchange == Exchange.BINANCE:
                                if pure_symbol in ["PEPE", "SHIB", "XEC", "LUNC", "FLOKI", "BONK", "SATS"]:
                                    vt_symbol = f"1000{pure_symbol}USDT.{self.exchange.value}"
                                    long_volume = long_volume / 1000
                                    short_volume = short_volume / 1000
                                    
                                else:
                                    vt_symbol = f"{pure_symbol}USDT.{self.exchange.value}"

                            # 持仓统计
                            if vt_symbol:
                                target_pos_data = {}
                                if long_volume:
                                    target_pos_data["long"] = {"volume":long_volume, "price":long_price}

                                if short_volume:
                                    target_pos_data["short"] = {"volume":short_volume, "price":short_price}

                                symbol_pos_data = target_trader_symbol_pos_dict.get(trader, {})
                                symbol_pos_data[vt_symbol] = target_pos_data
                                target_trader_symbol_pos_dict[trader] = symbol_pos_data

                # 仓位精度处理
                for trader, symbol_pos_data in target_trader_symbol_pos_dict.items():
                    for vt_symbol, pos_data in symbol_pos_data.items():
                        contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                        if contract:
                            for _, data in pos_data.items():
                                data["volume"] = round_to(data["volume"], contract.min_volume)

                # 历史持仓数据填补
                for trader, symbol_pos_data in self.target_trader_symbol_pos_dict.items():
                    for vt_symbol, pos_data in symbol_pos_data.items():
                        for direction, data in pos_data.items():
                            t_symbol_pos_data = target_trader_symbol_pos_dict.get(trader, {})
                            t_pos_data = t_symbol_pos_data.get(vt_symbol, {})
                            t_data = t_pos_data.get(direction, {})
                            if not t_data:
                                t_pos_data[direction] = {}
                                t_symbol_pos_data[vt_symbol] = t_pos_data
                                target_trader_symbol_pos_dict[trader] = t_symbol_pos_data

                if (not event) or (self.target_trader_symbol_pos_dict != target_trader_symbol_pos_dict):
                    self.check_position_queue.put(target_trader_symbol_pos_dict)
                    
                    # 发送钉钉通知
                    msg = f"跟单个人交易更新\n{datetime.now()}\n"
                    for trader, symbol_pos_data in target_trader_symbol_pos_dict.items():
                        trader_setting = self.portfolio.copy_setting.get(trader, {})
                        trader_name = trader_setting.get("trader", "")
                        for vt_symbol, pos_data in symbol_pos_data.items():
                            for direction, data in pos_data.items():
                                pos = data["volume"]
                                if direction == "short":
                                    pos *= -1
                                msg += f"\n{trader_name} {vt_symbol}：{pos}"
                    msg += "\n\n------------\n"

                    for trader_name, symbol_pos_data in self.portfolio.trader_name_position_dict.items():
                        if symbol_pos_data:
                            msg += f"\n【{trader_name}】"

                        for symbol, pos_data in symbol_pos_data.items():
                            msg += f"\n{symbol}"

                            long_data = pos_data.get("long", {})
                            long_volume = long_data.get("volume", 0)
                            long_price = long_data.get("price", 0)
                            long_lever = long_data.get("lever", 0)

                            short_data = pos_data.get("short", {})
                            short_volume = short_data.get("volume", 0)
                            short_price = short_data.get("price", 0)
                            short_lever = short_data.get("lever", 0)

                            if long_volume:
                                msg += f"\nlong（{long_lever}） {long_volume}@{long_price}\n"
                            
                            if short_volume:
                                msg += f"\nshort（{short_lever}） {short_volume}@{short_price}\n"

                    msg += "\n"
                    self.send_ding_talk(msg)
            
            except Empty:
                pass

            except Exception as e:
                print(f"check_trader_position_updated 报错：{e}")

    def check_target_pos(self):
        while True:
            if not self.trading:
                sleep(1)
                continue
            
            try:
                checking_data = self.check_position_queue.get(block=True, timeout=1)

                oms_engine = self.cta_engine.main_engine.engines["oms"]

                for trader, symbol_pos_data in checking_data.items():
                    for vt_symbol, pos_data in symbol_pos_data.items():
                        for direction, data in pos_data.items():
                            checking_pos = data.get("volume", 0)
                            if direction == "short":
                                checking_pos *= -1

                            contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                            if not contract:
                                self.send_ding_talk(f"交易合约{vt_symbol}不存在")
                                return

                            # 检查合约目标持仓是否发生变化
                            checking_pos = round_to(checking_pos, contract.min_volume)
                            t_symbol_pos_data = self.target_trader_symbol_pos_dict.get(trader, {})
                            t_pos_data = t_symbol_pos_data.get(vt_symbol, {})
                            t_data = t_pos_data.get(direction, {})
                            target_pos = t_data.get("volume", 0)
                            if direction == "short":
                                target_pos *= -1

                            if target_pos != checking_pos:
                                # 获取合约最新行情数据
                                tick = oms_engine.ticks.get(vt_symbol, None)
                                if not tick and target_pos:
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
                                    last_target_pos = target_pos
                                    target_pos = checking_pos

                                    # 初始开仓严格限价
                                    open_price = 0
                                    if target_pos and not last_target_pos:
                                        open_price = data["price"]

                                        # 避免限价单价格过高或者过低超出交易所限制而被拒单
                                        if direction == "long":
                                            open_price = min(open_price, tick.last_price*1.01)

                                        if direction == "short":
                                            open_price = max(open_price, tick.last_price*0.99)

                                    if target_pos:
                                        t_data["volume"] = abs(target_pos)
                                        t_data["price"] = data["price"]
                                        t_pos_data[direction] = t_data
                                        t_symbol_pos_data[vt_symbol] = t_pos_data
                                        self.target_trader_symbol_pos_dict[trader] = t_symbol_pos_data

                                    elif direction in t_pos_data:
                                        t_pos_data.pop(direction)
                                        if t_pos_data:
                                            t_symbol_pos_data[vt_symbol] = t_pos_data

                                        elif vt_symbol in t_symbol_pos_data:
                                            t_symbol_pos_data.pop(vt_symbol)

                                        if t_symbol_pos_data:
                                            self.target_trader_symbol_pos_dict[trader] = t_symbol_pos_data

                                        elif trader in self.target_trader_symbol_pos_dict:
                                            self.target_trader_symbol_pos_dict.pop(trader)

                                    # 取消该合约正在进行中的订单
                                    active_orders = oms_engine.get_all_active_orders(vt_symbol)
                                    strategy_orderids = self.cta_engine.strategy_orderid_map[self.strategy_name]
                                    for order in active_orders:
                                        if order.vt_orderid in strategy_orderids:
                                            self.cancel_order(order.vt_orderid)

                                    # 发出订单
                                    # 限价单价格
                                    long_open_price = tick.last_price * 1.0015 if tick else open_price
                                    short_open_price = tick.last_price * 0.9985 if tick else open_price
                                    long_close_price = tick.last_price * 1.01 if tick else open_price
                                    short_close_price = tick.last_price * 0.99 if tick else open_price
                                    
                                    # 当前交易员分类的实际持仓
                                    current_symbol_pos_data = self.trader_symbol_pos_dict.get(trader, {})
                                    current_pos_data = current_symbol_pos_data.get(vt_symbol, {})
                                    current_data = current_pos_data.get(direction, {})
                                    current_pos = current_data.get("volume", 0.0)
                                    if direction == "short":
                                        current_pos *= -1
                                    
                                    sub = abs(target_pos) - abs(last_target_pos)
                                    if direction == "long":
                                        if sub > 0:
                                            # 多头加仓
                                            price = open_price if open_price else long_open_price
                                            self.send_symbol_order(trader, vt_symbol, Direction.LONG, Offset.OPEN, price, abs(sub))

                                        elif sub < 0:
                                            # 多头减仓
                                            v = min(abs(current_pos), abs(sub))
                                            if v:
                                                self.send_symbol_order(trader, vt_symbol, Direction.SHORT, Offset.CLOSE, short_close_price, abs(v))

                                    elif direction == "short":
                                        if sub > 0:
                                            # 空头加仓
                                            price = open_price if open_price else short_open_price
                                            self.send_symbol_order(trader, vt_symbol, Direction.SHORT, Offset.OPEN, price, abs(sub))

                                        elif sub < 0:
                                            # 空头减仓
                                            v = min(abs(current_pos), abs(sub))
                                            if v:
                                                self.send_symbol_order(trader, vt_symbol, Direction.LONG, Offset.CLOSE, long_close_price, abs(v))

                                    # 更新根据交易员分类的pnl数据
                                    self.update_pnl_result_on_trader()
            except Empty:
                pass

            except Exception as e:
                msg = f"“检查目标仓位”报错：{e}"
                self.send_ding_talk(msg)
    
    # 更新交易员分类的pnl结果
    def update_pnl_result_on_trader(self):
        try:
            # 根据交易员分类的合约目标双向持仓数据
            target_trader_symbol_absolute_pos_dict = {}

            for trader, setting in self.portfolio.copy_setting.items():
                trader_name = setting.get("trader", "")
                if not trader_name:
                    continue

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
                    target_long_volume = long_volume * trade_value / copy_value
                    target_short_volume = short_volume * trade_value / copy_value

                    # 转换合约
                    pure_symbol = symbol.split("-")[0]

                    # 墙头草跟单山寨币仓位加倍
                    if (trader == "D5E7A8430A35CA84") and (pure_symbol not in ["BTC", "ETH", "XRP"]):
                        target_long_volume *= 2
                        target_short_volume *= 2

                    vt_symbol = ""
                    if self.exchange == Exchange.OKX:
                        vt_symbol = f"{pure_symbol}-USDT-SWAP.{self.exchange.value}"
                    
                    elif self.exchange == Exchange.BINANCE:
                        if pure_symbol in ["PEPE", "SHIB", "XEC", "LUNC", "FLOKI", "BONK", "SATS"]:
                            vt_symbol = f"1000{pure_symbol}USDT.{self.exchange.value}"
                            target_long_volume = target_long_volume / 1000
                            target_short_volume = target_short_volume / 1000
                            
                        else:
                            vt_symbol = f"{pure_symbol}USDT.{self.exchange.value}"

                    # 持仓统计
                    if vt_symbol:
                        symbol_pos_data = target_trader_symbol_absolute_pos_dict.get(trader_name, {})
                        pos_data = symbol_pos_data.get(vt_symbol, {})
                        pos_data["long_volume"] = pos_data.get("long_volume", 0) + target_long_volume
                        pos_data["short_volume"] = pos_data.get("short_volume", 0) + target_short_volume
                        symbol_pos_data[vt_symbol] = pos_data
                        target_trader_symbol_absolute_pos_dict[trader_name] = symbol_pos_data

            # 历史持仓数据填补
            for trader_name, symbol_pos_data in self.target_trader_symbol_absolute_pos_dict.items():
                for vt_symbol, real_pos_data in symbol_pos_data.items():
                    symbol_pos_data = target_trader_symbol_absolute_pos_dict.get(trader_name, {})
                    pos_data = symbol_pos_data.get(vt_symbol, {})

                    symbol_pos_data[vt_symbol] = pos_data
                    target_trader_symbol_absolute_pos_dict[trader_name] = symbol_pos_data

            # 计算PNL
            oms_engine = self.cta_engine.main_engine.engines["oms"]
            for trader_name, symbol_pos_data in target_trader_symbol_absolute_pos_dict.items():
                new_pnl = False

                for vt_symbol, pos_data in symbol_pos_data.items():
                    contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                    if contract:
                        pos_data["long_volume"] = round_to(pos_data.get("long_volume", 0), contract.min_volume)
                        pos_data["short_volume"] = round_to(pos_data.get("short_volume", 0), contract.min_volume)
                    
                    long_volume = pos_data.get("long_volume", 0)
                    short_volume = pos_data.get("short_volume", 0)

                    real_symbol_pos_data = self.target_trader_symbol_absolute_pos_dict.get(trader_name, {})
                    real_pos_data = real_symbol_pos_data.get(vt_symbol, {})
                    real_long_volume = real_pos_data.get("long_volume", 0)
                    real_long_price = real_pos_data.get("long_price", 0)
                    real_short_volume = real_pos_data.get("short_volume", 0)
                    real_short_price = real_pos_data.get("short_price", 0)

                    long_trade = round_to(long_volume - real_long_volume, contract.min_volume)
                    short_trade = round_to(short_volume - real_short_volume, contract.min_volume)
                    
                    # 判断开平仓，开仓更新平均开仓价格，平仓记录PNL并保存文件
                    tick = oms_engine.ticks.get(vt_symbol, None)
                    if long_trade > 0 and tick:
                        # 多头开仓
                        long_value = abs(real_long_volume*real_long_price) + abs(tick.last_price * long_trade)
                        long_price = long_value / abs(long_volume)
                        real_pos_data["long_volume"] = long_volume
                        real_pos_data["long_price"] = long_price

                    if long_trade < 0:
                        # 多头平仓
                        real_pos_data["long_volume"] = long_volume

                        open = real_long_price
                        close = tick.last_price
                        pnl = (close - open) * abs(long_trade)
                        pnl_data = {"time":datetime.now().strftime(f"%Y-%m-%d %H:%M:%S"),
                                    "vt_symbol":vt_symbol,
                                    "offset":"close_long",
                                    "open":open,
                                    "close":close,
                                    "volume":abs(long_trade),
                                    "pnl":pnl}
                        
                        pnl_data_list = self.trader_pnl_data_dict.get(trader_name, [])
                        pnl_data_list.append(pnl_data)
                        self.trader_pnl_data_dict[trader_name] = pnl_data_list
                        new_pnl = True
                    
                    if short_trade > 0 and tick:
                        # 空头开仓
                        short_value = abs(real_short_volume*real_short_price) + abs(tick.last_price * short_trade)
                        short_price = short_value / abs(short_volume)
                        real_pos_data["short_volume"] = short_volume
                        real_pos_data["short_price"] = short_price

                    if short_trade < 0:
                        # 空头平仓
                        real_pos_data["short_volume"] = short_volume

                        open = real_short_price
                        close = tick.last_price
                        pnl = (close - open) * abs(short_trade) * -1
                        pnl_data = {"time":datetime.now().strftime(f"%Y-%m-%d %H:%M:%S"),
                                    "vt_symbol":vt_symbol,
                                    "offset":"close_short",
                                    "open":open,
                                    "close":close,
                                    "volume":abs(short_trade),
                                    "pnl":pnl}
                        
                        pnl_data_list = self.trader_pnl_data_dict.get(trader_name, [])
                        pnl_data_list.append(pnl_data)
                        self.trader_pnl_data_dict[trader_name] = pnl_data_list
                        new_pnl = True

                    # 剔除空的数据，并且保存
                    if not real_pos_data.get("long_volume", 0):
                        if "long_volume" in real_pos_data:
                            real_pos_data.pop("long_volume")
                        
                        if "long_price" in real_pos_data:
                            real_pos_data.pop("long_price")

                    if not real_pos_data.get("short_volume", 0):
                        if "short_volume" in real_pos_data:
                            real_pos_data.pop("short_volume")
                        
                        if "short_price" in real_pos_data:
                            real_pos_data.pop("short_price")

                    if real_pos_data:
                        real_symbol_pos_data[vt_symbol] = real_pos_data

                    elif vt_symbol in real_symbol_pos_data:
                        real_symbol_pos_data.pop(vt_symbol)

                    if real_symbol_pos_data:
                        self.target_trader_symbol_absolute_pos_dict[trader_name] = real_symbol_pos_data
                    
                    elif trader_name in self.target_trader_symbol_absolute_pos_dict:
                        self.target_trader_symbol_absolute_pos_dict.pop(trader_name)

                if new_pnl:
                    # pnl数据写入文件
                    dir = os.getcwd()
                    dir_path = Path(dir).joinpath(f"BaiduSyncdisk{DIR_SYMBOL}PNL_{self.strategy_name}{DIR_SYMBOL}")
                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path)
                    file_path = dir_path.joinpath(f"{trader_name}.csv")

                    pnl_data_list = self.trader_pnl_data_dict.get(trader_name, [])
                    df_sorted = pd.DataFrame(pnl_data_list)
                    df_sorted = df_sorted.sort_values("time", ascending=False)
                    df_sorted.to_csv(file_path, index=False)

        except Exception as e:
            msg = f"“更新交易员分类的PNL结果”报错：{e}"
            self.send_ding_talk(msg)

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
                    for trader, symbol_pos_data in self.trader_symbol_pos_dict.items():
                        for vt_symbol, pos_data in symbol_pos_data.items():
                            for direction, data in pos_data.items():
                                pos = data["volume"]
                                if direction == "short":
                                    pos *= -1

                                tick = oms_engine.ticks.get(vt_symbol, None)
                                if tick:
                                    long_close_price = tick.last_price * 1.01
                                    short_close_price = tick.last_price * 0.99
                                    if pos > 0:
                                        # 平多
                                        self.send_symbol_order(trader, vt_symbol, Direction.SHORT, Offset.CLOSE, short_close_price, abs(pos))

                                    elif pos < 0:
                                        # 平空
                                        self.send_symbol_order(trader, vt_symbol, Direction.LONG, Offset.CLOSE, long_close_price, abs(pos))
                    
                    # 停止策略，发出通知
                    self.trading = False
                    msg = f"\n投资组合当前亏损：{position_pnl_rate}\n最大亏损限制：{self.trade_stop_loss}\n已强制清仓，停止策略"
                    self.send_ding_talk(msg)
                        
                self.position_pnl_rate = f"{round(position_pnl_rate * 100, 2)}%"
                
            except Exception as e:
                print(f"calculate_pnl 报错：{e}")

            sleep(1)

    def send_symbol_order(self, trader, symbol, direction, offset, price, volume, stop=False):
        contract = self.cta_engine.main_engine.get_contract(symbol)
        volume = round_to(abs(volume), contract.min_volume)
        if not volume or not price:
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

                if "LTC" in symbol and order_value <= 20:
                    value_cross = False

                if order_value <= 5:
                    value_cross = False
                
                if not value_cross:
                    self.send_ding_talk(f"开仓订单价值未满足要求\n合约：{symbol}\n价格：{tick.last_price}\n数量：{volume}\n价值：{order_value}")
                    return
        
        vt_orderids = super().send_symbol_order(symbol, direction, offset, price, volume, stop)
        for orderid in vt_orderids:
            self.orderid_trader_dict[orderid] = trader

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
