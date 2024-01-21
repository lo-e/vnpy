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
from time import sleep
from enum import Enum
from decimal import Decimal

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
        self.wait_tick_symbols = set() # 等待行情数据的合约集合
        self.position_pnl = 0 # 持仓盈亏
        self.position_pnl_rate = "" # 持仓盈亏占比（相对投资组合总资金）

        # 导入跟单设置
        self.copy_setting = setting.get("copy_setting", {})

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
        
        # 开启新线程等待行情数据
        t = Thread(target=self.wait_symbol_tick)
        t.start()

        # 开启新线程统计当前盈亏
        t = Thread(target=self.calculate_pnl)
        t.start()

        # 开启新线程获取交易员的当前带单
        t = Thread(target=self.fetch_copytrade_data)
        t.start()

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
            trade_assets = target_setting.get("trade_assets", 0)
            start = target_setting.get("start", False)
            if copy_assets and trade_assets and start:
                # 计算目标持仓
                target_pos = abs(position.volume * trade_assets / copy_assets)
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
        
        # 判断持仓变化
        self.check_target_pos(target_symbol_pos_dict)
        # print(f"\n")

    def check_target_pos(self, checking_data:dict):
        if not self.trading:
            return
        
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

    def fetch_copytrade_data(self):
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
                    ALvinnn111 '9B28742D954561AE'
                    """
                    trader_position_data = gateway.rest_api.query_copytrade(trader="9B28742D954561AE")
                    if isinstance(trader_position_data, list):
                        for d in trader_position_data:
                            symbol = d["instId"]
                            subPos = float(d["subPos"])
                            posSide = d["posSide"]

                            contract = self.cta_engine.main_engine.get_contract(f"{symbol}.OKX")
                            if contract:
                                pos = abs(contract.min_volume * subPos)
                                pos = round_to(pos, contract.min_volume)
                                if posSide == "short":
                                    pos = pos * -1
                                print(f"{symbol}\t{posSide}\t{pos}")
            
            except Exception as e:
                pass
            sleep(10)

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
