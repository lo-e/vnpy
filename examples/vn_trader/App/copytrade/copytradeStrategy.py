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

class CopytradeStrategy(CtaTemplate):
    """ 跟单交易策略 """

    className = "CopytradeStrategy"
    author = "loe"

    # 参数列表，保存了参数的名称
    parameters = [
        "strategy_name",
        "vt_symbol",
    ]

    # 变量列表，保存了变量的名称
    variables = [
        "symbol_pos_dict",
        "target_symbol_pos_dict"
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
        "symbol_pos_dict",
        "target_symbol_pos_dict"
    ]

    def __init__(self, ctaEngine, setting):
        self.symbol_pos_dict = {} # 合约持仓字典
        self.target_symbol_pos_dict = {} #  合约目标持仓字典
        self.wait_tick_symbols = set() # 等待行情数据的合约集合

        # 跟单设置
        self.copy_setting = {}

        # 完成setting.json参数的配置
        super(CopytradeStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

    def on_init(self):
        # 订阅交易所仓位更新
        self.cta_engine.event_engine.register(EVENT_MAINENGINE_POSITION_UPDATED, self.on_mainengine_position_updated)

        # 导入跟单设置
        dir_path = Path(os.path.dirname(os.path.realpath(__file__)))
        file_path = dir_path.joinpath("setting.json")
        setting = load_json_path(file_path)
        self.copy_setting = setting.get("copy_setting", {})

        # 订阅合约
        for vt_symbol in setting.get("vt_symbols", []):
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

        # oms_engine = self.cta_engine.main_engine.engines["oms"]
        # all_contracts = oms_engine.get_all_contracts()
        # for contract in all_contracts:
        #     req = SubscribeRequest(
        #         symbol=contract.symbol, exchange=contract.exchange
        #     )
        #     self.cta_engine.main_engine.subscribe(req, contract.gateway_name)

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
            if copy_assets and trade_assets:
                # 计算目标持仓
                target_pos = position.volume * trade_assets / copy_assets
                if position.direction == Direction.SHORT:
                    target_pos = abs(target_pos) * -1

                # 转换合约
                pure_symbol = position.symbol.split("-")[0]
                if pure_symbol in ["PEPE", "SHIB", "XEC", "LUNC", "FLOKI", "BONK"]:
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

    def wait_symbol_tick(self, vt_symbol:str):
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        while True:
            for vt_symbol in list(self.wait_tick_symbols):
                tick = oms_engine.ticks.get(vt_symbol, None)
                if tick:
                    self.on_mainengine_position_updated(event=None)
                    self.wait_tick_symbols.remove(vt_symbol)
            sleep(0.1)

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
