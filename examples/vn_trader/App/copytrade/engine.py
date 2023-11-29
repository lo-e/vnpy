import importlib
from operator import sub
import os
import traceback
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable
from datetime import datetime, timedelta
from threading import Thread
from queue import Queue
from copy import copy
from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import (
    OrderRequest,
    SubscribeRequest,
    HistoryRequest,
    LogData,
    TickData,
    BarData,
    ContractData,
)
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_TIMER,
)
from vnpy.trader.constant import (
    Direction,
    OrderType,
    Interval,
    Exchange,
    Offset,
    Status,
)
from vnpy.trader.utility import (
    load_json,
    load_json_path,
    save_json,
    extract_vt_symbol,
    round_to,
)
from vnpy.trader.utility import DIR_SYMBOL

from .base import APP_NAME
from vnpy.app.cta_strategy.base import (
    EVENT_CTA_LOG,
    EVENT_CTA_STRATEGY,
    EVENT_CTA_STOPORDER,
    EngineType,
    StopOrder,
    StopOrderStatus,
    STOPORDER_PREFIX,
    POSITION_DB_NAME,
    PORTFOLIO_DB_NAME,
)
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.converter import OffsetConverter
import re
from collections import OrderedDict
from time import sleep
from decimal import Decimal
from .copytradeStrategy import CopytradeStrategy
import json

STOP_STATUS_MAP = {
    Status.SUBMITTING: StopOrderStatus.WAITING,
    Status.NOTTRADED: StopOrderStatus.WAITING,
    Status.PARTTRADED: StopOrderStatus.TRIGGERED,
    Status.ALLTRADED: StopOrderStatus.TRIGGERED,
    Status.CANCELLED: StopOrderStatus.CANCELLED,
    Status.REJECTED: StopOrderStatus.CANCELLED,
}

from vnpy.app.cta_strategy.base import (
    TICK_DB_NAME,
    DAILY_DB_NAME,
    MINUTE_DB_NAME,
    MinuteDataBaseName,
)

class CopytradeEngine(BaseEngine):
    engine_type = EngineType.LIVE  # live trading engine

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(CopytradeEngine, self).__init__(main_engine, event_engine, APP_NAME)

        self.strategies = {}  # strategy_name: strategy

        self.orderid_strategy_map = {}  # vt_orderid: strategy
        self.strategy_orderid_map = defaultdict(set)  # strategy_name: orderid list

        self.init_thread = None
        self.init_queue = Queue()

        self.vt_tradeids = set()  # for filtering duplicate trade

        self.offset_converter = OffsetConverter(self.main_engine)

    def init_engine(self):
        setting = {"strategy_name": "COPYTRADE", "start": True}
        self.add_strategy(setting)
        self.register_event()
        self.write_log("跟单交易引擎初始化成功")

    def close(self):
        """"""
        self.stop_all_strategies()

    def register_event(self):
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event: Event):
        # 策略推送
        for strategy in self.strategies.values():
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_timer)

    def process_tick_event(self, event: Event):
        """"""
        pass

    def process_order_event(self, event: Event):
        """"""
        order = event.data

        self.offset_converter.update_order(order)

        strategy = self.orderid_strategy_map.get(order.vt_orderid, None)
        if not strategy:
            return

        # Remove vt_orderid if order is no longer active.
        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        if order.vt_orderid in vt_orderids and not order.is_active():
            vt_orderids.remove(order.vt_orderid)

        # For server stop order, call strategy on_stop_order function
        if order.type == OrderType.STOP:
            so = StopOrder(
                vt_symbol=order.vt_symbol,
                direction=order.direction,
                offset=order.offset,
                price=order.price,
                volume=order.volume,
                stop_orderid=order.vt_orderid,
                strategy_name=strategy.strategy_name,
                status=STOP_STATUS_MAP[order.status],
                vt_orderids=[order.vt_orderid],
            )
            self.call_strategy_func(strategy, strategy.on_stop_order, so)

        # Call strategy on_order function
        self.call_strategy_func(strategy, strategy.on_order, order)

    def process_trade_event(self, event: Event):
        trade = event.data

        # Filter duplicate trade push
        if trade.vt_tradeid in self.vt_tradeids:
            return
        self.vt_tradeids.add(trade.vt_tradeid)

        self.offset_converter.update_trade(trade)

        strategy = self.orderid_strategy_map.get(trade.vt_orderid, None)
        if not strategy:
            return

        if trade.direction == Direction.LONG:
            strategy.symbol_pos_dict[trade.vt_symbol] = float(
                Decimal(str(strategy.symbol_pos_dict.get(trade.vt_symbol, 0))) + Decimal(str(trade.volume))
            )

        else:
            strategy.symbol_pos_dict[trade.vt_symbol] = float(
                Decimal(str(strategy.symbol_pos_dict.get(trade.vt_symbol, 0))) - Decimal(str(trade.volume))
            )

        self.call_strategy_func(strategy, strategy.on_trade, trade)
        self.put_strategy_event(strategy)

    def process_position_event(self, event: Event):
        position = event.data
        self.offset_converter.update_position(position)

    def send_server_order(
        self,
        strategy: CtaTemplate,
        contract: ContractData,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        type: OrderType,
        lock: bool,
    ):
        """
        Send a new order to server.
        """
        # Create request and send order.
        original_req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            offset=offset,
            type=type,
            price=price,
            volume=volume,
        )

        # Convert with offset converter
        req_list = self.offset_converter.convert_order_request(original_req, lock)

        # Send Orders
        vt_orderids = []

        for req in req_list:
            vt_orderid = self.main_engine.send_order(req, contract.gateway_name)
            vt_orderids.append(vt_orderid)

            self.offset_converter.update_order_request(req, vt_orderid)

            # Save relationship between orderid and strategy.
            self.orderid_strategy_map[vt_orderid] = strategy
            self.strategy_orderid_map[strategy.strategy_name].add(vt_orderid)

        return vt_orderids

    def send_limit_order(
        self,
        strategy: CtaTemplate,
        contract: ContractData,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool,
    ):
        """
        Send a limit order to server.
        """
        return self.send_server_order(
            strategy, contract, direction, offset, price, volume, OrderType.LIMIT, lock
        )

    def cancel_server_order(self, strategy: CtaTemplate, vt_orderid: str):
        """
        Cancel existing order by vt_orderid.
        """
        order = self.main_engine.get_order(vt_orderid)
        if not order:
            self.write_log(f"撤单失败，找不到委托{vt_orderid}", strategy)
            return

        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def send_order(
        self,
        strategy: CtaTemplate,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        stop: bool,
        lock: bool,
    ):
        """ """
        contract = self.main_engine.get_contract(strategy.vt_symbol)
        if not contract:
            self.write_log(f"委托失败，找不到合约：{strategy.vt_symbol}", strategy)
            return ""

        # Round order price and volume to nearest incremental value
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)

        return self.send_limit_order(
            strategy, contract, direction, offset, price, volume, lock
        )

    def send_symbol_order(
        self,
        strategy: CtaTemplate,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        stop: bool,
        lock: bool,
    ):
        """ """
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f"委托失败，找不到合约：{vt_symbol}", strategy)
            return ""

        # Round order price and volume to nearest incremental value
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)

        return self.send_limit_order(
            strategy, contract, direction, offset, price, volume, lock
        )

    def cancel_order(self, strategy: CtaTemplate, vt_orderid: str):
        """ """
        self.cancel_server_order(strategy, vt_orderid)

    def cancel_all(self, strategy: CtaTemplate):
        """
        Cancel all active orders of a strategy.
        """
        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        if not vt_orderids:
            return

        for vt_orderid in copy(vt_orderids):
            self.cancel_order(strategy, vt_orderid)

    def get_engine_type(self):
        """"""
        return self.engine_type

    def call_strategy_func(
        self, strategy: CtaTemplate, func: Callable, params: Any = None
    ):
        """
        Call function of a strategy and catch any exception raised.
        """
        try:
            if params:
                func(params)
            else:
                func()
        except Exception:
            strategy.trading = False
            strategy.inited = False

            msg = f"触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg, strategy)

    def init_strategy(self, strategy_name: str):
        """
        Init a strategy.
        """
        self.init_queue.put(strategy_name)

        if not self.init_thread:
            self.init_thread = Thread(target=self._init_strategy)
            self.init_thread.start()

    def _init_strategy(self):
        """
        Init strategies in queue.
        """
        while not self.init_queue.empty():
            strategy_name = self.init_queue.get()
            strategy = self.strategies[strategy_name]

            if strategy.inited:
                self.write_log(f"{strategy_name}已经完成初始化，禁止重复操作")
                continue

            self.write_log(f"跟单交易策略{strategy_name}开始执行初始化")

            # Call on_init function of strategy
            self.call_strategy_func(strategy, strategy.on_init)

            # Subscribe market data
            for vt_symbol in strategy.symbol_pos_dict.keys():
                contract = self.main_engine.get_contract(vt_symbol)
                if contract:
                    req = SubscribeRequest(
                        symbol=contract.symbol, exchange=contract.exchange
                    )
                    self.main_engine.subscribe(req, contract.gateway_name)
                else:
                    self.write_log(f"行情订阅失败，找不到合约{strategy.vt_symbol}", strategy)

            # Put event to update init completed status.
            strategy.inited = True
            self.put_strategy_event(strategy)
            self.write_log(f"跟单交易策略{strategy_name}初始化完成")

        self.init_thread = None

    def start_strategy(self, strategy_name: str):
        """
        Start a strategy.
        """
        strategy = self.strategies[strategy_name]
        if not strategy.inited:
            self.write_log(f"策略{strategy.strategy_name}启动失败，请先初始化")
            return

        if strategy.trading:
            self.write_log(f"{strategy_name}已经启动，请勿重复操作")
            return

        self.call_strategy_func(strategy, strategy.on_start)
        strategy.trading = True
        self.put_strategy_event(strategy)
        self.write_log(f"跟单交易策略{strategy_name}启动")

    def stop_strategy(self, strategy_name: str):
        """
        Stop a strategy.
        """
        strategy = self.strategies[strategy_name]
        if not strategy.trading:
            return

        # Call on_stop function of the strategy
        self.call_strategy_func(strategy, strategy.on_stop)

        # Change trading status of strategy to False
        strategy.trading = False

        # Cancel all orders of the strategy
        self.cancel_all(strategy)

        # Update GUI
        self.put_strategy_event(strategy)

    def init_all_strategies(self):
        """ """
        for strategy_name in self.strategies.keys():
            self.init_strategy(strategy_name)

    def start_all_strategies(self):
        """ """
        for strategy_name in self.strategies.keys():
            self.start_strategy(strategy_name)

    def stop_all_strategies(self):
        """ """
        for strategy_name in self.strategies.keys():
            self.stop_strategy(strategy_name)

    def put_strategy_event(self, strategy: CtaTemplate):
        """
        Put an event to update strategy status.
        """
        # 保存strategy数据到数据库
        strategy_name = strategy.strategy_name
        if strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            self.saveSyncData(strategy)

        data = strategy.get_data()
        event1 = Event(EVENT_CTA_STRATEGY, data)
        self.event_engine.put(event1)

        event2 = Event(EVENT_CTA_STRATEGY + strategy_name, data)
        self.event_engine.put(event2)

    def write_log(self, msg: str, strategy: CtaTemplate = None):
        """
        Create cta engine log event.
        """
        if strategy:
            msg = f"{strategy.strategy_name}: {msg}"

        log = LogData(msg=msg, gateway_name="CtaStrategy")
        event = Event(type=EVENT_CTA_LOG, data=log)
        self.event_engine.put(event)

        # 输出日志内容
        print(f"{log.time}\t{log.gateway_name}\t{log.msg}")

    def send_email(self, msg: str, strategy: CtaTemplate = None, subject: str = ""):
        """
        Send email to default receiver.
        """
        if not subject:
            if strategy:
                subject = f"{strategy.strategy_name}"
            else:
                subject = "CTA策略引擎"

        self.main_engine.send_email(subject, msg)

    def send_dingtalk(self, msg: str, strategy: CtaTemplate = None):
        """
        Send dingtalk to default receiver.
        """
        if strategy:
            msg = f"{strategy.strategy_name}\n{msg}"

        self.main_engine.send_ding_talk(content=msg)

    def load_bar(self, vt_symbol, data_from, interval, window, callback):
        if interval == Interval.DAILY:
            dbName = DAILY_DB_NAME

        elif interval == Interval.MINUTE:
            dbName = MinuteDataBaseName(window)

        else:
            dbName = TICK_DB_NAME

        d = {"datetime": {"$gte": data_from}}
        collectionName = vt_symbol.upper()
        barData = self.main_engine.dbQuery(dbName, collectionName, d, "datetime")

        l = []
        for d in barData:
            bar = BarData(
                gateway_name="",
                symbol="",
                exchange=Exchange.BYBIT,
                datetime=None,
                endDatetime=None,
            )
            bar.__dict__ = d
            # 检查Bar数据是否有效
            if not bar.check_valid():
                raise ("Bar数据校验不通过！！")

            l.append(bar)
        return l

    def add_strategy(self, setting):
        """
        添加策略
        """
        try:
            name = setting["strategy_name"]
            start = setting["start"]
        except Exception:
            msg = traceback.format_exc()
            self.write_log(f"载入策略出错：{msg}")
            return

        if not start:
            return

        # 防止策略重名
        if name in self.strategies:
            self.write_log(f"策略实例重名：{name}")
            return

        # 创建策略实例
        strategy = CopytradeStrategy(self, setting)

        # 加载同步数据
        self.loadSyncData(strategy)
        self.strategies[name] = strategy

        # 发送事件
        self.put_strategy_event(strategy)

    def loadSyncData(self, strategy):
        """从数据库载入策略的持仓情况"""
        flt = {"strategy_name": strategy.strategy_name, "vt_symbol": strategy.vt_symbol}
        colleciton_name = f"{strategy.__class__.__name__}"
        syncData = self.main_engine.dbQuery(
            POSITION_DB_NAME, colleciton_name, flt
        )

        if not syncData:
            return

        d = syncData[0]

        for key in strategy.syncs:
            if key in d:
                strategy.__setattr__(key, d[key])

    def saveSyncData(self, strategy):
        """保存策略的持仓情况到数据库"""
        if not strategy.inited:
            return
        
        flt = {"strategy_name": strategy.strategy_name, "vt_symbol": strategy.vt_symbol}

        d = copy(flt)
        for key in strategy.syncs:
            d[key] = strategy.__getattribute__(key)

        # 保存到数据库
        colleciton_name = f"{strategy.__class__.__name__}"
        self.main_engine.dbUpdate(
            POSITION_DB_NAME,
            colleciton_name,
            d,
            flt,
            True,
            callback=self.strategyDbUpdateCallback,
        )

        # 保存到文件
        json_file = self.get_strategie_sync_file_path(strategy)
        try:
            with open(json_file, "w", encoding="utf-8") as file:
                file.write(
                    json.dumps(d, ensure_ascii=False)
                )
        except:
            pass
    
    def get_strategie_sync_file_path(self, strategy):
        dir = os.path.dirname(os.path.realpath(__file__))
        dir_path = Path(dir).joinpath(f"strategie_sync_data{DIR_SYMBOL}BaiduSyncdisk{DIR_SYMBOL}")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = dir_path.joinpath(f"{strategy.strategy_name}.json")
        return file_path
    
    def strategyDbUpdateCallback(self, back_data=None):
        try:
            if isinstance(back_data, dict):
                result = back_data.get("result", False)
                strategy_name = back_data.get("strategy_name", "")
                if result:
                    content = f"跟单交易策略{strategy_name}同步数据保存成功"
                else:
                    content = f"跟单交易策略{strategy_name}同步数据保存失败！！"
                    self.write_log(content)
            else:
                content = f"跟单交易策略同步数据保存失败！！"
                self.write_log(content)
        except:
            content = f"跟单交易策略同步数据保存失败！！"
            self.write_log(content)

    def initPortfolio(self):
        """初始化策略组合"""
        self.init_all_strategies()

    def startPortfolio(self):
        """启动策略组合"""
        self.start_all_strategies()

    def stopPortfolio(self):
        """停止策略组合"""
        self.stop_all_strategies()

    def get_strategy_parameters(self, strategy_name):
        """
        Get parameters of a strategy.
        """
        if strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            paramDict = OrderedDict()

            for key in strategy.parameters:
                paramDict[key] = strategy.__getattribute__(key)

            return paramDict
        else:
            self.write_log(f"策略实例不存在：{strategy_name}")
            return None

    def get_strategy_variables(self, strategy_name):
        """获取策略当前的变量字典"""
        if strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            varDict = OrderedDict()

            for key in strategy.variables:
                varDict[key] = strategy.__getattribute__(key)

            return varDict
        else:
            self.write_log(f"策略实例不存在：{strategy_name}")
            return None

    def get_strategy_names(self):
        """查询所有策略名称"""
        return self.strategies.keys()

    def getPriceTick(self, strategy):
        """获取最小价格变动"""
        contract = self.main_engine.getContract(strategy.vt_symbol)
        if contract:
            return contract.priceTick
        return 0