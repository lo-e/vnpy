""""""

import importlib
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
    ContractData
)
from vnpy.trader.event import (
    EVENT_TICK, 
    EVENT_ORDER, 
    EVENT_TRADE,
    EVENT_POSITION
)
from vnpy.trader.constant import (
    Direction, 
    OrderType, 
    Interval, 
    Exchange, 
    Offset, 
    Status
)
from vnpy.trader.utility import load_json, load_json_path, save_json, extract_vt_symbol, round_to
from vnpy.trader.database import database_manager

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
    TURTLE_PORTFOLIO_DB_NAME
)
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.app.cta_strategy.converter import OffsetConverter

""" modify by loe for Turtle """
from .turtlePortfolio import TurtlePortfolio
import re
from collections import OrderedDict
from time import sleep
from .dataservice import TurtleCryptoDataDownloading

STOP_STATUS_MAP = {
    Status.SUBMITTING: StopOrderStatus.WAITING,
    Status.NOTTRADED: StopOrderStatus.WAITING,
    Status.PARTTRADED: StopOrderStatus.TRIGGERED,
    Status.ALLTRADED: StopOrderStatus.TRIGGERED,
    Status.CANCELLED: StopOrderStatus.CANCELLED,
    Status.REJECTED: StopOrderStatus.CANCELLED
}

from vnpy.app.cta_strategy.base import (TICK_DB_NAME,
                                        DAILY_DB_NAME,
                                        MINUTE_DB_NAME)


class TurtleEngine(BaseEngine):
    """"""

    engine_type = EngineType.LIVE  # live trading engine

    setting_filename = 'TURTLE_setting.json'

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(TurtleEngine, self).__init__(
            main_engine, event_engine, APP_NAME)

        self.classes = {}           # class_name: stategy_class
        self.strategies = {}        # strategy_name: strategy

        self.symbol_strategy_map = defaultdict(
            list)                   # vt_symbol: strategy list
        self.orderid_strategy_map = {}  # vt_orderid: strategy
        self.strategy_orderid_map = defaultdict(
            set)                    # strategy_name: orderid list

        self.stop_order_count = 0   # for generating stop_orderid
        self.stop_orders = {}       # stop_orderid: stop_order

        self.init_thread = None
        self.init_queue = Queue()

        self.vt_tradeids = set()    # for filtering duplicate trade

        self.offset_converter = OffsetConverter(self.main_engine)

        """ modify by loe for Turtle """
        # 当前日期
        self.today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # 组合管理类
        self.turtlePortfolio = None
        # 数据引擎
        self.autoEngine = TurtleCryptoAutoEngine(main_engine=self.main_engine, turtle_engine=self, download_time='7:20', check_interval=5 * 60, reload_time=6, generate_time='8:00:01')

    def init_engine(self):
        """
        """
        self.load_strategy_class()
        self.load_strategy_setting()
        self.register_event()

        """ modify by loe """
        # 数据引擎启动
        self.autoEngine.start()

        self.write_log("海归策略引擎初始化成功")

    def close(self):
        """"""
        self.stop_all_strategies()

    def register_event(self):
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)

    def process_tick_event(self, event: Event):
        """"""
        tick = event.data

        strategies = self.symbol_strategy_map[tick.vt_symbol]
        if not strategies:
            return

        self.check_stop_order(tick)

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_tick, tick)

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
        """"""
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
            strategy.pos += trade.volume
        else:
            strategy.pos -= trade.volume

        self.call_strategy_func(strategy, strategy.on_trade, trade)
        self.put_strategy_event(strategy)

    def process_position_event(self, event: Event):
        """"""
        position = event.data

        self.offset_converter.update_position(position)

    def check_stop_order(self, tick: TickData):
        """"""
        for stop_order in list(self.stop_orders.values()):
            if stop_order.vt_symbol != tick.vt_symbol:
                continue

            long_triggered = (
                stop_order.direction == Direction.LONG and tick.last_price >= stop_order.price
            )
            short_triggered = (
                stop_order.direction == Direction.SHORT and tick.last_price <= stop_order.price
            )

            if long_triggered or short_triggered:
                strategy = self.strategies[stop_order.strategy_name]

                # To get excuted immediately after stop order is
                # triggered, use limit price if available, otherwise
                # use ask_price_5 or bid_price_5
                if stop_order.direction == Direction.LONG:
                    if tick.limit_up:
                        price = tick.limit_up
                    else:
                        price = tick.ask_price_5
                else:
                    if tick.limit_down:
                        price = tick.limit_down
                    else:
                        price = tick.bid_price_5
                
                contract = self.main_engine.get_contract(stop_order.vt_symbol)

                vt_orderids = self.send_limit_order(
                    strategy, 
                    contract,
                    stop_order.direction, 
                    stop_order.offset, 
                    price, 
                    stop_order.volume,
                    stop_order.lock
                )

                # Update stop order status if placed successfully
                if vt_orderids:
                    # Remove from relation map.
                    self.stop_orders.pop(stop_order.stop_orderid)

                    strategy_vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
                    if stop_order.stop_orderid in strategy_vt_orderids:
                        strategy_vt_orderids.remove(stop_order.stop_orderid)

                    # Change stop order status to cancelled and update to strategy.
                    stop_order.status = StopOrderStatus.TRIGGERED
                    stop_order.vt_orderids = vt_orderids

                    self.call_strategy_func(
                        strategy, strategy.on_stop_order, stop_order
                    )
                    self.put_stop_order_event(stop_order)

    def send_server_order(
        self,
        strategy: CtaTemplate,
        contract: ContractData,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        type: OrderType,
        lock: bool
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
            vt_orderid = self.main_engine.send_order(
                req, contract.gateway_name)
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
        lock: bool
    ):
        """
        Send a limit order to server.
        """
        return self.send_server_order(
            strategy,
            contract,
            direction,
            offset,
            price,
            volume,
            OrderType.LIMIT,
            lock
        )
    
    def send_server_stop_order(
        self,
        strategy: CtaTemplate,
        contract: ContractData,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool
    ):
        """
        Send a stop order to server.
        
        Should only be used if stop order supported 
        on the trading server.
        """
        return self.send_server_order(
            strategy,
            contract,
            direction,
            offset,
            price,
            volume,
            OrderType.STOP,
            lock
        )

    def send_local_stop_order(
        self,
        strategy: CtaTemplate,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool
    ):
        """
        Create a new local stop order.
        """
        self.stop_order_count += 1
        stop_orderid = f"{STOPORDER_PREFIX}.{self.stop_order_count}"

        stop_order = StopOrder(
            vt_symbol=strategy.vt_symbol,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            stop_orderid=stop_orderid,
            strategy_name=strategy.strategy_name,
            lock=lock
        )

        self.stop_orders[stop_orderid] = stop_order

        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        vt_orderids.add(stop_orderid)

        self.call_strategy_func(strategy, strategy.on_stop_order, stop_order)
        self.put_stop_order_event(stop_order)

        return stop_orderid

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

    def cancel_local_stop_order(self, strategy: CtaTemplate, stop_orderid: str):
        """
        Cancel a local stop order.
        """
        stop_order = self.stop_orders.get(stop_orderid, None)
        if not stop_order:
            return
        strategy = self.strategies[stop_order.strategy_name]

        # Remove from relation map.
        self.stop_orders.pop(stop_orderid)

        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        if stop_orderid in vt_orderids:
            vt_orderids.remove(stop_orderid)

        # Change stop order status to cancelled and update to strategy.
        stop_order.status = StopOrderStatus.CANCELLED

        self.call_strategy_func(strategy, strategy.on_stop_order, stop_order)
        self.put_stop_order_event(stop_order)

    def send_order(
        self,
        strategy: CtaTemplate,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        stop: bool,
        lock: bool
    ):
        """
        """
        contract = self.main_engine.get_contract(strategy.vt_symbol)
        if not contract:
            self.write_log(f"委托失败，找不到合约：{strategy.vt_symbol}", strategy)
            return ""
        
        # Round order price and volume to nearest incremental value
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)
        
        if stop:
            if contract.stop_supported:
                return self.send_server_stop_order(strategy, contract, direction, offset, price, volume, lock)
            else:
                return self.send_local_stop_order(strategy, direction, offset, price, volume, lock)
        else:
            return self.send_limit_order(strategy, contract, direction, offset, price, volume, lock)

    def send_symbol_order(
            self,
            strategy: CtaTemplate,
            vt_symbol:str,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            stop: bool,
            lock: bool
    ):
        """
        """
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f"委托失败，找不到合约：{vt_symbol}", strategy)
            return ""

        # Round order price and volume to nearest incremental value
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)

        if stop:
            if contract.stop_supported:
                return self.send_server_stop_order(strategy, contract, direction, offset, price, volume, lock)
            else:
                return self.send_local_stop_order(strategy, direction, offset, price, volume, lock)
        else:
            return self.send_limit_order(strategy, contract, direction, offset, price, volume, lock)

    def cancel_order(self, strategy: CtaTemplate, vt_orderid: str):
        """
        """
        if vt_orderid.startswith(STOPORDER_PREFIX):
            self.cancel_local_stop_order(strategy, vt_orderid)
        else:
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

    def load_tick(
        self, 
        vt_symbol: str,
        days: int,
        callback: Callable[[TickData], None]
    ):
        """"""
        symbol, exchange = extract_vt_symbol(vt_symbol)
        end = datetime.now()
        start = end - timedelta(days)

        ticks = database_manager.load_tick_data(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=end,
        )

        for tick in ticks:
            callback(tick)

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

            self.write_log(f"{strategy_name}开始执行初始化")

            # Call on_init function of strategy
            self.call_strategy_func(strategy, strategy.on_init)

            # Subscribe market data
            contract = self.main_engine.get_contract(strategy.vt_symbol)
            if contract:
                req = SubscribeRequest(
                    symbol=contract.symbol, exchange=contract.exchange)
                self.main_engine.subscribe(req, contract.gateway_name)
            else:
                self.write_log(f"行情订阅失败，找不到合约{strategy.vt_symbol}", strategy)

            """ modify by loe """
            # 订阅last_symbol行情
            if strategy.last_symbol:
                contract = self.main_engine.get_contract(strategy.last_symbol)
                if contract:
                    req = SubscribeRequest(
                        symbol=contract.symbol, exchange=contract.exchange)
                    self.main_engine.subscribe(req, contract.gateway_name)
                else:
                    self.write_log(f"行情订阅失败，找不到合约{strategy.last_symbol}", strategy)

            # Put event to update init completed status.
            strategy.inited = True
            self.put_strategy_event(strategy)
            self.write_log(f"{strategy_name}初始化完成")
        
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

    def load_strategy_class(self):
        """
        Load strategy class from source code.
        """
        path1 = Path(__file__).parent.joinpath("strategies")
        self.load_strategy_class_from_folder(
            path1, "App.Turtle_crypto.strategies")

    def load_strategy_class_from_folder(self, path: Path, module_name: str = ""):
        """
        Load strategy class from certain folder.
        """
        for dirpath, dirnames, filenames in os.walk(str(path)):
            for filename in filenames:
                if filename.endswith(".py"):
                    strategy_module_name = ".".join(
                        [module_name, filename.replace(".py", "")])
                    self.load_strategy_class_from_module(strategy_module_name)

    def load_strategy_class_from_module(self, module_name: str):
        """
        Load strategy class from module file.
        """
        try:
            module = importlib.import_module(module_name)

            for name in dir(module):
                value = getattr(module, name)
                if (isinstance(value, type) and issubclass(value, CtaTemplate) and value is not CtaTemplate):
                    self.classes[value.__name__] = value
        except:  # noqa
            msg = f"策略文件{module_name}加载失败，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def get_all_strategy_class_names(self):
        """
        Return names of strategy classes loaded.
        """
        return list(self.classes.keys())

    def init_all_strategies(self):
        """
        """
        for strategy_name in self.strategies.keys():
            self.init_strategy(strategy_name)

    def start_all_strategies(self):
        """
        """
        for strategy_name in self.strategies.keys():
            self.start_strategy(strategy_name)

    def stop_all_strategies(self):
        """
        """
        for strategy_name in self.strategies.keys():
            self.stop_strategy(strategy_name)

    def put_stop_order_event(self, stop_order: StopOrder):
        """
        Put an event to update stop order status.
        """
        event = Event(EVENT_CTA_STOPORDER, stop_order)
        self.event_engine.put(event)

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
        print(f'{log.time}\t{log.gateway_name}\t{log.msg}')


    def send_email(self, msg: str, strategy: CtaTemplate = None):
        """
        Send email to default receiver.
        """
        if strategy:
            subject = f"{strategy.strategy_name}"
        else:
            subject = "CTA策略引擎"

        self.main_engine.send_email(subject, msg)

    """ modify by loe for Turtle """
    def load_bar(self, vt_symbol, days, interval, callback):
        if interval == Interval.DAILY:
            dbName = DAILY_DB_NAME
        elif interval == Interval.MINUTE:
            dbName = MINUTE_DB_NAME
        else:
            dbName = TICK_DB_NAME

        startDate = self.today - timedelta(days)
        d = {'datetime': {'$gte': startDate}}
        collectionName = vt_symbol.upper()
        barData = self.main_engine.dbQuery(dbName, collectionName, d, 'datetime')

        l = []
        for d in barData:
            gateway_name = d['gateway_name']
            symbol = d['symbol']
            exchange = Exchange.RQ
            theDatetime = d['datetime']
            endDatetime = None

            bar = BarData(gateway_name=gateway_name, symbol=symbol, exchange=exchange, datetime=theDatetime,
                          endDatetime=endDatetime)
            bar.__dict__ = d
            l.append(bar)
        return l

    def load_strategy_setting(self):
        """
        Load setting file.
        """
        dir = os.path.dirname(os.path.realpath(__file__))
        file_path = Path(dir)
        file_path = file_path.joinpath(self.setting_filename)
        l = load_json_path(file_path)

        folioSetting = l.get('portfolio', None)
        self.turtlePortfolio = TurtlePortfolio(self, folioSetting)
        # 加载数据库组合数据
        self.loadPortfolioSyncData()
        self.savePortfolioSyncData()

        # 加载海归策略
        signalList = l.get('signal', None)
        for setting in signalList:
            self.add_strategy(setting)

    def add_strategy(self, setting):
        """
        Add a new strategy.
        """
        try:
            name = setting['strategy_name']
            class_name = setting['class_name']
            start = setting['start']
        except Exception:
            msg = traceback.format_exc()
            self.write_log(f'载入策略出错：{msg}')
            return

        if not start:
            return

        # 获取策略类
        strategy_class = self.classes.get(class_name, None)
        if not strategy_class:
            self.write_log(f'找不到策略类：{class_name}')
            return

        # 防止策略重名
        if name in self.strategies:
            self.write_log(f'策略实例重名：{name}')
            return

        # 创建策略实例
        strategy = strategy_class(self, self.turtlePortfolio, setting)
        # 加载同步数据
        self.loadSyncData(strategy)
        self.strategies[name] = strategy

        # Add vt_symbol to strategy map.
        strategies = self.symbol_strategy_map[strategy.vt_symbol]
        strategies.append(strategy)

        # 保存前主力Tick映射关系
        if strategy.last_symbol:
            strategies = self.symbol_strategy_map[strategy.last_symbol]
            strategies.append(strategy)

        self.put_strategy_event(strategy)

    def loadSyncData(self, strategy):
        """从数据库载入策略的持仓情况"""
        flt = {'strategy_name': strategy.strategy_name,
               'vt_symbol': strategy.vt_symbol}
        syncData = self.main_engine.dbQuery(POSITION_DB_NAME, strategy.__class__.__name__, flt)

        if not syncData:
            return

        d = syncData[0]

        for key in strategy.syncs:
            if key in d:
                strategy.__setattr__(key, d[key])

    def saveSyncData(self, strategy):
        """保存策略的持仓情况到数据库"""
        flt = {'strategy_name': strategy.strategy_name,
               'vt_symbol': strategy.vt_symbol}

        d = copy(flt)
        for key in strategy.syncs:
            d[key] = strategy.__getattribute__(key)

        self.main_engine.dbUpdate(POSITION_DB_NAME, strategy.__class__.__name__,
                                 d, flt, True)

        content = f'策略{strategy.strategy_name}同步数据保存成功，当前持仓{strategy.pos}'
        self.write_log(content)

    def savePortfolioSyncData(self):
        """保存组合变量到数据库"""
        d = {}
        for key in self.turtlePortfolio.syncList:
            d[key] = self.turtlePortfolio.__getattribute__(key)

        self.main_engine.dbUpdate(TURTLE_PORTFOLIO_DB_NAME, self.turtlePortfolio.name,
                                 d, {}, True)

        content = f'海龟组合{self.turtlePortfolio.name}\t数据保存成功'
        self.write_log(content)

        # ----------------------------------------------------------------------

    def loadPortfolioSyncData(self):
        """从数据库载入策略的持仓情况"""
        syncData = self.main_engine.dbQuery(TURTLE_PORTFOLIO_DB_NAME, self.turtlePortfolio.name, {})

        if not syncData:
            return

        d = syncData[0]

        for key in self.turtlePortfolio.syncList:
            if key in d:
                self.turtlePortfolio.__setattr__(key, d[key])

    def initPortfolio(self):
        """ 初始化海龟组合 """
        self.init_all_strategies()

    def startPortfolio(self):
        """ 启动海龟组合 """
        self.start_all_strategies()

    def stopPortfolio(self):
        """ 停止海龟组合 """
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
            self.write_log(f'策略实例不存在：{strategy_name}')
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
            self.write_log(f'策略实例不存在：{strategy_name}')
            return None

    def get_portfolio_variables(self):
        varDict = OrderedDict()

        for key in self.turtlePortfolio.varList:
            varDict[key] = self.turtlePortfolio.__getattribute__(key)

        return varDict

    def get_portfolio_parameters(self):
        """获取策略的参数字典"""
        paramDict = OrderedDict()

        for key in self.turtlePortfolio.paramList:
            paramDict[key] = self.turtlePortfolio.__getattribute__(key)

        return paramDict

    def get_strategy_names(self):
        """查询所有策略名称"""
        return self.strategies.keys()

    def getPriceTick(self, strategy):
        """获取最小价格变动"""
        contract = self.main_engine.getContract(strategy.vt_symbol)
        if contract:
            return contract.priceTick
        return 0

    """ modify by loe """
    # 新的DailyBar更新后需要自动重新初始化策略
    def reinit_strategies(self):
        for strategy_name in self.strategies.keys():
            strategy = self.strategies[strategy_name]
            if strategy.inited:
                temp = strategy.trading
                strategy.trading = False
                self.call_strategy_func(strategy, strategy.on_init)
                strategy.trading = temp
                self.put_strategy_event(strategy)
                self.write_log(f"{strategy_name} 重新初始化完成")

""" modify by loe """
# 数据下载引擎，每天固定时间从1Token自动下载策略回测及实盘必要的数据，并自动结合订阅下载的数据合成DailyBar，策略自动重新初始化
class TurtleCryptoAutoEngine(object):

    def __init__(self, main_engine:MainEngine, turtle_engine:TurtleEngine, download_time:str, check_interval:int, reload_time:int, generate_time:str):
        # download_time:'7:20', check_interval:5*60, reload_time:6, generate_time:'8:00:01'
        super(TurtleCryptoAutoEngine, self).__init__()
        self.contract_list = ['okef/btc.usd.q', 'okef/eth.usd.q', 'okef/eos.usd.q']
        self.main_engine = main_engine
        self.turtle_engine = turtle_engine
        self.download_time = download_time
        self.check_interval = check_interval
        self.reload_time = reload_time
        self.generate_time = generate_time
        self.downloading = False
        self.generating = False
        self.download_timer = Thread(target=self.on_download_timer)
        self.generate_timer = Thread(target=self.on_generate_timer)

    def start(self):
        self.download_timer.start()
        self.generate_timer.start()

    def on_download_timer(self):
        while True:
            try:
                self.checkAndDownload()
            except:
                try:
                    self.main_engine.send_email(subject='TURTLE_Crypto 数据下载', content=f'【未知错误】\n\n{traceback.format_exc()}')
                except:
                    pass
            sleep(self.check_interval)

    def on_generate_timer(self):
        while True:
            try:
                self.checkAndGenerate()
            except:
                try:
                    self.main_engine.send_email(subject='TURTLE_Crypto 数据更新', content=f'【未知错误】\n\n{traceback.format_exc()}')
                except:
                    pass

    def checkAndDownload(self):
        now = datetime.now()
        start_time = datetime.strptime(f'{now.year}-{now.month}-{now.day} {self.download_time}', '%Y-%m-%d %H:%M')
        end_time = start_time + timedelta(seconds=self.check_interval * self.reload_time)
        if now >= start_time and now <= end_time:
            if not self.downloading:
                turtleCryptoDataD = TurtleCryptoDataDownloading()
                self.downloading = True
                turtleCryptoDataD.download(contract_list=self.contract_list)
                self.downloading = False

    def checkAndGenerate(self):
        now = datetime.now()
        start_time = datetime.strptime(f'{now.year}-{now.month}-{now.day} {self.generate_time}', '%Y-%m-%d %H:%M:%S')
        end_time = start_time + timedelta(seconds=10)
        if now >= start_time and now <= end_time:
            if not self.generating:
                self.generating = True
                turtleCryptoDataD = TurtleCryptoDataDownloading()
                result, complete_msg, back_msg, lost_msg = turtleCryptoDataD.generate(contract_list=self.contract_list)
                email_msg = complete_msg + '\n\n' + lost_msg + back_msg
                print('\n\n' + lost_msg + back_msg)
                try:
                    self.main_engine.send_email(subject='TURTLE_Crypto 数据更新', content=email_msg)
                except:
                    pass
                if result:
                    # 海龟策略重新初始化
                    self.turtle_engine.reinit_strategies()
        else:
            self.generating = False