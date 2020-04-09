import traceback
import importlib
import os
from typing import List, Dict, Set, Callable, Any, Type
from collections import defaultdict
from copy import copy
from pathlib import Path
from datetime import datetime, timedelta

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import (
    EVENT_TICK, EVENT_POSITION, EVENT_CONTRACT,
    EVENT_ORDER, EVENT_TRADE, EVENT_TIMER
)
from vnpy.trader.utility import load_json, save_json
from vnpy.trader.object import (
    TickData, ContractData, LogData,
    SubscribeRequest, OrderRequest
)
from vnpy.trader.constant import (
    Direction, Offset, OrderType, Interval
)
from vnpy.trader.converter import OffsetConverter

from vnpy.app.spread_trading.base import (
    LegData, SpreadData,
    EVENT_SPREAD_DATA, EVENT_SPREAD_POS,
    EVENT_SPREAD_ALGO, EVENT_SPREAD_LOG,
    EVENT_SPREAD_STRATEGY,
    load_bar_data, load_tick_data
)

from vnpy.app.spread_trading.template import SpreadAlgoTemplate, SpreadStrategyTemplate
from .algo import SpreadTakerAlgo
from vnpy.app.cta_strategy.base import POSITION_DB_NAME

""" modify by loe """
from threading import Thread
from time import sleep
from ..Turtle.dataservice import TurtleDataDownloading

APP_NAME = "SpreadTrading"
class SpreadEngine(BaseEngine):
    """"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """Constructor"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.active = False

        self.data_engine: SpreadDataEngine = SpreadDataEngine(self)
        self.algo_engine: SpreadAlgoEngine = SpreadAlgoEngine(self)
        self.strategy_engine: SpreadStrategyEngine = SpreadStrategyEngine(self)

        self.add_spread = self.data_engine.add_spread
        self.remove_spread = self.data_engine.remove_spread
        self.get_spread = self.data_engine.get_spread
        self.get_all_spreads = self.data_engine.get_all_spreads

        self.start_algo = self.algo_engine.start_algo
        self.stop_algo = self.algo_engine.stop_algo
        self.manual_stop_algo = self.algo_engine.manual_stop_algo

    def start(self):
        """"""
        if self.active:
            return
        self.active = True

        self.data_engine.start()
        self.algo_engine.start()
        self.strategy_engine.start()

    def stop(self):
        """"""
        self.data_engine.stop()
        self.algo_engine.stop()
        self.strategy_engine.stop()

    def write_log(self, msg: str):
        """"""
        log = LogData(
            msg=msg,
            gateway_name=APP_NAME
        )
        event = Event(EVENT_SPREAD_LOG, log)
        self.event_engine.put(event)


class SpreadDataEngine:
    """"""
    setting_filename = "spread_trading_setting.json"

    def __init__(self, spread_engine: SpreadEngine):
        """"""
        self.spread_engine: SpreadEngine = spread_engine
        self.main_engine: MainEngine = spread_engine.main_engine
        self.event_engine: EventEngine = spread_engine.event_engine

        self.write_log = spread_engine.write_log

        self.legs: Dict[str, LegData] = {}          # vt_symbol: leg
        self.spreads: Dict[str, SpreadData] = {}    # name: spread
        self.symbol_spread_map: Dict[str, List[SpreadData]] = defaultdict(list)

    def start(self):
        """"""
        self.load_setting()
        self.register_event()

        self.write_log("价差数据引擎启动成功")

    def stop(self):
        """"""
        pass

    def load_setting(self) -> None:
        """"""
        setting = load_json(self.setting_filename)

        for spread_setting in setting:
            self.add_spread(
                spread_setting["name"],
                spread_setting["leg_settings"],
                spread_setting["active_symbol"],
                spread_setting.get("min_volume", 1),
                save=False
            )

    def save_setting(self) -> None:
        """"""
        setting = []

        for spread in self.spreads.values():
            leg_settings = []
            for leg in spread.legs.values():
                price_multiplier = spread.price_multipliers[leg.vt_symbol]
                trading_multiplier = spread.trading_multipliers[leg.vt_symbol]
                inverse_contract = spread.inverse_contracts[leg.vt_symbol]

                leg_setting = {
                    "vt_symbol": leg.vt_symbol,
                    "price_multiplier": price_multiplier,
                    "trading_multiplier": trading_multiplier,
                    "inverse_contract": inverse_contract
                }
                leg_settings.append(leg_setting)

            spread_setting = {
                "name": spread.name,
                "leg_settings": leg_settings,
                "active_symbol": spread.active_leg.vt_symbol,
                "min_volume": spread.min_volume
            }
            setting.append(spread_setting)

        save_json(self.setting_filename, setting)

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick = event.data

        leg = self.legs.get(tick.vt_symbol, None)
        if not leg:
            return
        leg.update_tick(tick)

        for spread in self.symbol_spread_map[tick.vt_symbol]:
            spread.calculate_price()
            self.put_data_event(spread)

    def process_position_event(self, event: Event) -> None:
        """"""
        position = event.data

        leg = self.legs.get(position.vt_symbol, None)
        if not leg:
            return
        leg.update_position(position)

        for spread in self.symbol_spread_map[position.vt_symbol]:
            spread.calculate_pos()
            self.put_pos_event(spread)

    def process_trade_event(self, event: Event) -> None:
        """"""
        trade = event.data

        leg = self.legs.get(trade.vt_symbol, None)
        if not leg:
            return
        leg.update_trade(trade)

        for spread in self.symbol_spread_map[trade.vt_symbol]:
            spread.calculate_pos()
            self.put_pos_event(spread)

    def process_contract_event(self, event: Event) -> None:
        """"""
        contract = event.data
        leg = self.legs.get(contract.vt_symbol, None)

        if leg:
            # Update contract data
            leg.update_contract(contract)

            req = SubscribeRequest(
                contract.symbol, contract.exchange
            )
            self.main_engine.subscribe(req, contract.gateway_name)

    def put_data_event(self, spread: SpreadData) -> None:
        """"""
        event = Event(EVENT_SPREAD_DATA, spread)
        self.event_engine.put(event)

    def put_pos_event(self, spread: SpreadData) -> None:
        """"""
        event = Event(EVENT_SPREAD_POS, spread)
        self.event_engine.put(event)

    def get_leg(self, vt_symbol: str) -> LegData:
        """"""
        leg = self.legs.get(vt_symbol, None)

        if not leg:
            leg = LegData(vt_symbol)
            self.legs[vt_symbol] = leg

            # Subscribe market data
            contract = self.main_engine.get_contract(vt_symbol)
            if contract:
                leg.update_contract(contract)

                req = SubscribeRequest(
                    contract.symbol,
                    contract.exchange
                )
                self.main_engine.subscribe(req, contract.gateway_name)

            # Initialize leg position
            for direction in Direction:
                vt_positionid = f"{vt_symbol}.{direction.value}"
                position = self.main_engine.get_position(vt_positionid)

                if position:
                    leg.update_position(position)

        return leg

    def add_spread(
        self,
        name: str,
        leg_settings: List[Dict],
        active_symbol: str,
        min_volume: float,
        save: bool = True
    ) -> None:
        """"""
        if name in self.spreads:
            self.write_log("价差创建失败，名称重复：{}".format(name))
            return

        legs: List[LegData] = []
        price_multipliers: Dict[str, int] = {}
        trading_multipliers: Dict[str, int] = {}
        inverse_contracts: Dict[str, bool] = {}

        for leg_setting in leg_settings:
            vt_symbol = leg_setting["vt_symbol"]
            leg = self.get_leg(vt_symbol)

            legs.append(leg)
            price_multipliers[vt_symbol] = leg_setting["price_multiplier"]
            trading_multipliers[vt_symbol] = leg_setting["trading_multiplier"]
            inverse_contracts[vt_symbol] = leg_setting.get(
                "inverse_contract", False)

        spread = SpreadData(
            name,
            legs,
            price_multipliers,
            trading_multipliers,
            active_symbol,
            inverse_contracts,
            min_volume
        )
        self.spreads[name] = spread

        for leg in spread.legs.values():
            self.symbol_spread_map[leg.vt_symbol].append(spread)

        if save:
            self.save_setting()

        self.write_log("价差创建成功：{}".format(name))
        self.put_data_event(spread)

    def remove_spread(self, name: str) -> None:
        """"""
        if name not in self.spreads:
            return

        spread = self.spreads.pop(name)

        for leg in spread.legs.values():
            self.symbol_spread_map[leg.vt_symbol].remove(spread)

        self.save_setting()
        self.write_log("价差移除成功：{}，重启后生效".format(name))

    def get_spread(self, name: str) -> SpreadData:
        """"""
        spread = self.spreads.get(name, None)
        return spread

    def get_all_spreads(self) -> List[SpreadData]:
        """"""
        return list(self.spreads.values())


class SpreadAlgoEngine:
    """"""
    algo_class = SpreadTakerAlgo

    def __init__(self, spread_engine: SpreadEngine):
        """"""
        self.spread_engine: SpreadEngine = spread_engine
        self.main_engine: MainEngine = spread_engine.main_engine
        self.event_engine: EventEngine = spread_engine.event_engine

        self.write_log = spread_engine.write_log

        self.spreads: Dict[str: SpreadData] = {}
        self.algos: Dict[str: SpreadAlgoTemplate] = {}

        self.order_algo_map: dict[str: SpreadAlgoTemplate] = {}
        self.symbol_algo_map: dict[str: SpreadAlgoTemplate] = defaultdict(list)

        self.algo_count: int = 0
        self.vt_tradeids: Set = set()

        self.offset_converter: OffsetConverter = OffsetConverter(
            self.main_engine
        )

    def start(self):
        """"""
        self.register_event()

        self.write_log("价差算法引擎启动成功")

    def stop(self):
        """"""
        for algo in self.algos.values():
            self.stop_algo(algo)

    def register_event(self):
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(
            EVENT_SPREAD_DATA, self.process_spread_event
        )

    def process_spread_event(self, event: Event):
        """"""
        spread: SpreadData = event.data
        self.spreads[spread.name] = spread

    def process_tick_event(self, event: Event):
        """"""
        tick = event.data
        algos = self.symbol_algo_map[tick.vt_symbol]
        if not algos:
            return

        buf = copy(algos)
        for algo in buf:
            if not algo.is_active():
                algos.remove(algo)
            else:
                algo.update_tick(tick)

    def process_order_event(self, event: Event):
        """"""
        order = event.data

        self.offset_converter.update_order(order)

        algo = self.order_algo_map.get(order.vt_orderid, None)
        if algo and algo.is_active():
            algo.update_order(order)

    def process_trade_event(self, event: Event):
        """"""
        trade = event.data

        # Filter duplicate trade push
        if trade.vt_tradeid in self.vt_tradeids:
            return
        self.vt_tradeids.add(trade.vt_tradeid)

        self.offset_converter.update_trade(trade)

        algo = self.order_algo_map.get(trade.vt_orderid, None)
        if algo and algo.is_active():
            algo.update_trade(trade)

    def process_position_event(self, event: Event):
        """"""
        position = event.data

        self.offset_converter.update_position(position)

    def process_timer_event(self, event: Event):
        """"""
        buf = list(self.algos.values())

        for algo in buf:
            if not algo.is_active():
                self.algos.pop(algo.algoid)
            else:
                algo.update_timer()

    def start_algo(
        self,
        spread_name: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        payup: int,
        interval: int,
        lock: bool
    ) -> str:
        # Find spread object
        spread = self.spreads.get(spread_name, None)
        if not spread:
            self.write_log("创建价差算法失败，找不到价差：{}".format(spread_name))
            return ""

        # Generate algoid str
        self.algo_count += 1
        algo_count_str = str(self.algo_count).rjust(6, "0")
        algoid = f"{self.algo_class.algo_name}_{algo_count_str}"

        # Create algo object
        algo = self.algo_class(
            self,
            algoid,
            spread,
            direction,
            offset,
            price,
            volume,
            payup,
            interval,
            lock
        )
        self.algos[algoid] = algo

        # Generate map between vt_symbol and algo
        for leg in spread.legs.values():
            self.symbol_algo_map[leg.vt_symbol].append(algo)

        # Put event to update GUI
        self.put_algo_event(algo)

        return algoid

    def stop_algo(
        self,
        algoid: str
    ):
        """"""
        algo = self.algos.get(algoid, None)
        if not algo:
            self.write_log("停止价差算法失败，找不到算法：{}".format(algoid))
            return

        algo.stop()

    def manual_stop_algo(
        self,
        algoid: str
    ):
        """"""
        algo = self.algos.get(algoid, None)
        if not algo:
            self.write_log("停止价差算法失败，找不到算法：{}".format(algoid))
            return

        algo.manual_stop()

    def put_algo_event(self, algo: SpreadAlgoTemplate) -> None:
        """"""
        event = Event(EVENT_SPREAD_ALGO, algo)
        self.event_engine.put(event)

    """ modify by loe """
    def on_traded_changed(self, algo: SpreadAlgoTemplate, changed=0) -> None:
        """"""
        strategy_engine = self.spread_engine.strategy_engine
        strategy = strategy_engine.algo_strategy_map.get(algo.algoid, None)
        if strategy:
            strategy.spread_pos += changed
            strategy.on_traded_changed(algo=algo, changed=changed)
        strategy.put_event()

    def write_algo_log(self, algo: SpreadAlgoTemplate, msg: str) -> None:
        """"""
        msg = f"{algo.algoid}：{msg}"
        self.write_log(msg)

    def send_order(
        self,
        algo: SpreadAlgoTemplate,
        vt_symbol: str,
        price: float,
        volume: float,
        direction: Direction,
        lock: bool
    ) -> List[str]:
        """"""
        holding = self.offset_converter.get_position_holding(vt_symbol)
        contract = self.main_engine.get_contract(vt_symbol)

        if direction == Direction.LONG:
            available = holding.short_pos - holding.short_pos_frozen
        else:
            available = holding.long_pos - holding.long_pos_frozen

        """ modify by loe """
        # 1Token接口的合约持仓信息bug，导致开平仓判断错误，这里强制1Token接口的开平操作统一做开仓。
        if contract.gateway_name == '1TOKEN':
            offset = Offset.OPEN
        else:
            # If no position to close, just open new
            if not available:
                offset = Offset.OPEN
            # If enougth position to close, just close old
            elif volume < available:
                offset = Offset.CLOSE
            # Otherwise, just close existing position
            else:
                volume = available
                offset = Offset.CLOSE

        original_req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            offset=offset,
            type=OrderType.LIMIT,
            price=price,
            volume=volume
        )

        # Convert with offset converter
        req_list = self.offset_converter.convert_order_request(
            original_req, lock)

        # Send Orders
        vt_orderids = []

        for req in req_list:
            vt_orderid = self.main_engine.send_order(
                req, contract.gateway_name)

            # Check if sending order successful
            if not vt_orderid:
                continue

            vt_orderids.append(vt_orderid)

            self.offset_converter.update_order_request(req, vt_orderid)

            # Save relationship between orderid and algo.
            self.order_algo_map[vt_orderid] = algo

        return vt_orderids

    def cancel_order(self, algo: SpreadAlgoTemplate, vt_orderid: str) -> None:
        """"""
        order = self.main_engine.get_order(vt_orderid)
        if not order:
            self.write_algo_log(algo, "撤单失败，找不到委托{}".format(vt_orderid))
            return

        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def get_tick(self, vt_symbol: str) -> TickData:
        """"""
        return self.main_engine.get_tick(vt_symbol)

    def get_contract(self, vt_symbol: str) -> ContractData:
        """"""
        return self.main_engine.get_contract(vt_symbol)


class SpreadStrategyEngine:
    """"""

    setting_filename = "spraed_trading_strategy.json"

    def __init__(self, spread_engine: SpreadEngine):
        """"""
        self.spread_engine: SpreadEngine = spread_engine
        self.main_engine: MainEngine = spread_engine.main_engine
        self.event_engine: EventEngine = spread_engine.event_engine

        self.write_log = spread_engine.write_log

        self.strategy_setting: Dict[str: Dict] = {}

        self.classes: Dict[str: Type[SpreadStrategyTemplate]] = {}
        self.strategies: Dict[str: SpreadStrategyTemplate] = {}

        self.order_strategy_map: dict[str: SpreadStrategyTemplate] = {}
        self.algo_strategy_map: dict[str: SpreadStrategyTemplate] = {}
        self.spread_strategy_map: dict[str: SpreadStrategyTemplate] = defaultdict(
            list)

        self.vt_tradeids: Set = set()

        self.load_strategy_class()

        """ modify by loe """
        # 数据引擎【下载时间 ['10:20', '11:35', '15:35', '23:35']】
        self.autoEngine = SpreadAutoEngine(main_engine=self.spread_engine.main_engine, spread_strategy_engine=self, download_time_list=['10:20', '11:35', '15:05', '23:35'],
                                           reconnect_time='20:00', check_interval=10 * 60, reload_time=6)
        self.downloading_flag: datetime = None
        self.download_callback_list = []

    def start(self):
        """"""
        self.load_strategy_setting()
        self.register_event()

        """ modify by loe """
        # 数据引擎启动
        self.autoEngine.start()

        self.write_log("价差策略引擎启动成功")

    def close(self):
        """"""
        self.stop_all_strategies()

    def load_strategy_class(self):
        """
        Load strategy class from source code.
        """
        path1 = Path(__file__).parent.joinpath("strategies")
        self.load_strategy_class_from_folder(
            path1, "App.spread_trading.strategies")

    def load_strategy_class_from_folder(self, path: Path, module_name: str = ""):
        """
        Load strategy class from certain folder.
        """
        for dirpath, dirnames, filenames in os.walk(str(path)):
            for filename in filenames:
                if filename.endswith(".py"):
                    strategy_module_name = ".".join(
                        [module_name, filename.replace(".py", "")])
                elif filename.endswith(".pyd"):
                    strategy_module_name = ".".join(
                        [module_name, filename.split(".")[0]])

                self.load_strategy_class_from_module(strategy_module_name)

    def load_strategy_class_from_module(self, module_name: str):
        """
        Load strategy class from module file.
        """
        try:
            module = importlib.import_module(module_name)

            for name in dir(module):
                value = getattr(module, name)
                if (isinstance(value, type) and issubclass(value, SpreadStrategyTemplate) and value is not SpreadStrategyTemplate):
                    self.classes[value.__name__] = value
        except:  # noqa
            msg = f"策略文件{module_name}加载失败，触发异常：\n{traceback.format_exc()}"
            self.write_log(msg)

    def get_all_strategy_class_names(self):
        """"""
        return list(self.classes.keys())

    def load_strategy_setting(self):
        """
        Load setting file.
        """
        self.strategy_setting = load_json(self.setting_filename)

        for strategy_name, strategy_config in self.strategy_setting.items():
            self.add_strategy(
                strategy_config["class_name"],
                strategy_name,
                strategy_config["spread_name"],
                strategy_config["setting"]
            )

    def update_strategy_setting(self, strategy_name: str, setting: dict):
        """
        Update setting file.
        """
        strategy = self.strategies[strategy_name]

        self.strategy_setting[strategy_name] = {
            "class_name": strategy.__class__.__name__,
            "spread_name": strategy.spread_name,
            "setting": setting,
        }
        save_json(self.setting_filename, self.strategy_setting)

    def remove_strategy_setting(self, strategy_name: str):
        """
        Update setting file.
        """
        if strategy_name not in self.strategy_setting:
            return

        self.strategy_setting.pop(strategy_name)
        save_json(self.setting_filename, self.strategy_setting)

    def register_event(self):
        """"""
        ee = self.event_engine
        ee.register(EVENT_ORDER, self.process_order_event)
        ee.register(EVENT_TRADE, self.process_trade_event)
        ee.register(EVENT_SPREAD_DATA, self.process_spread_data_event)
        ee.register(EVENT_SPREAD_POS, self.process_spread_pos_event)
        ee.register(EVENT_SPREAD_ALGO, self.process_spread_algo_event)
        """ modify by loe """
        ee.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event:Event):
        for strategy in self.strategies.values():
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_timer)

    def process_spread_data_event(self, event: Event):
        """"""
        spread = event.data
        strategies = self.spread_strategy_map[spread.name]

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_spread_data)

    def process_spread_pos_event(self, event: Event):
        """"""
        spread = event.data
        strategies = self.spread_strategy_map[spread.name]

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_spread_pos)

    def process_spread_algo_event(self, event: Event):
        """"""
        algo = event.data
        strategy = self.algo_strategy_map.get(algo.algoid, None)

        if strategy:
            self.call_strategy_func(
                strategy, strategy.update_spread_algo, algo)

    def process_order_event(self, event: Event):
        """"""
        order = event.data
        strategy = self.order_strategy_map.get(order.vt_orderid, None)

        if strategy:
            self.call_strategy_func(strategy, strategy.update_order, order)

    def process_trade_event(self, event: Event):
        """"""
        trade = event.data
        strategy = self.order_strategy_map.get(trade.vt_orderid, None)

        if strategy:
            self.call_strategy_func(strategy, strategy.on_trade, trade)

    def call_strategy_func(
        self, strategy: SpreadStrategyTemplate, func: Callable, params: Any = None
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
            self.write_strategy_log(strategy, msg)

    def add_strategy(
        self, class_name: str, strategy_name: str, spread_name: str, setting: dict
    ):
        """
        Add a new strategy.
        """
        if strategy_name in self.strategies:
            self.write_log(f"创建策略失败，存在重名{strategy_name}")
            return

        strategy_class = self.classes.get(class_name, None)
        if not strategy_class:
            self.write_log(f"创建策略失败，找不到策略类{class_name}")
            return

        spread = self.spread_engine.get_spread(spread_name)
        if not spread:
            self.write_log(f"创建策略失败，找不到价差{spread_name}")
            return

        strategy = strategy_class(self, strategy_name, spread, setting)
        """ modify by loe """
        # 加载同步数据
        self.loadSyncData(strategy)
        self.strategies[strategy_name] = strategy

        # Add vt_symbol to strategy map.
        strategies = self.spread_strategy_map[spread_name]
        strategies.append(strategy)

        # Update to setting file.
        self.update_strategy_setting(strategy_name, setting)

        self.put_strategy_event(strategy)

    def edit_strategy(self, strategy_name: str, setting: dict):
        """
        Edit parameters of a strategy.
        """
        strategy = self.strategies[strategy_name]
        strategy.update_setting(setting)

        self.update_strategy_setting(strategy_name, setting)
        self.put_strategy_event(strategy)

    def remove_strategy(self, strategy_name: str):
        """
        Remove a strategy.
        """
        strategy = self.strategies[strategy_name]
        if strategy.trading:
            self.write_log(f"策略{strategy.strategy_name}移除失败，请先停止")
            return

        # Remove setting
        self.remove_strategy_setting(strategy_name)

        # Remove from symbol strategy map
        strategies = self.spread_strategy_map[strategy.spread_name]
        strategies.remove(strategy)

        # Remove from strategies
        self.strategies.pop(strategy_name)

        return True

    def init_strategy(self, strategy_name: str):
        """"""
        strategy = self.strategies[strategy_name]

        if strategy.inited:
            self.write_log(f"{strategy_name}已经完成初始化，禁止重复操作")
            return

        self.call_strategy_func(strategy, strategy.on_init)
        strategy.inited = True

        self.put_strategy_event(strategy)
        self.write_log(f"{strategy_name}初始化完成")

    def start_strategy(self, strategy_name: str):
        """"""
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
        """"""
        strategy = self.strategies[strategy_name]
        if not strategy.trading:
            return

        self.call_strategy_func(strategy, strategy.on_stop)

        strategy.stop_all_algos()
        strategy.cancel_all_orders()

        strategy.trading = False

        self.put_strategy_event(strategy)

    def init_all_strategies(self):
        """"""
        for strategy in self.strategies.keys():
            self.init_strategy(strategy)

    def start_all_strategies(self):
        """"""
        for strategy in self.strategies.keys():
            self.start_strategy(strategy)

    def stop_all_strategies(self):
        """"""
        for strategy in self.strategies.keys():
            self.stop_strategy(strategy)

    def get_strategy_class_parameters(self, class_name: str):
        """
        Get default parameters of a strategy class.
        """
        strategy_class = self.classes[class_name]

        parameters = {}
        for name in strategy_class.parameters:
            parameters[name] = getattr(strategy_class, name)

        return parameters

    def get_strategy_parameters(self, strategy_name):
        """
        Get parameters of a strategy.
        """
        strategy = self.strategies[strategy_name]
        return strategy.get_parameters()

    def start_algo(
        self,
        strategy: SpreadStrategyTemplate,
        spread_name: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        payup: int,
        interval: int,
        lock: bool
    ) -> str:
        """"""
        algoid = self.spread_engine.start_algo(
            spread_name,
            direction,
            offset,
            price,
            volume,
            payup,
            interval,
            lock
        )

        self.algo_strategy_map[algoid] = strategy

        return algoid

    def stop_algo(self, strategy: SpreadStrategyTemplate, algoid: str):
        """"""
        self.spread_engine.stop_algo(algoid)

    def stop_all_algos(self, strategy: SpreadStrategyTemplate):
        """"""
        pass

    """ modify by loe """
    def get_algo(self, algoid:str):
        algo = self.spread_engine.algo_engine.algos.get(algoid, None)
        return algo

    def send_order(
        self,
        strategy: SpreadStrategyTemplate,
        vt_symbol: str,
        price: float,
        volume: float,
        direction: Direction,
        offset: Offset,
        lock: bool
    ) -> List[str]:
        contract = self.main_engine.get_contract(vt_symbol)

        original_req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            offset=offset,
            type=OrderType.LIMIT,
            price=price,
            volume=volume
        )

        # Convert with offset converter
        req_list = self.offset_converter.convert_order_request(
            original_req, lock)

        # Send Orders
        vt_orderids = []

        for req in req_list:
            vt_orderid = self.main_engine.send_order(
                req, contract.gateway_name)

            # Check if sending order successful
            if not vt_orderid:
                continue

            vt_orderids.append(vt_orderid)

            self.offset_converter.update_order_request(req, vt_orderid)

            # Save relationship between orderid and strategy.
            self.order_strategy_map[vt_orderid] = strategy

        return vt_orderids

    def cancel_order(self, strategy: SpreadStrategyTemplate, vt_orderid: str):
        """"""
        order = self.main_engine.get_order(vt_orderid)
        if not order:
            self.write_strategy_log(
                strategy, "撤单失败，找不到委托{}".format(vt_orderid))
            return

        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def cancel_all_orders(self, strategy: SpreadStrategyTemplate):
        """"""
        pass

    def put_strategy_event(self, strategy: SpreadStrategyTemplate):
        """"""
        """ modify by loe """
        # 保存strategy数据到数据库
        strategy_name = strategy.strategy_name
        if strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            self.saveSyncData(strategy)

        data = strategy.get_data()
        event = Event(EVENT_SPREAD_STRATEGY, data)
        self.event_engine.put(event)

    def write_strategy_log(self, strategy: SpreadStrategyTemplate, msg: str):
        """"""
        msg = f"{strategy.strategy_name}：{msg}"
        self.write_log(msg)

    def send_strategy_email(self, strategy: SpreadStrategyTemplate, msg: str):
        """"""
        if strategy:
            subject = f"{strategy.strategy_name}"
        else:
            subject = "价差策略引擎"

        self.main_engine.send_email(subject, msg)

    def load_bar(
        self, spread: SpreadData, days: int, interval: Interval, callback: Callable
    ):
        """"""
        end = datetime.now()
        start = end - timedelta(days)

        bars = load_bar_data(spread, interval, start, end)

        for bar in bars:
            callback(bar)

    def load_tick(self, spread: SpreadData, days: int, callback: Callable):
        """"""
        end = datetime.now()
        start = end - timedelta(days)

        ticks = load_tick_data(spread, start, end)

        for tick in ticks:
            callback(tick)

    """ modify by loe """
    def saveSyncData(self, strategy):
        """保存策略的持仓情况到数据库"""
        flt = {'strategy_name': strategy.strategy_name,
               'spread_name': strategy.spread_name}

        d = copy(flt)
        for key in strategy.syncs:
            d[key] = strategy.__getattribute__(key)

        self.main_engine.dbUpdate(POSITION_DB_NAME, strategy.__class__.__name__,
                                  d, flt, True, callback=self.dbUpdateCallback)


    def dbUpdateCallback(self, back_data=None):
        try:
            if isinstance(back_data, dict):
                result = back_data.get('result', False)
                strategy_name = back_data.get('strategy_name', '')
                if result:
                    content = f'策略{strategy_name}同步数据保存成功。'
                else:
                    content = f'策略{strategy_name}同步数据保存失败！！'
                self.write_log(content)
            else:
                content = f'价差交易策略同步数据保存失败！！'
                self.write_log(content)
        except:
            content = f'价差交易策略同步数据保存失败！！'
            self.write_log(content)

    def loadSyncData(self, strategy):
        """从数据库载入策略的持仓情况"""
        flt = {'strategy_name': strategy.strategy_name,
               'spread_name': strategy.spread_name}
        syncData = self.main_engine.dbQuery(POSITION_DB_NAME, strategy.__class__.__name__, flt)

        if not syncData:
            return

        d = syncData[0]

        for key in strategy.syncs:
            if key in d:
                strategy.__setattr__(key, d[key])

    def getAlgo(self, algoid):
        if not algoid:
            return None

        algo_engine = self.spread_engine.algo_engine
        return algo_engine.algos.get(algoid, None)

    """ modify by loe """

    # 新的DailyBar更新后需要自动重新初始化策略
    def reinit_and_restart_strategies(self):
        for strategy_name in self.strategies.keys():
            strategy = self.strategies[strategy_name]
            if strategy.inited:
                temp = strategy.trading
                # 重新初始化
                strategy.trading = False
                self.call_strategy_func(strategy, strategy.on_init)
                strategy.trading = temp
                # 重新启动
                self.call_strategy_func(strategy, strategy.on_start)

                self.put_strategy_event(strategy)
                self.write_log(f"{strategy_name} 重新初始化完成")

    def downloading_recent_data(self, callback:None):
        if callback not in self.download_callback_list:
            self.download_callback_list.append(callback)

        if not self.downloading_flag or datetime.now() >= self.downloading_flag + timedelta(seconds=20):
            self.downloading_flag = datetime.now()
            last_datetime, msg = TurtleDataDownloading().download_minute_multi_jq(recent_minute=5)
            for the_callback in self.download_callback_list:
                the_callback(last_datetime, msg)

""" modify by loe """
# 数据自动化引擎，每天固定时间下载策略回测及实盘必要的数据，自动重连CTP和重新初始化策略
class SpreadAutoEngine(object):

    def __init__(self, main_engine:MainEngine, spread_strategy_engine:SpreadStrategyEngine, download_time_list:list, reconnect_time:str, check_interval:int, reload_time:int):
        # download_time:['10:20', '11:35', '15:35', '23:35'], reconnect_time:'20:00' check_interval:10*60, reload_time:6
        super(SpreadAutoEngine, self).__init__()

        self.main_engine = main_engine
        self.spread_strategy_engine = spread_strategy_engine
        self.download_time_list = download_time_list
        self.reconnect_time = reconnect_time
        self.check_interval = check_interval
        self.reload_time = reload_time
        self.downloading = False
        self.restarting = False
        self.restarted = False
        self.download_timer = Thread(target=self.on_download_timer)
        self.reconnect_timer = Thread(target=self.on_reconnect_timer)

    def start(self):
        self.init_download_need = True
        self.download_timer.start()
        self.reconnect_timer.start()

    def on_download_timer(self):
        while True:
            try:
                self.checkAndDownload()
            except:
                try:
                    self.downloading = False
                    self.main_engine.send_email(subject='SPREAD 数据更新', content=f'【未知错误】\n\n{traceback.format_exc()}')
                except:
                    pass
            sleep(10)

    def checkAndDownload(self):
        now = datetime.now()
        check_time = False
        for download_time in self.download_time_list:
            start_time = datetime.strptime(f'{now.year}-{now.month}-{now.day} {download_time}', '%Y-%m-%d %H:%M')
            end_time = start_time + timedelta(seconds=60)
            if now >= start_time and now <= end_time:
                check_time = True
                break
        if self.init_download_need or check_time:
            if not self.downloading:
                turtleDataD = TurtleDataDownloading()
                self.downloading = True
                last_datetime, msg = turtleDataD.download_minute_multi_jq(days=0)
                self.downloading = False
                if not self.init_download_need:
                    # SPREAD重新初始化
                    self.spread_strategy_engine.reinit_and_restart_strategies()
                    msg = f'{msg}\n\n策略重新初始化成功'
                self.main_engine.send_email(subject='SPREAD 数据更新', content=msg)
                if not self.init_download_need:
                    sleep(10*60)
                self.init_download_need = False

    def on_reconnect_timer(self):
        while True:
            try:
                self.checkAndReconnect()
            except:
                try:
                    self.main_engine.send_email(subject='SPREAD 服务器重连、策略重新初始化', content=f'【未知错误】\n\n{traceback.format_exc()}')
                except:
                    pass
            sleep(self.check_interval)

    def checkAndReconnect(self):
        now = datetime.now()
        start_time = datetime.strptime(f'{now.year}-{now.month}-{now.day} {self.reconnect_time}', '%Y-%m-%d %H:%M')
        end_time = start_time + timedelta(seconds=self.check_interval * self.reload_time)
        if now >= start_time and now <= end_time:
            if not self.restarting and not self.restarted:
                self.restarting = True
                result, return_msg = self.main_engine.reconnect(gateway_name='CTP')
                self.restarting = False
                self.restarted = result
                # SPREAD重新初始化
                self.spread_strategy_engine.reinit_and_restart_strategies()
                return_msg = f'{return_msg}\n\n策略重新初始化成功'
                try:
                    self.main_engine.send_email(subject='SPREAD 服务器重连、策略重新初始化', content=return_msg)
                except:
                    pass
        else:
            self.restarted = False
