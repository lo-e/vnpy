
from collections import defaultdict
from typing import Dict, List, Set, Callable
from copy import copy

from vnpy.trader.object import (
    TickData, TradeData, OrderData, ContractData, BarData
)
from vnpy.trader.constant import Direction, Status, Offset, Interval
from vnpy.trader.utility import virtual, floor_to, ceil_to, round_to

from .base import SpreadData, calculate_inverse_volume

""" modify by loe """
#数据下载
from concurrent.futures import ThreadPoolExecutor
import datetime
""" fake """
from datetime import timedelta
import re

""" modify by loe """
#====== 交易时间 ======
#商品期货
MORNING_START_CF = datetime.time(9, 0)
MORNING_REST_CF = datetime.time(10, 15)
MORNING_RESTART_CF = datetime.time(10, 30)
MORNING_END_CF = datetime.time(11, 30)
AFTERNOON_START_CF = datetime.time(13, 30)
AFTERNOON_END_CF = datetime.time(15, 0)

# 商品期货夜盘时间
NIGHT_START_CF = datetime.time(21, 0)
NIGHT_END_CF_N = datetime.time(23, 0) # 到夜间收盘
NIGHT_END_CF_NM = datetime.time(1, 0) # 到凌晨收盘
NIGHT_END_CF_M = datetime.time(2, 30) # 到凌晨收盘

#股指期货
MORNING_PRE_START_SF = datetime.time(6, 0)
MORNING_START_SF = datetime.time(9, 30)
MORNING_END_SF = datetime.time(11, 30)
AFTERNOON_START_SF = datetime.time(13, 0)
AFTERNOON_END_SF = datetime.time(15, 0)

#国债期货
MORNING_START_BD_SF = datetime.time(9, 15)
AFTERNOON_END_BD_SF = datetime.time(15, 15)

from enum import Enum

class NType(Enum):
    """
    Type of night
    """
    NONE = "None"
    EARLY = "23:00"
    MID = "1:00"
    LATER = "2:30"

# 夜盘类别
def get_night_type(symbol):
    # 能够识别 'RB10_RB05', 'RB2010.SHFE'
    target_symbol = copy(symbol)
    target_symbol = target_symbol.upper()
    target_symbol = target_symbol.split('_')[0]
    target_symbol = target_symbol.split('.')[0]
    startSymbol = re.sub("\d", "", target_symbol)

    if startSymbol in ['CF', 'CS', 'EG', 'RM', 'SP', 'SR', 'TA', 'ZC', 'RU', 'SA', 'NR']:
        return NType.EARLY

    if startSymbol in ['CU', 'PB', 'ZN']:
        return NType.MID

    if startSymbol in ['SC']:
        return NType.LATER

    return NType.NONE

# 是否是股指期货
def is_finance_stock_symbol(symbol):
    # 能够识别 'RB10_RB05', 'RB2010.SHFE'
    financeSymbols = ['IF', 'IC', 'IH']
    target_symbol = copy(symbol)
    target_symbol = target_symbol.upper()
    target_symbol = target_symbol.split('_')[0]
    target_symbol = target_symbol.split('.')[0]
    startSymbol = re.sub("\d", "", target_symbol)
    if startSymbol in financeSymbols:
        return True
    else:
        return False

# 是否是债券期货
def is_finance_bond_symbol(symbol):
    # 能够识别 'RB10_RB05', 'RB2010.SHFE'
    financeSymbols = ['TF', 'TS']
    target_symbol = copy(symbol)
    target_symbol = target_symbol.upper()
    target_symbol = target_symbol.split('_')[0]
    target_symbol = target_symbol.split('.')[0]
    startSymbol = re.sub("\d", "", target_symbol)
    if startSymbol in financeSymbols:
        return True
    else:
        return False

def check_trading_time(symbol, the_datetime:datetime.datetime):
    result = True
    t = the_datetime.time()
    is_stock = is_finance_stock_symbol(symbol)
    is_bond = is_finance_bond_symbol(symbol)
    if is_stock:
        # 股指期货
        if t < MORNING_START_SF or MORNING_END_SF <= t < AFTERNOON_START_SF or AFTERNOON_END_SF <= t:
            result = False

    elif is_bond:
        # 债券期货
        if t < MORNING_START_BD_SF or MORNING_END_SF <= t < AFTERNOON_START_SF or AFTERNOON_END_BD_SF <= t:
            result = False

    else:
        # 商品期货，根据夜盘时间分别判断
        night_type = get_night_type(symbol=symbol)
        if night_type == NType.NONE:
            # 没有夜盘
            if t < MORNING_START_CF or MORNING_REST_CF <= t < MORNING_RESTART_CF or MORNING_END_CF <= t < AFTERNOON_START_CF or AFTERNOON_END_CF <= t:
                result = False

        elif night_type == NType.EARLY:
            # 夜盘到23：00
            if t < MORNING_START_CF or MORNING_REST_CF <= t < MORNING_RESTART_CF or MORNING_END_CF <= t < AFTERNOON_START_CF or AFTERNOON_END_CF <= t < NIGHT_START_CF or t >= NIGHT_END_CF_N:
                result = False

        elif night_type == NType.MID:
            # 夜盘到1：00
            if NIGHT_END_CF_NM <= t < MORNING_START_CF or MORNING_REST_CF <= t < MORNING_RESTART_CF or MORNING_END_CF <= t < AFTERNOON_START_CF or AFTERNOON_END_CF <= t < NIGHT_START_CF:
                result = False

        elif night_type == NType.LATER:
            # 夜盘到2：30
            if NIGHT_END_CF_M <= t < MORNING_START_CF or MORNING_REST_CF <= t < MORNING_RESTART_CF or MORNING_END_CF <= t < AFTERNOON_START_CF or AFTERNOON_END_CF <= t < NIGHT_START_CF:
                result = False

    return result

def check_tick_valid(tick:TickData):
    # 判断tick数据是否有效
    if not tick.symbol or not tick.datetime:
        return False

    if check_trading_time(symbol=tick.symbol, the_datetime=tick.datetime):
        return True
    else:
        return False

def check_spread_valid(spread:SpreadData):
    result = True
    for leg in spread.legs.values():
        if leg.tick:
            if not check_tick_valid(tick=leg.tick):
                result = False
                break
        else:
            result = False
            break
    return result

class SpreadAlgoTemplate:
    """
    Template for implementing spread trading algos.
    """
    algo_name = "AlgoTemplate"

    def __init__(
        self,
        algo_engine,
        algoid: str,
        spread: SpreadData,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        payup: int,
        interval: int,
        lock: bool,
    ):
        """"""
        self.algo_engine = algo_engine
        self.algoid: str = algoid

        self.init_datetime = datetime.datetime.now()
        self.stop_datetime = None
        self.leg_traded_desc = ''

        self.spread: SpreadData = spread
        self.spread_name: str = spread.name

        self.offset: Offset = offset
        self.direction: Direction = direction
        self.price: float = price
        self.volume: float = volume
        self.payup: int = payup
        self.interval = interval
        self.lock = lock

        if direction == Direction.LONG:
            self.target = volume
        else:
            self.target = -volume

        self.status: Status = Status.NOTTRADED  # Algo status
        self.count: int = 0                     # Timer count
        self.traded: float = 0                  # Volume traded
        self.traded_volume: float = 0           # Volume traded (Abs value)

        self.leg_traded: Dict[str, float] = defaultdict(int)
        self.leg_orders: Dict[str, List[str]] = defaultdict(list)

        """ modify by loe """
        # 用于平仓算法，值为True时尽快平仓
        self.close_anyway = False
        # 用于保证金的计算，并且用于开仓算法
        self.ready_open_traded = 0

        self.write_log("算法已启动")

    def is_active(self):
        """"""
        if self.status not in [Status.CANCELLED, Status.ALLTRADED]:
            return True
        else:
            return False

    def check_order_finished(self):
        """"""
        finished = True

        for leg in self.spread.legs.values():
            vt_orderids = self.leg_orders[leg.vt_symbol]

            if vt_orderids:
                finished = False
                break

        return finished

    def check_hedge_finished(self):
        """"""
        active_symbol = self.spread.active_leg.vt_symbol
        active_traded = self.leg_traded[active_symbol]

        spread_volume = self.spread.calculate_spread_volume(
            active_symbol, active_traded
        )

        finished = True

        for leg in self.spread.passive_legs:
            passive_symbol = leg.vt_symbol

            leg_target = self.spread.calculate_leg_volume(
                passive_symbol, spread_volume
            )
            leg_traded = self.leg_traded[passive_symbol]

            if leg_traded != leg_target:
                finished = False
                break

        return finished

    """ modify by loe """
    def check_leg_traded(self):
        for value in self.leg_traded.values():
            if value:
                return True
        return False

    def stop(self):
        """"""
        if not self.check_order_finished() or not self.check_hedge_finished():
            # 有订单正在进行或者出现断腿情况，保持算法运行
            return False

        if self.is_active():
            self.cancel_all_order()
            self.status = Status.CANCELLED
            self.write_log("算法已停止")
            self.stop_datetime = datetime.datetime.now()
            self.put_event()

        return True

    def manual_stop(self):
        """"""
        if self.is_active():
            self.cancel_all_order()
            self.status = Status.CANCELLED
            self.write_log("算法已停止")
            self.stop_datetime = datetime.datetime.now()
            self.put_event()

        return True

    def update_tick(self, tick: TickData):
        """"""
        self.on_tick(tick)

    def update_trade(self, trade: TradeData):
        """"""
        # For inverse contract:
        # record coin trading volume as leg trading volume,
        # not contract volume!
        if self.spread.is_inverse(trade.vt_symbol):
            size = self.spread.get_leg_size(trade.vt_symbol)

            trade_volume = calculate_inverse_volume(
                trade.volume,
                trade.price,
                size
            )
        else:
            trade_volume = trade.volume

        if trade.direction == Direction.LONG:
            self.leg_traded[trade.vt_symbol] += trade_volume
        else:
            self.leg_traded[trade.vt_symbol] -= trade_volume

        msg = "委托成交，{}，{}，{}@{}".format(
            trade.vt_symbol,
            trade.direction,
            trade.volume,
            trade.price
        )
        self.write_log(msg)

        self.leg_traded_desc = f'{self.leg_traded}'

        self.calculate_traded()
        self.put_event()

        self.on_trade(trade)

    def update_order(self, order: OrderData):
        """"""
        if not order.is_active():
            vt_orderids = self.leg_orders[order.vt_symbol]
            if order.vt_orderid in vt_orderids:
                vt_orderids.remove(order.vt_orderid)

        self.on_order(order)

    def update_timer(self):
        """"""
        self.count += 1
        if self.count > self.interval:
            self.count = 0
            self.on_interval()

        self.put_event()

    def put_event(self):
        """"""
        self.algo_engine.put_algo_event(self)

    def write_log(self, msg: str):
        """"""
        self.algo_engine.write_algo_log(self, msg)

    def send_long_order(self, vt_symbol: str, price: float, volume: float):
        """"""
        self.send_order(vt_symbol, price, volume, Direction.LONG)

    def send_short_order(self, vt_symbol: str, price: float, volume: float):
        """"""
        self.send_order(vt_symbol, price, volume, Direction.SHORT)

    def send_order(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        direction: Direction,
    ):
        """"""
        # For inverse contract:
        # calculate contract trading volume from coin trading volume
        if self.spread.is_inverse(vt_symbol):
            size = self.spread.get_leg_size(vt_symbol)

            if self.offset == Offset.CLOSE:
                leg = self.spread.legs[vt_symbol]
                volume = volume * leg.net_pos_price / size
            else:
                volume = volume * price / size

        # Round order volume to min_volume of contract
        leg = self.spread.legs[vt_symbol]
        volume = round_to(volume, leg.min_volume)

        vt_orderids = self.algo_engine.send_order(
            self,
            vt_symbol,
            price,
            volume,
            direction,
            self.lock
        )

        self.leg_orders[vt_symbol].extend(vt_orderids)

        msg = "发出委托，{}，{}，{}@{}".format(
            vt_symbol,
            direction,
            volume,
            price
        )
        self.write_log(msg)

    def cancel_leg_order(self, vt_symbol: str):
        """"""
        """ modify by loe """
        # 判断是否是交易时间
        if check_trading_time(symbol=vt_symbol, the_datetime=datetime.datetime.now()):
            for vt_orderid in self.leg_orders[vt_symbol]:
                self.algo_engine.cancel_order(self, vt_orderid)

    def cancel_all_order(self):
        """"""
        for vt_symbol in self.leg_orders.keys():
            self.cancel_leg_order(vt_symbol)

    def calculate_traded(self):
        """"""
        """ modify by loe """
        # 将traded变化推送给策略
        last_traded = self.traded

        self.traded = 0

        for n, leg in enumerate(self.spread.legs.values()):
            leg_traded = self.leg_traded[leg.vt_symbol]
            trading_multiplier = self.spread.trading_multipliers[
                leg.vt_symbol]

            adjusted_leg_traded = leg_traded / trading_multiplier
            adjusted_leg_traded = round_to(
                adjusted_leg_traded, self.spread.min_volume)

            if adjusted_leg_traded > 0:
                adjusted_leg_traded = floor_to(
                    adjusted_leg_traded, self.spread.min_volume)
            else:
                adjusted_leg_traded = ceil_to(
                    adjusted_leg_traded, self.spread.min_volume)

            if not n:
                self.traded = adjusted_leg_traded
            else:
                if adjusted_leg_traded > 0:
                    self.traded = min(self.traded, adjusted_leg_traded)
                elif adjusted_leg_traded < 0:
                    self.traded = max(self.traded, adjusted_leg_traded)
                else:
                    self.traded = 0

        self.traded_volume = abs(self.traded)

        if self.traded == self.target:
            self.status = Status.ALLTRADED
        elif not self.traded:
            self.status = Status.NOTTRADED
        else:
            self.status = Status.PARTTRADED

        """ modify by loe """
        changed = self.traded - last_traded
        if self.offset == Offset.OPEN and self.ready_open_traded:
            # 用于保证金风控
            self.ready_open_traded -= changed
        self.on_traded_changed(changed)

    def get_tick(self, vt_symbol: str) -> TickData:
        """"""
        return self.algo_engine.get_tick(vt_symbol)

    def get_contract(self, vt_symbol: str) -> ContractData:
        """"""
        return self.algo_engine.get_contract(vt_symbol)

    @virtual
    def on_tick(self, tick: TickData):
        """"""
        pass

    @virtual
    def on_order(self, order: OrderData):
        """"""
        pass

    @virtual
    def on_trade(self, trade: TradeData):
        """"""
        pass

    @virtual
    def on_interval(self):
        """"""
        pass

    """ modify by loe """
    def on_traded_changed(self, changed=0):
        self.algo_engine.on_traded_changed(self, changed=changed)

class SpreadStrategyTemplate:
    """
    Template for implementing spread trading strategies.
    """

    author: str = ""
    parameters: List[str] = []
    variables: List[str] = []

    """ modify by loe """
    spread_pos = 0.0
    syncs = []
    timer_event_cross = False

    def __init__(
        self,
        strategy_engine,
        strategy_name: str,
        spread: SpreadData,
        setting: dict
    ):
        """"""
        self.strategy_engine = strategy_engine
        self.strategy_name = strategy_name
        self.spread = spread
        self.spread_name = spread.name

        self.inited = False
        self.trading = False

        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")
        """ modify by loe """
        self.variables.insert(2, "spread_pos")

        """ modify by loe """
        self.syncs = copy(self.syncs)
        if 'spread_pos' not in self.syncs:
            self.syncs.insert(0, "spread_pos")

        self.vt_orderids: Set[str] = set()
        self.algoids: Set[str] = set()
        self.buy_algoids_list: Set[str] = set()
        self.sell_algoids_list: Set[str] = set()
        self.short_algoids_list: Set[str] = set()
        self.cover_algoids_list: Set[str] = set()

        """ modify by loe """
        # 停止创建新的开仓算法
        self.stop_open = False

        # 对所有新开的平仓算法强制平仓
        self.close_anyway = False

        self.update_setting(setting)

        """ modify by loe """
        # 数据下载
        self.prepare_for_downloading()

    """ modify by loe """
    def prepare_for_downloading(self):
        thread_executor = ThreadPoolExecutor(max_workers=10)
        thread_executor.submit(self.download_data)

    def download_data(self):
        pass

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    @classmethod
    def get_class_parameters(cls):
        """
        Get default parameters dict of strategy class.
        """
        class_parameters = {}
        for name in cls.parameters:
            class_parameters[name] = getattr(cls, name)
        return class_parameters

    def get_parameters(self):
        """
        Get strategy parameters dict.
        """
        strategy_parameters = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self):
        """
        Get strategy variables dict.
        """
        strategy_variables = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self):
        """
        Get strategy data.
        """
        strategy_data = {
            "strategy_name": self.strategy_name,
            "spread_name": self.spread_name,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    def update_spread_algo(self, algo: SpreadAlgoTemplate):
        """
        Callback when algo status is updated.
        """
        # 一旦有算法出现成交，立即停止其他正在运行的算法
        self.check_and_stop_other_algo(algo)

        if not algo.is_active():
            if algo.algoid in self.algoids:
                self.algoids.remove(algo.algoid)

            if algo.algoid in self.buy_algoids_list:
                self.buy_algoids_list.remove(algo.algoid)

            if algo.algoid in self.short_algoids_list:
                self.short_algoids_list.remove(algo.algoid)

            if algo.algoid in self.sell_algoids_list:
                self.sell_algoids_list.remove(algo.algoid)

            if algo.algoid in self.cover_algoids_list:
                self.cover_algoids_list.remove(algo.algoid)

        self.on_spread_algo(algo)

    def update_order(self, order: OrderData):
        """
        Callback when order status is updated.
        """
        if not order.is_active() and order.vt_orderid in self.vt_orderids:
            self.vt_orderids.remove(order.vt_orderid)

        self.on_order(order)

    @virtual
    def on_init(self):
        """
        Callback when strategy is inited.
        """
        pass

    @virtual
    def on_start(self):
        """
        Callback when strategy is started.
        """
        pass

    @virtual
    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        pass

    @virtual
    def on_spread_data(self):
        """
        Callback when spread price is updated.
        """
        pass

    @virtual
    def on_spread_tick(self, tick: TickData):
        """
        Callback when new spread tick data is generated.
        """
        pass

    @virtual
    def on_spread_bar(self, bar: BarData):
        """
        Callback when new spread bar data is generated.
        """
        pass

    @virtual
    def on_spread_pos(self):
        """
        Callback when spread position is updated.
        """
        pass

    @virtual
    def on_spread_algo(self, algo: SpreadAlgoTemplate):
        """
        Callback when algo status is updated.
        """
        pass

    @virtual
    def on_order(self, order: OrderData):
        """
        Callback when order status is updated.
        """
        pass

    @virtual
    def on_trade(self, trade: TradeData):
        """
        Callback when new trade data is received.
        """
        pass

    def start_algo(
        self,
        direction: Direction,
        price: float,
        volume: float,
        payup: int,
        interval: int,
        lock: bool,
        offset: Offset
    ) -> str:
        """"""
        if not self.trading:
            return ""

        algoid: str = self.strategy_engine.start_algo(
            self,
            self.spread_name,
            direction,
            offset,
            price,
            volume,
            payup,
            interval,
            lock
        )

        """" modify by loe """
        if offset == Offset.CLOSE and self.close_anyway:
            the_algo = self.strategy_engine.get_algo(algoid=algoid)
            if the_algo:
                the_algo.close_anyway = True

        self.algoids.add(algoid)

        """ modify by loe """
        if offset == Offset.OPEN:
            if direction == Direction.LONG:
                self.buy_algoids_list.add(algoid)

            elif direction == Direction.SHORT:
                self.short_algoids_list.add(algoid)

        elif offset == Offset.CLOSE or offset == Offset.CLOSETODAY or offset == Offset.CLOSEYESTERDAY:
            if direction == Direction.LONG:
                self.cover_algoids_list.add(algoid)

            elif direction == Direction.SHORT:
                self.sell_algoids_list.add(algoid)

        return algoid

    def start_long_algo(
        self,
        price: float,
        volume: float,
        payup: int,
        interval: int,
        lock: bool = False,
        offset: Offset = Offset.NONE
    ) -> str:
        """"""
        return self.start_algo(
            Direction.LONG, price, volume,
            payup, interval, lock, offset
        )

    def start_short_algo(
        self,
        price: float,
        volume: float,
        payup: int,
        interval: int,
        lock: bool = False,
        offset: Offset = Offset.NONE
    ) -> str:
        """"""
        return self.start_algo(
            Direction.SHORT, price, volume,
            payup, interval, lock, offset
        )

    def stop_algo(self, algoid: str):
        """"""
        if not self.trading:
            return

        self.strategy_engine.stop_algo(self, algoid)

    def stop_all_algos(self):
        """"""
        for algoid in list(self.algoids):
            self.stop_algo(algoid)

    def buy(self, vt_symbol: str, price: float, volume: float, lock: bool = False) -> List[str]:
        """"""
        return self.send_order(vt_symbol, price, volume, Direction.LONG, Offset.OPEN, lock)

    def sell(self, vt_symbol: str, price: float, volume: float, lock: bool = False) -> List[str]:
        """"""
        return self.send_order(vt_symbol, price, volume, Direction.SHORT, Offset.CLOSE, lock)

    def short(self, vt_symbol: str, price: float, volume: float, lock: bool = False) -> List[str]:
        """"""
        return self.send_order(vt_symbol, price, volume, Direction.SHORT, Offset.OPEN, lock)

    def cover(self, vt_symbol: str, price: float, volume: float, lock: bool = False) -> List[str]:
        """"""
        return self.send_order(vt_symbol, price, volume, Direction.LONG, Offset.CLOSE, lock)

    def send_order(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        direction: Direction,
        offset: Offset,
        lock: bool
    ) -> List[str]:
        """"""
        if not self.trading:
            return []

        vt_orderids: List[str] = self.strategy_engine.send_order(
            self,
            vt_symbol,
            price,
            volume,
            direction,
            offset,
            lock
        )

        for vt_orderid in vt_orderids:
            self.vt_orderids.add(vt_orderid)

        return vt_orderids

    def cancel_order(self, vt_orderid: str):
        """"""
        if not self.trading:
            return

        self.strategy_engine.cancel_order(self, vt_orderid)

    def cancel_all_orders(self):
        """"""
        for vt_orderid in self.vt_orderids:
            self.cancel_order(vt_orderid)

    def put_event(self):
        """"""
        self.strategy_engine.put_strategy_event(self)

    def write_log(self, msg: str):
        """"""
        self.strategy_engine.write_strategy_log(self, msg)

    def get_spread_tick(self) -> TickData:
        """"""
        return self.spread.to_tick()

    def get_leg_tick(self, vt_symbol: str) -> TickData:
        """"""
        leg = self.spread.legs.get(vt_symbol, None)

        if not leg:
            return None

        return leg.tick

    def get_leg_pos(self, vt_symbol: str, direction: Direction = Direction.NET) -> float:
        """"""
        leg = self.spread.legs.get(vt_symbol, None)

        if not leg:
            return None

        if direction == Direction.NET:
            return leg.net_pos
        elif direction == Direction.LONG:
            return leg.long_pos
        else:
            return leg.short_pos

    def send_email(self, msg: str):
        """
        Send email to default receiver.
        """
        if self.inited:
            self.strategy_engine.send_strategy_email(self, msg=msg)

    def load_bar(
        self,
        days: int,
        interval: Interval = Interval.MINUTE,
        callback: Callable = None,
    ):
        """
        Load historical bar data for initializing strategy.
        """
        if not callback:
            callback = self.on_spread_bar

        self.strategy_engine.load_bar(self.spread, days, interval, callback)

    def load_recent_bar(
        self,
        count: int,
        interval: Interval = Interval.MINUTE,
        callback: Callable = None,
    ):
        """
        Load historical bar data for initializing strategy.
        """
        if not callback:
            callback = self.on_spread_bar

        self.strategy_engine.load_recent_bar(self.spread, count, interval, callback)

    def load_tick(self, days: int):
        """
        Load historical tick data for initializing strategy.
        """
        self.strategy_engine.load_tick(self.spread, days, self.on_spread_tick)

    """ modify by loe """
    def check_and_stop_other_algo(self, algo: SpreadAlgoTemplate):
        # 只要开仓算法的一条腿有任何成交，立刻停止其他开仓算法算法
        if (algo.algoid in self.short_algoids_list or algo.algoid in self.buy_algoids_list) and algo.check_leg_traded():
            for short_algoid in self.short_algoids_list:
                if short_algoid != algo.algoid:
                    """
                    # 需要停止的算法初始化时间间隔不超过10秒
                    short_algo = self.strategy_engine.get_algo(algoid=short_algoid)
                    if short_algo:
                        delta = abs(algo.init_datetime - short_algo.init_datetime)
                        if delta >= timedelta(seconds=10):
                            continue
                    """

                    self.stop_algo(short_algoid)

            for buy_algoid in self.buy_algoids_list:
                if buy_algoid != algo.algoid:
                    """
                    # 需要停止的算法初始化时间间隔不超过10秒
                    buy_algo = self.strategy_engine.get_algo(algoid=buy_algoid)
                    if buy_algo:
                        delta = abs(algo.init_datetime - buy_algo.init_datetime)
                        if delta >= timedelta(seconds=10):
                            continue
                    """

                    self.stop_algo(buy_algoid)

    def check_algo_trading(self):
        # 检查是否有算法部分成交但是没有全部成交
        result = False
        for algoid in self.algoids:
            algo = self.strategy_engine.get_algo(algoid=algoid)
            if algo.check_leg_traded() and algo.is_active():
                result = True
                break
        return result

    def check_algo_hedge_finished(self):
        # 检查是否有算法断腿
        result = True
        for algoid in self.algoids:
            algo = self.strategy_engine.get_algo(algoid=algoid)
            if not algo.check_hedge_finished():
                result = False
                break
        return result

    def check_algo_order_finished(self):
        # 检查是否有算法存在正在处理的订单
        result = True
        for algoid in self.algoids:
            algo = self.strategy_engine.get_algo(algoid=algoid)
            if not algo.check_order_finished():
                result = False
                break
        return result

    def on_traded_changed(self, algo: SpreadAlgoTemplate, changed=0):
        pass

    def put_timer_event(self):
        self.timer_event_cross = True

    def on_timer(self):
        if self.timer_event_cross:
            self.put_event()
            self.timer_event_cross = False