""""""
from abc import ABC
from copy import copy
from typing import Any, Callable

from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import virtual

from .base import StopOrder, EngineType, EXCHANGE_SYMBOL_DICT

""" modify by loe """
import re
from pymongo import MongoClient
from vnpy.app.cta_strategy.base import TICK_DB_NAME, ORDER_DB_NAME, TRADE_DB_NAME
from copy import copy

""" modify by loe """
from collections import defaultdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
from vnpy.trader.constant import Exchange
from datetime import datetime, timedelta

class TradeMode(Enum):
    """
    TradeMode of the trade.
    """
    BACKTESTING = "backtesting"     # 回测
    ACTUAL = "actual"               # 实盘

class CtaTemplate(ABC):
    """"""

    author = ""
    parameters = []
    variables = []
    """ modify by loe """
    syncs = []
    max_bond_dic = defaultdict(int) #{'date':date, 'pos':pos, 'bond':bond}
    trade_mode = None
    tick_price:float = 1.0

    mongoClient = None
    timer_event_cross = False

    def __init__(
        self,
        cta_engine: Any,
        strategy_name: str,
        vt_symbol: str,
        setting: dict,
    ):
        """"""
        """ modify by loe """
        if 'testing' in cta_engine.__class__.__name__:
            self.trade_mode = TradeMode.BACKTESTING
        else:
            self.trade_mode = TradeMode.ACTUAL

        self.cta_engine = cta_engine
        self.strategy_name = strategy_name
        self.vt_symbol = vt_symbol
        self.thread_executor = ThreadPoolExecutor(max_workers=10)

        self.inited = False
        self.trading = False
        self.pos = 0

        """ modify by loe """
        self.parameters = copy(self.parameters)
        if 'tick_price' not in self.parameters:
            self.parameters.insert(0, "tick_price")

        self.variables = copy(self.variables)
        if 'inited' not in self.variables:
            self.variables.insert(0, "inited")
        if 'trading' not in self.variables:
            self.variables.insert(1, "trading")
        if 'pos' not in self.variables:
            self.variables.insert(2, "pos")

        self.syncs = copy(self.syncs)
        if 'pos' not in self.syncs:
            self.syncs.insert(0, "pos")

        self.update_setting(setting)
        """ modify by loe """
        self.check_vt_symbol()

    """ modify by loe """
    def check_vt_symbol(self):
        sepList = self.vt_symbol.split('.')
        symbol = sepList[0]
        startSymbol = re.sub("\d", "", symbol).upper()
        for key, value in EXCHANGE_SYMBOL_DICT.items():
            if startSymbol in value:
                self.vt_symbol = '.'.join([symbol, key.value])
                break

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
            "vt_symbol": self.vt_symbol,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

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
    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        pass

    @virtual
    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        pass

    @virtual
    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """

        """ modify by loe """
        try:
            # 邮件提醒
            self.send_email(f'====== 成交 ======\n\n合约：{trade.symbol}\n开平：{trade.offset}\n方向：{trade.direction} \n价格：{trade.price}\n数量：{trade.volume}')
        except:
            pass

        """ modify by loe """
        try:
            # 保存成交数据到数据库
            trade_dic = {'symbol':trade.symbol, 'price': trade.price, 'volume': trade.volume, 'direction': trade.direction.value, 'offset': trade.offset.value}
            self.saveTradeToDb(trade_dic)
        except:
            pass

        pass

    @virtual
    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    @virtual
    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass

    def buy(self, price: float, volume: float, stop: bool = False, lock: bool = False):
        """
        Send buy order to open a long position.
        """
        return self.send_order(Direction.LONG, Offset.OPEN, price, volume, stop, lock)

    def sell(self, price: float, volume: float, stop: bool = False, lock: bool = False):
        """
        Send sell order to close a long position.
        """
        return self.send_order(Direction.SHORT, Offset.CLOSE, price, volume, stop, lock)

    def short(self, price: float, volume: float, stop: bool = False, lock: bool = False):
        """
        Send short order to open as short position.
        """
        return self.send_order(Direction.SHORT, Offset.OPEN, price, volume, stop, lock)

    def cover(self, price: float, volume: float, stop: bool = False, lock: bool = False):
        """
        Send cover order to close a short position.
        """
        return self.send_order(Direction.LONG, Offset.CLOSE, price, volume, stop, lock)

    def send_order(
        self,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        stop: bool = False,
        lock: bool = False
    ):
        """
        Send a new order.
        """
        if self.trading:
            vt_orderids = self.cta_engine.send_order(
                self, direction, offset, price, volume, stop, lock
            )

            """ modify by loe """
            try:
                # 邮件提醒
                self.send_email(f'====== 委托 ======\n\n合约：{self.vt_symbol}\n开平：{offset}\n方向：{direction} \n价格：{price}\n数量：{volume}')
            except:
                pass


            """ modify by loe """
            try:
                # 保存委托数据到数据库
                order_dic = {'symbol':self.vt_symbol, 'price': price, 'volume': volume, 'direction': direction.value, 'offset': offset.value}
                self.saveOrderToDb(order_dic)
            except:
                pass

            return vt_orderids
        else:
            return []

    def cancel_order(self, vt_orderid: str):
        """
        Cancel an existing order.
        """
        if self.trading:
            self.cta_engine.cancel_order(self, vt_orderid)

    def cancel_all(self):
        """
        Cancel all orders sent by strategy.
        """
        if self.trading:
            self.cta_engine.cancel_all(self)

    def write_log(self, msg: str):
        """
        Write a log message.
        """
        self.cta_engine.write_log(msg, self)

    def get_engine_type(self):
        """
        Return whether the cta_engine is backtesting or live trading.
        """
        return self.cta_engine.get_engine_type()

    def get_pricetick(self):
        """
        Return pricetick data of trading contract.
        """
        return self.cta_engine.get_pricetick(self)

    def load_bar(
        self,
        days: int,
        interval: Interval = Interval.MINUTE,
        callback: Callable = None,
        use_database: bool = False
    ):
        """
        Load historical bar data for initializing strategy.
        """
        if not callback:
            callback = self.on_bar

        return self.cta_engine.load_bar(self.vt_symbol, days, interval, callback)

    def load_tick(self, days: int):
        """
        Load historical tick data for initializing strategy.
        """
        self.cta_engine.load_tick(self.vt_symbol, days, self.on_tick)

    def put_event(self):
        """
        Put an strategy data event for ui update.
        """
        if self.inited:
            self.cta_engine.put_strategy_event(self)

    def send_email(self, msg):
        """
        Send email to default receiver.
        """
        if self.inited:
            self.cta_engine.send_email(msg, self)

    def sync_data(self):
        """
        Sync strategy variables value into disk storage.
        """
        if self.trading:
            self.cta_engine.sync_strategy_data(self)

    """ modify by loe """
    def sendSymbolOrder(self, symbol, direction, offset, price, volume, stop=False):
        """发送委托"""
        if self.trading:
            vt_orderids = self.cta_engine.send_symbol_order(
                self, symbol, direction, offset, price, volume, stop, False)

            """ modify by loe """
            try:
                # 邮件提醒
                self.send_email(f'====== 委托 ======\n\n合约：{symbol}\n开平：{offset}\n方向：{direction} \n价格：{price}\n数量：{volume}')
            except:
                pass

            """ modify by loe """
            try:
                # 保存委托数据到数据库
                order_dic = {'symbol': symbol, 'price': price, 'volume': volume, 'direction': direction.value,
                             'offset': offset.value}
                self.saveOrderToDb(order_dic)
            except:
                pass

            return vt_orderids
        else:
            # 交易停止时发单返回空字符串
            return []

    """ modify by loe """
    def getMongoClient(self):
        if not self.mongoClient:
            self.mongoClient = MongoClient('localhost', 27017, serverSelectionTimeoutMS=600)
        return self.mongoClient

    # 保存tick到数据库
    def saveTick(self, tick:TickData):
        self.thread_executor.submit(self.do_save_tick, tick)

    def do_save_tick(self, tick:TickData):
        try:
            # 交易所枚举类型无法保存数据库，先转换成字符串
            temp = copy(tick)

            if temp.last_price == 0:
                # 目前发现Bybit接口tick数据会出现价格为0的情况
                self.write_log(f'TICK价格出错\n{tick.__dict__}')
                return

            if temp.exchange == Exchange.BYBIT:
                # Bybit交易所的时间需要调整为北京时间
                temp.datetime = temp.datetime + timedelta(hours=8)
            temp.exchange = temp.exchange.value
            temp.symbol = temp.symbol.upper()
            client = self.getMongoClient()
            tick_db = client[TICK_DB_NAME]
            collection = tick_db[temp.symbol]
            collection.create_index('datetime')
            collection.update_many({'datetime': temp.datetime}, {'$set': temp.__dict__}, upsert=True)
        except:
            pass

    # 计算最佳委托价格
    def bestLimitOrderPrice(self, tick, direction, multi=20):
        if direction == Direction.LONG:
            if tick.limit_up:
                price = min(tick.limit_up, tick.last_price + self.tick_price * multi)
                #price = min(tick.limit_up, tick.last_price * 1.06)
            else:
                price = tick.last_price + self.tick_price * multi
                #price = tick.last_price * 1.06
            return price

        if direction == Direction.SHORT:
            if tick.limit_down:
                price = max(tick.limit_down, tick.last_price - self.tick_price * multi)
                #price = max(tick.limit_down, tick.last_price * 0.94)
            else:
                price = tick.last_price - self.tick_price * multi
                #price = tick.last_price * 0.94
            return price

        return 0

    # 保存委托数据到数据库
    def saveOrderToDb(self, order_dict:dict):
        self.thread_executor.submit(self.do_save_order, order_dict)

    def do_save_order(self, order_dict:dict):
        try:
            strategy_variables = self.get_variables()
            d = {'datetime':datetime.now(),
                 'order_data':order_dict,
                 'variables':strategy_variables}
            client = self.getMongoClient()
            order_db = client[ORDER_DB_NAME]
            collection = order_db[self.strategy_name]
            collection.insert_one(d)
        except:
            pass

    # 保存成交数据到数据库
    def saveTradeToDb(self, trade_dic:dict):
        self.thread_executor.submit(self.do_save_trade, trade_dic)

    def do_save_trade(self, trade_dic:dict):
        try:
            strategy_variables = self.get_variables()
            d = {'datetime':datetime.now(),
                 'trade_data':trade_dic,
                 'variables':strategy_variables}
            client = self.getMongoClient()
            trade_db = client[TRADE_DB_NAME]
            collection = trade_db[self.strategy_name]
            collection.insert_one(d)
        except:
            pass

    """ modify by loe """
    def put_timer_event(self):
        self.timer_event_cross = True

    def on_timer(self):
        if self.timer_event_cross:
            self.put_event()
            self.timer_event_cross = False

class CtaSignal(ABC):
    """"""

    def __init__(self):
        """"""
        self.signal_pos = 0

    @virtual
    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        pass

    @virtual
    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        pass

    def set_signal_pos(self, pos):
        """"""
        self.signal_pos = pos

    def get_signal_pos(self):
        """"""
        return self.signal_pos


class TargetPosTemplate(CtaTemplate):
    """"""
    tick_add = 1

    last_tick = None
    last_bar = None
    target_pos = 0

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.active_orderids = []
        self.cancel_orderids = []

        self.variables.append("target_pos")

    @virtual
    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        self.last_tick = tick

        if self.trading:
            self.trade()

    @virtual
    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        self.last_bar = bar

    @virtual
    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        vt_orderid = order.vt_orderid

        if not order.is_active():
            if vt_orderid in self.active_orderids:
                self.active_orderids.remove(vt_orderid)

            if vt_orderid in self.cancel_orderids:
                self.cancel_orderids.remove(vt_orderid)

    def check_order_finished(self):
        """"""
        if self.active_orderids:
            return False
        else:
            return True

    def set_target_pos(self, target_pos):
        """"""
        self.target_pos = target_pos
        self.trade()

    def trade(self):
        """"""
        if not self.check_order_finished():
            self.cancel_old_order()
        else:
            self.send_new_order()

    def cancel_old_order(self):
        """"""
        for vt_orderid in self.active_orderids:
            if vt_orderid not in self.cancel_orderids:
                self.cancel_order(vt_orderid)
                self.cancel_orderids.append(vt_orderid)

    def send_new_order(self):
        """"""
        pos_change = self.target_pos - self.pos
        if not pos_change:
            return

        long_price = 0
        short_price = 0

        if self.last_tick:
            if pos_change > 0:
                long_price = self.last_tick.ask_price_1 + self.tick_add
                if self.last_tick.limit_up:
                    long_price = min(long_price, self.last_tick.limit_up)
            else:
                short_price = self.last_tick.bid_price_1 - self.tick_add
                if self.last_tick.limit_down:
                    short_price = max(short_price, self.last_tick.limit_down)

        else:
            if pos_change > 0:
                long_price = self.last_bar.close_price + self.tick_add
            else:
                short_price = self.last_bar.close_price - self.tick_add

        if self.get_engine_type() == EngineType.BACKTESTING:
            if pos_change > 0:
                vt_orderids = self.buy(long_price, abs(pos_change))
            else:
                vt_orderids = self.short(short_price, abs(pos_change))
            self.active_orderids.extend(vt_orderids)

        else:
            if self.active_orderids:
                return

            if pos_change > 0:
                if self.pos < 0:
                    if pos_change < abs(self.pos):
                        vt_orderids = self.cover(long_price, pos_change)
                    else:
                        vt_orderids = self.cover(long_price, abs(self.pos))
                else:
                    vt_orderids = self.buy(long_price, abs(pos_change))
            else:
                if self.pos > 0:
                    if abs(pos_change) < self.pos:
                        vt_orderids = self.sell(short_price, abs(pos_change))
                    else:
                        vt_orderids = self.sell(short_price, abs(self.pos))
                else:
                    vt_orderids = self.short(short_price, abs(pos_change))
            self.active_orderids.extend(vt_orderids)
