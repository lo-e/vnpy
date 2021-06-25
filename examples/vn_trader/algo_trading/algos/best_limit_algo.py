from random import uniform

from vnpy.trader.constant import Offset, Direction
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine
from vnpy.trader.utility import round_to
from vnpy.trader.constant import Status

from ..template import AlgoTemplate


class BestLimitAlgo(AlgoTemplate):
    """"""
    def __init__(
        self,
        algo_engine: BaseEngine,
        algo_name: str,
        setting: dict
    ):
        """"""
        super().__init__(algo_engine, algo_name, setting)

        # Parameters
        self.strategy = setting['strategy']
        self.vt_symbol = setting["vt_symbol"]
        self.direction = Direction(setting["direction"])
        self.volume = setting["volume"]
        self.offset = Offset(setting["offset"])

        # Variables
        self.vt_orderid = ""
        self.buffer_orderid = ""
        self.traded = 0
        self.last_tick = None
        self.mark_price = 0
        self.reject_order_count = 0

        # 初始化tick数据开始发出委托
        init_tick = setting.get('tick', None)
        if init_tick:
            self.on_tick(init_tick)

        self.subscribe(self.vt_symbol)

    def on_tick(self, tick: TickData):
        """"""
        self.last_tick = tick

        if self.direction == Direction.LONG:
            if not self.vt_orderid and not self.buffer_orderid:
                self.buy_best_limit()
            elif self.vt_orderid and self.buffer_orderid and self.mark_price != self.last_tick.ask_price_1:
                self.cancel_all()
                self.vt_orderid = ""
        else:
            if not self.vt_orderid and not self.buffer_orderid:
                self.sell_best_limit()
            elif self.vt_orderid and self.buffer_orderid and self.mark_price != self.last_tick.bid_price_1:
                self.cancel_all()
                self.vt_orderid = ""

    def on_trade(self, trade: TradeData):
        """"""
        self.traded += trade.volume

        if self.traded >= self.volume:
            self.write_log(f"已交易数量：{self.traded}，总数量：{self.volume}")
            self.stop()

    def on_order(self, order: OrderData):
        """"""
        #if not order.is_active():
        if order.is_failed():
            self.vt_orderid = ""
            self.buffer_orderid = ""
            self.mark_price = 0
            if order.status == Status.REJECTED:
                self.reject_order_count  += 1
                # 异常风控
                if self.reject_order_count >= 10:
                    self.stop()

    def buy_best_limit(self):
        """"""
        volume_left = self.volume - self.traded
        order_volume = volume_left

        # 挂单委托价格计算方式选其一
        #"""
        contract = self.algo_engine.get_contract(self, self.vt_symbol)
        if contract:
            self.mark_price = self.last_tick.ask_price_1
            order_price = self.last_tick.ask_price_1 - contract.pricetick
        else:
            return
        #"""

        #self.mark_price = self.last_tick.bid_price_1
        #order_price = self.last_tick.bid_price_1

        self.vt_orderid = self.buy(
            self.strategy,
            self.vt_symbol,
            order_price,
            order_volume,
            offset=self.offset
        )
        self.buffer_orderid = self.vt_orderid

    def sell_best_limit(self):
        """"""
        volume_left = self.volume - self.traded
        order_volume = volume_left

        # 挂单委托价格计算方式选其一
        # """
        contract = self.algo_engine.get_contract(self, self.vt_symbol)
        if contract:
            self.mark_price = self.last_tick.bid_price_1
            order_price = self.last_tick.bid_price_1 + contract.pricetick
        else:
            return
        # """

        #self.mark_price = self.last_tick.ask_price_1
        #order_price = self.last_tick.ask_price_1

        self.vt_orderid = self.sell(
            self.strategy,
            self.vt_symbol,
            order_price,
            order_volume,
            offset=self.offset
        )
        self.buffer_orderid = self.vt_orderid
