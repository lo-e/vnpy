from vnpy.trader.constant import Direction
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine
from ..template import AlgoTemplate
import math
import numpy as np
import pandas as pd

class GridAlgo(AlgoTemplate):
    """"""

    display_name = "Grid 网格"

    default_setting = {
        "editable": [
            "是",
            "否"
        ],
        "vt_symbol": "BTCUSDT.BYBIT",
        "capital":0.0,
        "guide_price": 0.0,
        "grid_count":0,
        "grid_price": 0.0,
        "grid_volume": 0.0,
        "interval": 10
    }

    variables = [
        "pos",
        "timer_count",
        "long_orderid",
        "short_orderid"
    ]

    def __init__(
        self,
        algo_engine: BaseEngine,
        algo_name: str,
        setting: dict
    ):
        """"""
        super().__init__(algo_engine, algo_name, setting)

        # Parameters
        editable_text = setting['editable']
        if editable_text == "是":
            self.editable = True
        else:
            self.editable = False
        self.vt_symbol = setting["vt_symbol"]
        self.capital = setting['capital']
        self.guide_price = setting["guide_price"]
        self.grid_count = setting['grid_count']
        self.grid_price = setting["grid_price"]
        self.grid_volume = setting["grid_volume"]
        self.interval = setting["interval"]

        # Variables
        self.pos = 0
        self.timer_count = 0
        self.long_orderid = ""
        self.short_orderid = ""
        self.last_tick = None
        self.grid = None

        self.subscribe(self.vt_symbol)
        self.put_parameters_event()
        self.put_variables_event()

    """ modify by loe """
    @classmethod
    def auto_parameters(cls):
        return {'editable':'否',
                "vt_symbol": "BTCUSDT.BYBIT",
                "capital":10000.0,
                "guide_price": 33000.0,
                "grid_count":100,
                "grid_price": 100.0,
                "grid_volume": 0.001,
                "interval": 10
                }

    """ modify by loe """
    def on_start(self):
        self.creat_grid()

    # 创建网格
    def creat_grid(self):
        if self.guide_price <= 0 or self.grid_count <= 0 or self.grid_price <= 0 or self.grid_volume <= 0:
            self.active = False
            return

        # 价格数列
        grid_price_up = self.guide_price + (self.grid_count + 1) * self.grid_price
        grid_price_down = self.guide_price - self.grid_count * self.grid_price
        if grid_price_down <= 0:
            self.active = False
            return
        grid_price_array = np.arange(grid_price_down, grid_price_up, self.grid_price)

        # 仓位数列
        grid_pos_up = self.grid_count * self.grid_volume
        grid_pos_down = (self.grid_count + 1) * self.grid_volume * -1
        grid_pos_array = np.arange(grid_pos_up, grid_pos_down, -1 * self.grid_volume)

        # 网格
        self.grid = pd.Series(grid_pos_array, index=grid_price_array)

    def get_target_pos(self, tick_price):
        grid_price_array = self.grid.index
        # 网格价格序列中最接近tick_price的值
        result = min(grid_price_array, key=lambda x: abs(x - tick_price))
        # 获取最接近 tick_price值的前后下标
        index_result = list(grid_price_array).index(result)
        index_front = index_result - 1
        index_after = index_result + 1
        # 根据当前仓位决定目标仓位
        if index_front >= 0:
            price_front = grid_price_array[index_front]
            pos_front = self.grid[price_front]
            if pos_front == self.pos:
                return pos_front

        if index_after < len(grid_price_array):
            price_after = grid_price_array[index_after]
            pos_after = self.grid[price_after]
            if pos_after == self.pos:
                return pos_after

        pos_result = self.grid[result]
        return pos_result

    def on_tick(self, tick: TickData):
        """"""
        self.last_tick = tick
        target_pos = self.get_target_pos(tick.last_price)
        a = 2

    def on_timer(self):
        """"""
        return

        if not self.last_tick:
            return

        self.timer_count += 1
        if self.timer_count < self.interval:
            self.put_variables_event()
            return
        self.timer_count = 0

        if self.vt_orderid:
            self.cancel_all()

        # Calculate target volume to buy and sell
        target_buy_distance = (
            self.grid_price - self.last_tick.ask_price_1) / self.grid_price
        target_buy_position = math.floor(
            target_buy_distance) * self.grid_volume
        target_buy_volume = target_buy_position - self.pos

        # Calculate target volume to sell
        target_sell_distance = (
            self.grid_price - self.last_tick.bid_price_1) / self.grid_price
        target_sell_position = math.ceil(
            target_sell_distance) * self.grid_volume
        target_sell_volume = self.pos - target_sell_position

        # Buy when price dropping
        if target_buy_volume > 0:
            self.vt_orderid = self.buy(
                self.vt_symbol,
                self.last_tick.ask_price_1,
                min(target_buy_volume, self.last_tick.ask_volume_1)
            )
        # Sell when price rising
        elif target_sell_volume > 0:
            self.vt_orderid = self.sell(
                self.vt_symbol,
                self.last_tick.bid_price_1,
                min(target_sell_volume, self.last_tick.bid_volume_1)
            )

        # Update UI
        self.put_variables_event()

    def on_order(self, order: OrderData):
        """"""
        if not order.is_active():
            self.vt_orderid = ""
            self.put_variables_event()

    def on_trade(self, trade: TradeData):
        """"""
        if trade.direction == Direction.LONG:
            self.pos += trade.volume
        else:
            self.pos -= trade.volume

        self.put_variables_event()
