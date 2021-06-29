from vnpy.trader.constant import Direction, Offset, Status
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine
from ..template import AlgoTemplate
import math
import numpy as np
import pandas as pd
import decimal

class GridAlgo(AlgoTemplate):
    """"""

    display_name = "Grid 网格"

    default_setting = {
        "editable": [
            "是",
            "否"
        ],
        "vt_symbol": "",
        "capital":0.0,
        "guide_price": 0.0,
        "grid_count":0,
        "grid_price": 0.0,
        "grid_volume": 0.0,
        "interval": 0
    }

    variables = [
        "pos",
        "timer_count",
        "long_orderid",
        "short_orderid",
        'reject_order_count',
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
        self.bestLimitAlgo_names = set()
        self.reject_order_count = 0

        self.subscribe(self.vt_symbol)
        self.put_parameters_event()
        self.put_variables_event()

    """ modify by loe """
    @classmethod
    def auto_parameters(cls):
        return {'editable':'否',
                "vt_symbol": "BTCUSDT.BYBIT",
                "capital":10000.0,
                "guide_price": 35000.0,
                "grid_count":100,
                "grid_price": 100.0,
                "grid_volume": 0.001,
                "interval": 2
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
        grid_price_array_decimal = np.arange(decimal.Decimal(str(grid_price_down)), decimal.Decimal(str(grid_price_up)), decimal.Decimal(str(self.grid_price)))
        grid_price_array_float = []
        for decimal_value in grid_price_array_decimal:
            float_value = float(decimal_value)
            grid_price_array_float.append(float_value)
        grid_price_array_float = np.array(grid_price_array_float)

        # 仓位数列
        grid_pos_up = self.grid_count * self.grid_volume
        grid_pos_down = (self.grid_count + 1) * self.grid_volume * -1
        grid_pos_array_decimal = np.arange(decimal.Decimal(str(grid_pos_up)), decimal.Decimal(str(grid_pos_down)), decimal.Decimal(str(-1 * self.grid_volume)))
        grid_pos_array_float = []
        for decimal_value in grid_pos_array_decimal:
            float_value = float(decimal_value)
            grid_pos_array_float.append(float_value)
        grid_pos_array_float = np.array(grid_pos_array_float)

        # 网格
        self.grid = pd.Series(grid_pos_array_float, index=grid_price_array_float)

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

    def check_position(self):
        if not self.last_tick:
            return

        if self.bestLimitAlgo_names:
            return

        if self.long_orderid or self.short_orderid:
            return

        target_pos = self.get_target_pos(self.last_tick.last_price)
        if self.pos == target_pos:
            # 当前价格上方网格挂空单，当前价格下方网格挂多单
            grid_price_array = self.grid.index
            grid_pos_array = self.grid.values
            index_pos = list(grid_pos_array).index(self.pos)
            index_front = index_pos - 1
            index_after = index_pos + 1

            # 挂多单
            if index_front >= 0:
                price_front = grid_price_array[index_front]
                if self.pos < 0:
                    offset = Offset.CLOSE
                else:
                    offset = Offset.OPEN

                self.long_orderid = self.buy(vt_symbol=self.vt_symbol,
                                             price=price_front,
                                             volume=self.grid_volume,
                                             offset=offset)

            # 挂空单
            if index_after < len(grid_pos_array):
                price_after = grid_price_array[index_after]
                if self.pos > 0:
                    offset = Offset.CLOSE
                else:
                    offset = Offset.OPEN

                self.short_orderid = self.sell(vt_symbol=self.vt_symbol,
                                               price=price_after,
                                               volume=self.grid_volume,
                                               offset=offset)

        else:
            # 计算委托量
            long_open_volume = 0
            long_close_volume = 0
            short_open_volume = 0
            short_close_volume = 0
            if target_pos >= 0:
                distance = target_pos - self.pos
                if distance >= target_pos:
                    # 平空 + 开多
                    long_open_volume = target_pos
                    long_close_volume = abs(self.pos)
                elif distance >= 0:
                    # 开多
                    long_open_volume = distance
                else:
                    # 平多
                    short_close_volume = abs(distance)
            else:
                distance = target_pos - self.pos
                if distance <= target_pos:
                    # 平多 + 开空
                    short_open_volume = abs(target_pos)
                    short_close_volume = abs(self.pos)
                elif distance <= 0:
                    # 开空
                    short_open_volume = abs(distance)
                else:
                    # 平空
                    long_close_volume = abs(distance)

            # 最优限价算法开平仓
            if long_open_volume:
                setting = {'template_name': 'BestLimitAlgo',
                           'top_algo': self,
                           'vt_symbol': self.vt_symbol,
                           'direction': '多',
                           'volume': long_open_volume,
                           'offset': '开',
                           'tick': self.last_tick}
                algo_name = self.algo_engine.start_algo(setting=setting)
                self.bestLimitAlgo_names.add(algo_name)

            if long_close_volume:
                setting = {'template_name': 'BestLimitAlgo',
                           'top_algo': self,
                           'vt_symbol': self.vt_symbol,
                           'direction': '多',
                           'volume': long_close_volume,
                           'offset': '平',
                           'tick': self.last_tick}
                algo_name = self.algo_engine.start_algo(setting=setting)
                self.bestLimitAlgo_names.add(algo_name)

            if short_open_volume:
                setting = {'template_name': 'BestLimitAlgo',
                           'top_algo': self,
                           'vt_symbol': self.vt_symbol,
                           'direction': '空',
                           'volume': short_open_volume,
                           'offset': '开',
                           'tick': self.last_tick}
                algo_name = self.algo_engine.start_algo(setting=setting)
                self.bestLimitAlgo_names.add(algo_name)

            if short_close_volume:
                setting = {'template_name': 'BestLimitAlgo',
                           'top_algo': self,
                           'vt_symbol': self.vt_symbol,
                           'direction': '空',
                           'volume': short_close_volume,
                           'offset': '平',
                           'tick': self.last_tick}
                algo_name = self.algo_engine.start_algo(setting=setting)
                self.bestLimitAlgo_names.add(algo_name)

    def on_timer(self):
        """"""
        self.timer_count += 1
        if self.timer_count < self.interval:
            self.put_variables_event()
            return
        self.timer_count = 0

        # 检查最优限价算法
        complete_algo_names = set()
        for algo_name in self.bestLimitAlgo_names:
            algo = self.algo_engine.algos.get(algo_name, None)
            if not algo:
                complete_algo_names.add(algo_name)
        if complete_algo_names:
            self.bestLimitAlgo_names -= complete_algo_names
        # 检查仓位
        self.check_position()

        self.put_variables_event()

    def on_order(self, order: OrderData):
        """"""
        if not order.is_active():
            self.long_orderid = ''
            self.short_orderid = ''
            self.cancel_all()

            if order.status == Status.REJECTED:
                self.reject_order_count += 1
                # 异常风控
                if self.reject_order_count >= 10:
                    self.stop()

        self.put_variables_event()

    def on_trade(self, trade: TradeData):
        """"""
        if trade.direction == Direction.LONG:
            self.pos += trade.volume
        else:
            self.pos -= trade.volume

        self.put_variables_event()
