from vnpy.trader.constant import Direction, Offset, Status
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine
from ..template import AlgoTemplate
import math
import numpy as np
import pandas as pd
import decimal
from vnpy.trader.utility import round_to

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
        "long_orderids",
        "short_orderids",
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
        self.check_enable = False
        self.long_orderids = set()
        self.short_orderids = set()
        self.last_tick = None
        self.grid = None
        self.bestLimitAlgo_names = set()
        self.reject_order_count = 0
        self.cancel_orderids = []

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
                "grid_count":1000,
                "grid_price": 10.0,
                "grid_volume": 0.01,
                "interval": 10
                }

    """ modify by loe """
    def on_start(self):
        self.creat_grid()
        self.saveSyncData()

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

    def on_tick(self, tick: TickData):
        """"""
        if self.last_tick and self.last_tick.datetime >= tick.datetime:
            return
        self.last_tick = tick

        if self.check_enable:
            self.check_long_short_order()
            self.check_enable = False

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

    def check_position(self):
        if not self.last_tick:
            return

        if self.bestLimitAlgo_names:
            return

        if self.long_orderids or self.short_orderids:
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

                long_orderid = self.buy(vt_symbol=self.vt_symbol,
                                        price=price_front,
                                        volume=self.grid_volume,
                                        offset=offset)
                self.long_orderids.add(long_orderid)

            # 挂空单
            if index_after < len(grid_pos_array):
                price_after = grid_price_array[index_after]
                if self.pos > 0:
                    offset = Offset.CLOSE
                else:
                    offset = Offset.OPEN

                short_orderid = self.sell(vt_symbol=self.vt_symbol,
                                          price=price_after,
                                          volume=self.grid_volume,
                                          offset=offset)
                self.short_orderids.add(short_orderid)

        else:
            # 计算委托量
            long_open_volume = 0
            long_close_volume = 0
            short_open_volume = 0
            short_close_volume = 0
            if target_pos >= 0:
                distance = float(decimal.Decimal(str(target_pos)) - decimal.Decimal(str(self.pos)))
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
                distance = float(decimal.Decimal(str(target_pos)) - decimal.Decimal(str(self.pos)))
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

    def get_long_short_target(self, tick:TickData):
        long_price = None
        long_target = None
        short_price = None
        short_target = None

        grid_price_array = self.grid.index
        grid_pos_array = self.grid.values
        # 确定多单目标仓位
        if tick.bid_price_1:
            long_index_array = np.argwhere(grid_price_array < tick.bid_price_1)
            if len(long_index_array):
                long_index_result = long_index_array[-1][-1]
                long_price = grid_price_array[long_index_result]
                long_target = grid_pos_array[long_index_result]
                if long_target <= self.pos:
                    long_price = None
                    long_target = None
                    long_index_array = np.argwhere(grid_pos_array > self.pos)
                    if len(long_index_array):
                        long_index_result = long_index_array[-1][-1]
                        long_price = grid_price_array[long_index_result]
                        long_target = grid_pos_array[long_index_result]

        # 确定空单目标仓位
        if tick.ask_price_1:
            short_index_array = np.argwhere(grid_price_array > tick.ask_price_1)
            if len(short_index_array):
                short_index_result = short_index_array[0][0]
                short_price = grid_price_array[short_index_result]
                short_target = grid_pos_array[short_index_result]
                if short_target >= self.pos:
                    short_price = None
                    short_target = None
                    short_index_array = np.argwhere(grid_pos_array < self.pos)
                    if len(short_index_array):
                        short_index_result = short_index_array[0][0]
                        short_price = grid_price_array[short_index_result]
                        short_target = grid_pos_array[short_index_result]

        long_dict = {'long_price':long_price,
                     'long_target':long_target}

        short_dict = {'short_price':short_price,
                      'short_target':short_target}

        return long_dict, short_dict

    def check_long_short_order(self):
        if not self.last_tick:
            return

        if self.long_orderids or self.short_orderids:
            return

        long_dict, short_dict = self.get_long_short_target(tick=self.last_tick)
        long_price = long_dict['long_price']
        long_target = long_dict['long_target']
        short_price = short_dict['short_price']
        short_target = short_dict['short_target']

        long_open_volume = 0
        long_close_volume = 0
        short_open_volume = 0
        short_close_volume = 0

        # 计算多单委托参数
        if long_target:
            distance = float(decimal.Decimal(str(long_target)) - decimal.Decimal(str(self.pos)))
            if distance <= 0:
                # 检查逻辑错误
                self.write_log('check_long_short_order检查逻辑错误')
                return

            if self.pos >= 0:
                long_open_volume = distance
            elif distance > abs(self.pos):
                long_open_volume = float(decimal.Decimal(str(distance)) - decimal.Decimal(str(abs(self.pos))))
                long_close_volume = abs(self.pos)
            else:
                long_close_volume = distance

        # 计算空单委托参数
        if short_target:
            distance = float(decimal.Decimal(str(self.pos)) - decimal.Decimal(str(short_target)))
            if distance <= 0:
                # 检查逻辑错误
                self.write_log('check_long_short_order检查逻辑错误')
                return

            if self.pos <= 0:
                short_open_volume = distance
            elif distance > abs(self.pos):
                short_open_volume = float(decimal.Decimal(str(distance)) - decimal.Decimal(str(abs(self.pos))))
                short_close_volume = abs(self.pos)
            else:
                short_close_volume = distance

        long_open_orderid = ''
        long_close_orderid = ''
        short_open_orderid = ''
        short_close_orderid = ''

        # 发出多单委托
        if long_open_volume:
            long_open_orderid = self.buy(vt_symbol=self.vt_symbol,
                                         price=long_price,
                                         volume=long_open_volume,
                                         offset=Offset.OPEN)

        if long_close_volume:
            long_close_orderid = self.buy(vt_symbol=self.vt_symbol,
                                          price=long_price,
                                          volume=long_close_volume,
                                          offset=Offset.CLOSE)

        # 发出空单委托
        if short_open_volume:
            short_open_orderid = self.sell(vt_symbol=self.vt_symbol,
                                           price=short_price,
                                           volume=short_open_volume,
                                           offset=Offset.OPEN)

        if short_close_volume:
            short_close_orderid = self.sell(vt_symbol=self.vt_symbol,
                                            price=short_price,
                                            volume=short_close_volume,
                                            offset=Offset.CLOSE)

        if long_open_orderid:
            self.long_orderids.add(long_open_orderid)

        if long_close_orderid:
            self.long_orderids.add(long_close_orderid)

        if short_open_orderid:
            self.short_orderids.add(short_open_orderid)

        if short_close_orderid:
            self.short_orderids.add(short_close_orderid)

    def on_timer(self):
        """"""
        self.timer_count += 1
        if self.timer_count >= self.interval:
            self.timer_count = 0
            self.cancel_all()

        """
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
        """
        self.check_enable = True

        self.put_variables_event()
        self.saveSyncData()

    def on_order(self, order: OrderData):
        """"""
        if (order.vt_orderid in self.long_orderids or order.vt_orderid in self.short_orderids) and not order.is_active():
            if order.vt_orderid in self.long_orderids:
                self.long_orderids.remove(order.vt_orderid)
            if order.vt_orderid in self.short_orderids:
                self.short_orderids.remove(order.vt_orderid)

            if not self.long_orderids or not self.short_orderids:
                self.cancel_all()

            if order.status == Status.REJECTED:
                self.reject_order_count += 1
                # 异常风控
                if self.reject_order_count >= 10:
                    self.stop()


            self.put_variables_event()
            self.saveSyncData()

    def on_trade(self, trade: TradeData):
        """"""
        # 仓位确定
        if trade.direction == Direction.LONG:
            self.pos = float(decimal.Decimal(str(self.pos)) + decimal.Decimal(str(trade.volume)))
        else:
            self.pos = float(decimal.Decimal(str(self.pos)) - decimal.Decimal(str(trade.volume)))

        # 保证pos精度正确
        contract = self.algo_engine.get_contract(self, self.vt_symbol)
        if contract:
            self.pos = round_to(self.pos, contract.min_volume)
        self.put_variables_event()

    """ modify by loe """
    def cancel_order(self, vt_orderid: str):
        """"""
        if vt_orderid in self.cancel_orderids:
            return
        self.cancel_orderids.append(vt_orderid)
        if len(self.cancel_orderids) >= 10:
            self.cancel_orderids.pop(0)

        super().cancel_order(vt_orderid=vt_orderid)
