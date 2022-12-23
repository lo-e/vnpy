from vnpy.trader.constant import Direction, Offset, Status
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine
from ..template import AlgoTemplate
import numpy as np
import pandas as pd
import decimal
from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData
from vnpy.trader.utility import ArrayManager
from enum import Enum
from datetime import datetime, timedelta
from vnpy.trader.utility import round_to, floor_to, ceil_to
from vnpy.trader.utility import BarGenerator
from typing import Callable

TRADE_SYMBOL = 'BTCUSDT.BYBIT'

class GridStatus(Enum):
    """
    Mode of Grid Status.
    """
    OPEN = "开启"
    CLOSE = "关闭"

class GridAlgoNormal(AlgoTemplate):
    """"""

    display_name = "Grid 网格【熊市建仓】"
    AUTO_FLAG = True

    default_setting = {
        "editable": [
            "是",
            "否"
        ],
        "algo_name": "",
        "vt_symbol": "",
        "capital_c": 0.0,
        "grid_volume": 0.0,
        "grid_up": 0.0,
        "grid_down": 0.0,
        "interval": 0
    }

    variables = [
        "pos",
        "timer_count",
        "status",
        "grid_count",
        "grid_price",
        "current_pnl",
        "reject_order_count",
        "long_orderids",
        "short_orderids"
    ]

    syncs = ['pos',
             'current_pnl',
             'setting_data']

    def __init__(
        self,
        algo_engine: BaseEngine,
        algo_name: str,
        setting: dict
    ):
        """"""
        super().__init__(algo_engine, algo_name, setting)
        self.new_setting = setting

        # Parameters
        editable_text = setting['editable']
        if editable_text == "是":
            self.editable = True
        else:
            self.editable = False
        self.algo_name = setting["algo_name"]
        self.vt_symbol = setting["vt_symbol"]
        self.capital_c = setting["capital_c"]
        self.grid_volume = setting["grid_volume"]
        self.grid_up = setting["grid_up"]
        self.grid_down = setting["grid_down"]
        self.interval = setting["interval"]

        # Variables
        self.pos = 0
        self.timer_count = 0
        self.check_enable = False
        self.cancel_ls_enable = True
        self.tick_error = False
        self.long_orderids = set()
        self.short_orderids = set()
        self.last_tick = None
        self.grid = None
        self.grid_count = 0
        self.grid_price = 0
        self.bestLimitAlgo_names = set()
        self.reject_order_count = 0
        self.reject_order_timecounter = 0
        self.current_pnl = 0
        self.setting_data = {}
        self.status = GridStatus.OPEN

        # 订阅行情数据
        self.subscribe(self.vt_symbol)

        self.put_parameters_event()
        self.put_variables_event()

    def reinit(self):
        tick = self.algo_engine.get_tick_subscribe(algo=None, vt_symbol=TRADE_SYMBOL)
        if not tick:
            return False

        setting = self.__class__.auto_parameters(algo_engine=self.algo_engine)
        if setting:
            # Parameters
            self.vt_symbol = setting["vt_symbol"]
            self.capital_c = setting["capital_c"]
            self.grid_volume = setting["grid_volume"]
            self.grid_up = setting["grid_up"]
            self.grid_down = setting["grid_down"]
            self.interval = setting["interval"]

            self.new_setting = setting
            self.status = GridStatus.OPEN
            self.on_start()
            self.cancel_all()
            return True
        else:
            return False

    @classmethod
    # 自动生成算法参数
    def auto_parameters(cls, algo_engine:BaseEngine):
        setting = {
            "editable": "是",
            "algo_name": "BTCUSDT现货网格",
            "vt_symbol": "BTCUSDT.BYBIT",
            "capital_c": 0.006,
            "grid_volume": 0.00005,
            "grid_up": 17000.0,
            "grid_down": 16000.0,
            "interval": 10
        }
        return setting

    @classmethod
    # 一键启动多个算法，返回初始化参数组合
    def one_start(cls, algo_engine:BaseEngine):
        return []

    def check_environment(self):
        contract = self.algo_engine.main_engine.get_contract(vt_symbol=self.vt_symbol)
        if contract and contract.pricetick:
            self.tick_price = contract.pricetick
        else:
            self.write_log(f'tick_price无法确认，停止算法')
            self.stop()
            return

    # 创建网格
    def create_grid(self):
        if self.capital_c <= 0 or self.grid_volume <= 0 or self.grid_up <= 0 or self.grid_down < 0:
            self.active = False
            return

        self.grid_count = int(self.capital_c / self.grid_volume)
        self.grid_price = ceil_to((self.grid_up - self.grid_down) / self.grid_count, 1)

        # 价格数列
        grid_price_up = decimal.Decimal(str(self.grid_down)) + (decimal.Decimal(str(self.grid_count)) + decimal.Decimal(str(1))) * decimal.Decimal(str(self.grid_price))
        grid_price_down = self.grid_down
        grid_price_array_decimal = np.arange(decimal.Decimal(str(grid_price_down)), decimal.Decimal(str(grid_price_up)), decimal.Decimal(str(self.grid_price)))
        grid_price_array_float = []
        for decimal_value in grid_price_array_decimal:
            float_value = float(decimal_value)
            grid_price_array_float.append(float_value)
        grid_price_array_float = np.array(grid_price_array_float)

        # 仓位数列
        grid_pos_up = decimal.Decimal(str(self.grid_count)) * decimal.Decimal(str(self.grid_volume))
        grid_pos_down = decimal.Decimal(str(self.grid_volume)) * decimal.Decimal(str(-1))
        grid_pos_array_decimal = np.arange(decimal.Decimal(str(grid_pos_up)), decimal.Decimal(str(grid_pos_down)), decimal.Decimal(str(-1 * self.grid_volume)))

        grid_pos_array_float = []
        for decimal_value in grid_pos_array_decimal:
            float_value = float(decimal_value)
            grid_pos_array_float.append(float_value)
        grid_pos_array_float = np.array(grid_pos_array_float)

        # 网格
        self.grid = pd.Series(grid_pos_array_float, index=grid_price_array_float)

    def on_start(self):
        # 创建网格
        self.create_grid()

        self.setting_data = self.new_setting
        self.saveSyncData()

        self.put_parameters_event()
        self.put_variables_event()

    def on_tick(self, tick: TickData):
        """"""
        # 买一价或卖一价缺一不可
        if not tick.bid_price_1 or not tick.ask_price_1:
            return

        # 理论上买一价小于卖一价，如果不是，数据可能异常，为了避免taker成交增加手续费成本，不做委托
        if tick.bid_price_1 >= tick.ask_price_1:
            if not self.tick_error:
                self.write_log(f'tick买卖一档数据异常\tbid_price_1： {tick.bid_price_1}\task_price_1：{tick.ask_price_1}')
                self.tick_error = True
            return

        self.last_tick = tick
        if self.check_enable:
            self.check_enable = False
            self.check_long_short_order()

    def get_target_pos(self, the_price):
        grid_price_array = self.grid.index
        # 网格价格序列中最接近the_price的值
        result = min(grid_price_array, key=lambda x: abs(x - the_price))
        # 获取最接近the_price值的前后下标
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

        # 清仓状态
        if self.status == GridStatus.CLOSE and self.pos > 0:
            short_price = tick.bid_price_1 - self.tick_price * 200
            short_target = 0

        elif self.status == GridStatus.OPEN:
            # 确定多单目标仓位
            long_index_array = np.argwhere(grid_price_array < tick.ask_price_1)
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

                        # 风控，委托买价过低当前价位可能会导致一直拒单
                        if long_price < tick.bid_price_1 - self.grid_price * 20:
                            long_price = None
                            long_target = None

            elif tick.ask_price_1 <= grid_price_array[0]:
                # 价格低于grid_down
                long_price = grid_price_array[0]
                long_target = grid_pos_array[0]
                if long_target <= self.pos:
                    long_price = None
                    long_target = None

            # 确定空单目标仓位
            short_index_array = np.argwhere(grid_price_array > tick.bid_price_1)
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

                        # 风控，委托卖价过高当前价位可能会导致一直拒单
                        if short_price > tick.ask_price_1 + self.grid_price * 20:
                            short_price = None
                            short_target = None

            elif tick.bid_price_1 >= grid_price_array[-1]:
                # 价格高于grid_up
                short_price = grid_price_array[-1]
                short_target = grid_pos_array[-1]
                if short_target >= self.pos:
                    short_price = None
                    short_target = None

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
        if long_target != None:
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
        if short_target != None:
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

            # 现货网格不允许做空
            short_open_volume = 0

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
        # 取消订单
        if self.timer_count >= self.interval:
            self.timer_count = 0
            self.cancel_ls_enable = True
            self.cancel_all()

        # 拒单计数
        if self.reject_order_timecounter:
            self.reject_order_timecounter += 1

            if self.reject_order_timecounter > 60 * 10:
                self.reject_order_timecounter = 0
                self.reject_order_count = 0

        # 检查当前环境
        self.check_environment()

        self.check_enable = True
        self.tick_error = False

        self.put_variables_event()
        self.saveSyncData()

    def on_order(self, order: OrderData):
        """"""
        if (order.vt_orderid in self.long_orderids or order.vt_orderid in self.short_orderids) and not order.is_active():
            if order.vt_orderid in self.long_orderids:
                self.long_orderids.remove(order.vt_orderid)
                if order.status == Status.PARTTRADED or order.status == Status.ALLTRADED:
                    self.cancel_short_orders()

            if order.vt_orderid in self.short_orderids:
                self.short_orderids.remove(order.vt_orderid)
                if order.status == Status.PARTTRADED or order.status == Status.ALLTRADED:
                    self.cancel_long_orders()

            if order.status == Status.REJECTED:
                self.reject_order_count += 1

                if not self.reject_order_timecounter:
                    # 开始计时
                    self.reject_order_timecounter = 1

                if self.reject_order_count >= 10:
                    # 异常风控
                    self.stop()

                    subject = '异常风控'
                    content = f'拒单次数过多，停止网格{self.algo_name}'
                    self.send_ding_talk(subject=subject, content=content)

            self.check_enable = True
            self.put_variables_event()
            self.saveSyncData()

    def on_trade(self, trade: TradeData):
        """"""
        last_pos = self.pos

        # 仓位确定
        if trade.direction == Direction.LONG:
            self.pos = float(decimal.Decimal(str(self.pos)) + decimal.Decimal(str(trade.volume)))
        else:
            self.pos = float(decimal.Decimal(str(self.pos)) - decimal.Decimal(str(trade.volume)))

        # 保证pos精度正确
        # contract = self.algo_engine.get_contract(self, self.vt_symbol)
        # if contract:
        #     self.pos = round_to(self.pos, contract.min_volume)

        # 计算当前盈利
        trade_offset = self.algo_engine.orderid_offset_map.get(trade.vt_orderid, None)
        if trade_offset and trade_offset == Offset.CLOSE or trade_offset == Offset.CLOSEYESTERDAY or trade_offset == Offset.CLOSETODAY:
            count = int(round(trade.volume / self.grid_volume, 0))
            actual_volume = 0
            for i in range(count):
                actual_volume += self.grid_volume * (i + 1)
            self.current_pnl += actual_volume * self.grid_price

        self.put_variables_event()
        self.saveSyncData()

    def cancel_long_orders(self):
        if not self.cancel_ls_enable:
            # 限制一个计时周期的执行频率，避免API返回错误
            return

        self.cancel_ls_enable = False
        for vt_orderid in self.long_orderids:
            self.cancel_order(vt_orderid=vt_orderid)

    def cancel_short_orders(self):
        if not self.cancel_ls_enable:
            # 限制一个计时周期的执行频率，避免API返回错误
            return

        self.cancel_ls_enable = False
        for vt_orderid in self.short_orderids:
            self.cancel_order(vt_orderid=vt_orderid)

    def send_ding_talk(self, subject:str, content:str):
        try:
            self.algo_engine.main_engine.send_ding_talk(content=f'主题\n============\n{subject}\n\n内容\n============\n{content}')
        except:
            pass