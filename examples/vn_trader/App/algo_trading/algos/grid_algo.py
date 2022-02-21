from vnpy.trader.constant import Direction, Offset, Status
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine
from ..template import AlgoTemplate
from math import ceil
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
TRADE_CAPITAL = 2000
window_time = ['00:00:00', '08:00:00', '16:00:00']

class Mode(Enum):
    """
    Mode of Grid Trade.
    """
    DAILY = "日内"
    LONG = "长周期"

class GridStatus(Enum):
    """
    Mode of Grid Status.
    """
    OPEN = "开启"
    PREPARE = "准备初始建仓"
    WAITINGCLOSE = "等待信号平仓"
    CLOSE = "关闭"

class GridDirection(Enum):
    """
    Mode of Grid Status.
    """
    LONG = "看涨"
    OPEN = "看涨看跌"
    SHORT = "看跌"

class GridAlgo(AlgoTemplate):
    """"""

    display_name = "Grid 网格"
    AUTO_FLAG = True

    default_setting = {
        "editable": [
            "是",
            "否"
        ],
        "algo_name": "",
        "mode": [
            "日内",
            "长周期"
        ],
        "grid_direction": [
            "看涨",
            "看涨看跌",
            "看跌"
        ],
        "ratio_close": [
            "是",
            "否",
        ],
        "vt_symbol": "",
        "guide_price": 0.0,
        "grid_count": 0,
        "grid_price": 0.0,
        "grid_volume": 0.0,
        "exit_price1":0.0,
        "exit_price2":0.0,
        "interval": 0,
    }

    variables = [
        "pos",
        "pos_reiniting",
        "timer_count",
        "status",
        "grid_direction",
        "max_volume",
        "volume_rate",
        "gridUp",
        "guide_price",
        "stop_price",
        "stop_price_remain",
        "gridDown",
        "exit_price1",
        "exit_price2",
        "reject_order_count",
        "long_orderids",
        "short_orderids"
    ]

    syncs = ['pos',
             'volume_rate',
             'setting_data']

    max_grid_count = 10000
    min_grid_price = 2.0

    # ****** DB模式参数 ******
    # 指标数据参数
    gridWindow = 20

    # ****** CUSTOM模式参数 ******
    # 建仓价位线，将历史最高价下跌多少作为安全建仓价位
    init_line = 0.4
    init_pos_complete = False

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
        self.guide_price = setting["guide_price"]
        self.grid_count = setting['grid_count']
        self.grid_price = setting["grid_price"]
        self.grid_volume = setting["grid_volume"]
        self.interval = setting["interval"]
        self.mode = Mode(setting['mode'])
        self.grid_direction = GridDirection(setting['grid_direction'])
        # 是否资金费率结算前清仓
        ratio_close = setting['ratio_close']
        if ratio_close == "是":
            self.ratio_close = True
        else:
            self.ratio_close = False
        self.exit_price1 = setting.get('exit_price1', 0.0)
        self.exit_price2 = setting.get('exit_price2', 0.0)

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
        self.bestLimitAlgo_names = set()
        self.reject_order_count = 0
        self.reject_order_timecounter = 0
        self.gridUp = 0
        self.gridDown = 0
        self.current_pnl = 0
        self.est_max_loss = 0
        self.est_max_pnl = 0
        self.setting_data = {}
        self.cancel_orderids = []
        self.status = GridStatus.OPEN
        self.max_volume = decimal.Decimal(str(self.grid_volume)) * decimal.Decimal(str(self.grid_count))
        # 停止价格相关
        self.stop_price = self.guide_price
        self.stop_price_timecounter = 0
        self.stop_price_remain = ''
        self.stop_price_init = False

        # 仓位管理相关
        self.volume_rate = 1
        self.pos_reiniting = False
        self.pos_reiniting_target = 0
        self.pos_reiniting_waiting = False

        self.am = ArrayManager(self.gridWindow + 1)

        self.subscribe(self.vt_symbol)
        self.put_parameters_event()
        self.put_variables_event()

    def reinit(self):
        tick = self.algo_engine.get_tick_subscribe(algo=None, vt_symbol=TRADE_SYMBOL)
        if not tick:
            return False

        setting = self.__class__.get_parameters(algo_engine=self.algo_engine,
                                                refer_price=tick.last_price,
                                                grid_direction=self.grid_direction)
        if setting:
            # Parameters
            self.guide_price = setting["guide_price"]
            self.grid_count = setting['grid_count']
            self.grid_price = setting["grid_price"]
            self.grid_volume = setting["grid_volume"]
            self.exit_price1 = setting.get('exit_price1', 0.0)
            self.exit_price2 = setting.get('exit_price2', 0.0)

            self.max_volume = decimal.Decimal(str(self.grid_volume)) * decimal.Decimal(str(self.grid_count))
            self.volume_rate = 1
            self.stop_price = self.guide_price
            self.new_setting = setting
            self.status = GridStatus.OPEN
            self.on_start()
            self.cancel_all()
            return True
        else:
            return False

    """ modify by loe """
    # ============================================================
    @classmethod
    # 自动生成单个算法参数
    def auto_parameters(cls, algo_engine:BaseEngine):
        tick = algo_engine.get_tick_subscribe(algo=None, vt_symbol=TRADE_SYMBOL)
        if not tick:
            return None

        if cls.AUTO_FLAG:
            grid_direction = GridDirection.LONG
            cls.AUTO_FLAG = not cls.AUTO_FLAG
        else:
            grid_direction = GridDirection.SHORT
            cls.AUTO_FLAG = not cls.AUTO_FLAG
        setting = cls.get_parameters(algo_engine=algo_engine,
                                     refer_price=tick.last_price,
                                     grid_direction=grid_direction)
        return setting

    @classmethod
    # 一键启动多个算法，返回初始化参数组合
    def one_start(cls, algo_engine:BaseEngine):
        tick = algo_engine.get_tick_subscribe(algo=None, vt_symbol=TRADE_SYMBOL)
        if not tick:
            return []

        grid_long_setting = cls.get_parameters(algo_engine=algo_engine,
                                               refer_price=tick.last_price,
                                               grid_direction=GridDirection.LONG)

        grid_short_setting = cls.get_parameters(algo_engine=algo_engine,
                                                refer_price=tick.last_price,
                                                grid_direction=GridDirection.SHORT)

        if grid_long_setting and grid_short_setting:
            return [grid_long_setting, grid_short_setting]
        else:
            return []

    @classmethod
    # 参数生成
    def get_parameters(cls, algo_engine:BaseEngine, refer_price:float, grid_direction:GridDirection):
        """
        line_price = 44480
        grid_width = 1040

        est_commision = line_price * 0.00075
        grid_price = ceil_to(est_commision, 10)

        exit_price1 = 0.0
        exit_price2 = 0.0
        """

        #"""
        est_commision = refer_price * 0.00075
        grid_price = ceil_to(est_commision/2.0, 10)
        line_price = round_to(refer_price, grid_price)

        # 根据pivot点位设置网格宽度
        generator = GridParametersGenerator(algo_engine=algo_engine, vt_symbol=TRADE_SYMBOL)
        generator.generate()
        if not generator.pivot:
            return None
        grid_width = max(abs(generator.long_entry3 - line_price), abs(line_price - generator.short_entry3))
        if grid_direction == GridDirection.LONG:
            exit_price1 = generator.short_entry1
            exit_price2 = generator.short_entry2

        elif grid_direction == GridDirection.OPEN:
            exit_price1 = 0.0
            exit_price2 = 0.0

        else:
            exit_price1 = generator.long_entry1
            exit_price2 = generator.long_entry2

        # """

        grid_count = ceil(grid_width / grid_price)
        grid_width = grid_price * grid_count

        # 网格仓位大小
        total_volume = TRADE_CAPITAL / grid_width
        grid_volume = floor_to(total_volume / grid_count, 0.001)

        if grid_direction == GridDirection.LONG:
            algo_name = 'grid_long'
            guide_price = line_price + grid_width

        elif grid_direction == GridDirection.OPEN:
            algo_name = 'grid_open'
            guide_price = line_price
        else:
            algo_name = 'grid_short'
            guide_price = line_price - grid_width

        return {"editable": '是',
                "algo_name": algo_name,
                "mode": '日内',
                "grid_direction": grid_direction.value,
                "ratio_close": "否",
                "vt_symbol": TRADE_SYMBOL,
                "guide_price": guide_price,
                "grid_count": grid_count,
                "grid_price": grid_price,
                "grid_volume": grid_volume,
                "exit_price1":exit_price1,
                "exit_price2":exit_price2,
                "interval": 20
                }
    # ============================================================

    def check_init(self):
        contract = self.algo_engine.main_engine.get_contract(vt_symbol=self.vt_symbol)
        if contract and contract.pricetick:
            self.tick_price = contract.pricetick
        else:
            self.write_log(f'tick_price无法确认，停止算法')
            self.stop()
            return

        if self.pos:
            # 初始化有仓位，状态设为OPEN
            self.status = GridStatus.OPEN

        if self.mode == Mode.DAILY:
            # 网格上下限
            grid_width = decimal.Decimal(str(self.grid_count)) * decimal.Decimal(str(self.grid_price))
            self.gridUp = float(decimal.Decimal(str(self.guide_price)) + decimal.Decimal(str(grid_width)))
            self.gridDown = float(decimal.Decimal(str(self.guide_price)) - decimal.Decimal(str(grid_width)))

        elif self.mode == Mode.LONG:
            # 网格上下限
            self.gridUp = float(decimal.Decimal(str(self.guide_price)) * decimal.Decimal(str(2)))
            self.gridDown = 0.0
            # 网格的大小
            self.grid_price = float(decimal.Decimal(str(self.guide_price)) / decimal.Decimal(str(self.grid_count)))
            if self.grid_price < self.min_grid_price:
                self.grid_price = self.min_grid_price
                self.grid_count = int(self.guide_price / self.grid_price)
            else:
                self.grid_count = self.grid_count

    # 创建网格
    def create_grid(self):
        if self.guide_price <= 0 or self.grid_count <= 0 or self.grid_price <= 0 or self.grid_volume <= 0:
            self.active = False
            return

        # 价格数列
        grid_price_up = decimal.Decimal(str(self.guide_price)) + (decimal.Decimal(str(self.grid_count)) + decimal.Decimal(str(1))) * decimal.Decimal(str(self.grid_price))
        grid_price_down = decimal.Decimal(str(self.guide_price)) - decimal.Decimal(str(self.grid_count)) * decimal.Decimal(str(self.grid_price))
        if grid_price_down < 0:
            self.active = False
            return
        grid_price_array_decimal = np.arange(decimal.Decimal(str(grid_price_down)), decimal.Decimal(str(grid_price_up)), decimal.Decimal(str(self.grid_price)))
        grid_price_array_float = []
        for decimal_value in grid_price_array_decimal:
            float_value = float(decimal_value)
            grid_price_array_float.append(float_value)
        grid_price_array_float = np.array(grid_price_array_float)

        # 仓位数列
        total_volume = self.grid_volume * self.grid_count
        total_volume = total_volume * self.volume_rate
        grid_volume = floor_to(total_volume / self.grid_count, 0.001)
        self.max_volume = decimal.Decimal(str(grid_volume)) * decimal.Decimal(str(self.grid_count))

        grid_pos_up = decimal.Decimal(str(self.grid_count)) * decimal.Decimal(str(grid_volume))
        grid_pos_down = (decimal.Decimal(str(self.grid_count)) + decimal.Decimal(str(1))) * decimal.Decimal(str(grid_volume)) * decimal.Decimal(str(-1))
        grid_pos_array_decimal = np.arange(decimal.Decimal(str(grid_pos_up)), decimal.Decimal(str(grid_pos_down)), decimal.Decimal(str(-1 * grid_volume)))
        grid_pos_array_float = []
        for decimal_value in grid_pos_array_decimal:
            float_value = float(decimal_value)
            grid_pos_array_float.append(float_value)
        grid_pos_array_float = np.array(grid_pos_array_float)

        # 网格
        self.grid = pd.Series(grid_pos_array_float, index=grid_price_array_float)

    def estimate_max_loss(self, price):
        grid_price_array = self.grid.index
        result_price = min(grid_price_array, key=lambda x: abs(x - price))
        result_target = self.grid[result_price]
        if result_price <= self.guide_price:
            max_loss_price = result_price - grid_price_array[0]
        else:
            max_loss_price = grid_price_array[-1] - result_price

        est_target_loss = abs(result_target) * max_loss_price
        est_left_loss = (((self.grid_count * self.grid_volume) - abs(result_target)) / 2.0) * (max_loss_price - self.grid_price)
        self.est_max_loss = est_target_loss + est_left_loss

    def estimate_max_pnl(self, price):
        grid_price_array = self.grid.index
        result_price = min(grid_price_array, key=lambda x: abs(x - price))
        result_target = self.grid[result_price]
        max_pnl_price = abs(self.guide_price - result_price)
        self.est_max_pnl = (abs(result_target) / 2.0) * (max_pnl_price + self.grid_price)

    """ modify by loe """
    def on_start(self):
        self.check_init()
        self.create_grid()
        self.pos_reiniting_waiting = True

        self.setting_data = self.new_setting
        self.saveSyncData()

        self.put_parameters_event()
        self.put_variables_event()

    def on_tick(self, tick: TickData):
        """"""
        # 只要最新tick
        if self.last_tick and self.last_tick.datetime >= tick.datetime:
            return

        # 理论上买一价小于卖一价，如果不是，数据可能异常，为了避免taker成交增加手续费成本，不做委托
        if tick.bid_price_1 >= tick.ask_price_1:
            if not self.tick_error:
                self.write_log(f'tick买卖一档数据异常\tbid_price_1： {tick.bid_price_1}\task_price_1：{tick.ask_price_1}')
                self.tick_error = True
            return

        self.last_tick = tick
        if self.pos_reiniting_waiting:
            self.pos_reiniting_waiting = False
            self.reinit_pos()

        if self.check_enable:
            self.check_enable = False
            self.check_status()
            self.check_volume()
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

        # 初始化仓位
        if self.pos_reiniting:
            if self.pos_reiniting_target > self.pos:
                # 多单
                long_target = self.pos_reiniting_target
                long_price = tick.bid_price_1 - 2 * self.tick_price

            elif self.pos_reiniting_target < self.pos:
                # 空单
                short_target = self.pos_reiniting_target
                short_price = tick.ask_price_1 + 2 * self.tick_price

            else:
                self.pos_reiniting = False

        else:
            # 结算前清仓
            if self.pos < 0 and self.status == GridStatus.CLOSE:
                long_price = tick.last_price + self.tick_price * 200
                long_target = 0

            if self.pos > 0 and self.status == GridStatus.CLOSE:
                short_price = tick.last_price - self.tick_price * 200
                short_target = 0

            if self.status == GridStatus.PREPARE or self.status == GridStatus.OPEN:
                # 确定多单目标仓位
                if tick.bid_price_1:
                    long_index_array = np.argwhere(grid_price_array < tick.bid_price_1)
                    if len(long_index_array) and self.status == GridStatus.OPEN:
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

                    elif tick.bid_price_1 <= self.gridDown:
                        # 价格低于gridDown
                        long_price = tick.bid_price_1 - 2 * self.tick_price
                        long_target = grid_pos_array[0]
                        if long_target <= self.pos:
                            long_price = None
                            long_target = None

                        # 风控，价格低于gridDown过多停止多单，前提是网格组合状态都为PREPARE
                        if long_price and long_price < self.gridDown - self.grid_price and self.status == GridStatus.PREPARE:
                            other_open = False
                            for algo in self.algo_engine.algos.values():
                                if isinstance(algo, GridAlgo) and algo.algo_name != self.algo_name:
                                    if algo.status == GridStatus.OPEN:
                                        other_open = True
                                        break

                            if not other_open:
                                long_price = None
                                long_target = None

                # 确定空单目标仓位
                if tick.ask_price_1:
                    short_index_array = np.argwhere(grid_price_array > tick.ask_price_1)
                    if len(short_index_array) and self.status == GridStatus.OPEN:
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

                    elif tick.ask_price_1 >= self.gridUp:
                        # 价格高于gridUp
                        short_price = tick.ask_price_1 + 2 * self.tick_price
                        short_target = grid_pos_array[-1]
                        if short_target >= self.pos:
                            short_price = None
                            short_target = None

                        # 风控，价格低于gridDown过多停止多单，前提是网格组合状态都为PREPARE
                        if short_price and short_price > self.gridUp + self.grid_price and self.status == GridStatus.PREPARE:
                            other_open = False
                            for algo in self.algo_engine.algos.values():
                                if isinstance(algo, GridAlgo) and algo.algo_name != self.algo_name:
                                    if algo.status == GridStatus.OPEN:
                                        other_open = True
                                        break

                            if not other_open:
                                long_price = None
                                long_target = None

        if self.mode == Mode.LONG:
            # 自定义模式仓位管理
            # 1、初始建仓目标未达成时，多仓委托价格不能高于建仓价位线
            if not self.init_pos_complete:
                if long_price and long_price >= self.guide_price * self.init_line:
                    long_price = None
                    long_target = None

            # 2、不允许持有空仓
            if short_target and short_target < 0:
                short_target = 0.0

        long_dict = {'long_price':long_price,
                     'long_target':long_target}

        short_dict = {'short_price':short_price,
                      'short_target':short_target}

        return long_dict, short_dict

    def check_status(self):
        # PREPARE初始建仓完成后OPEN
        if self.status == GridStatus.PREPARE:
            grid_pos_array = self.grid.values
            if self.pos >= grid_pos_array[0] or self.pos <= grid_pos_array[-1]:
                self.status = GridStatus.OPEN

        # 资金费率结算前准备清仓
        end_time = next_window_bar_datetime(self.last_tick.datetime)
        if self.ratio_close and without_timezone(self.last_tick.datetime) >= end_time - timedelta(minutes=6):
            if self.status != GridStatus.CLOSE:
                self.status = GridStatus.WAITINGCLOSE
                # 向其他网格算法同步状态
                self.algo_engine.on_algo_update(algo_name=self.algo_name)

    def check_volume(self):
        last_volume_rate = self.volume_rate

        if self.grid_direction == GridDirection.LONG:
            if self.exit_price1 and self.last_tick.last_price <= self.exit_price1:
                self.volume_rate = min(self.volume_rate, 0.5)

                if self.exit_price2:
                    stop_price = self.gridDown + abs(self.exit_price1 - self.exit_price2)
                    self.stop_price = min(self.stop_price, stop_price)
                    self.stop_price = min(self.stop_price, self.guide_price)

            if self.exit_price2 and self.last_tick.last_price <= self.exit_price2:
                self.volume_rate = min(self.volume_rate, 0.5*0.5)

                stop_price = self.gridDown
                self.stop_price = min(self.stop_price, stop_price)
                self.stop_price = min(self.stop_price, self.guide_price)

        elif self.grid_direction == GridDirection.SHORT:
            if self.exit_price1 and self.last_tick.last_price >= self.exit_price1:
                self.volume_rate = min(self.volume_rate, 0.5)

                if self.exit_price2:
                    stop_price = self.gridUp - abs(self.exit_price2 - self.exit_price1)
                    self.stop_price = max(self.stop_price, stop_price)
                    self.stop_price = max(self.stop_price, self.guide_price)

            if self.exit_price2 and self.last_tick.last_price >= self.exit_price2:
                self.volume_rate = min(self.volume_rate, 0.5*0.5)

                stop_price = self.gridUp
                self.stop_price = max(self.stop_price, stop_price)
                self.stop_price = max(self.stop_price, self.guide_price)

        # 更新网格
        if last_volume_rate != self.volume_rate:
            self.create_grid()
            self.reinit_pos()

    def reinit_pos(self):
        if not self.last_tick:
            return

        self.pos_reiniting = True

        grid_price_array = self.grid.index
        grid_pos_array = self.grid.values
        idx = (np.abs(grid_price_array - self.last_tick.last_price)).argmin()
        self.pos_reiniting_target = grid_pos_array[idx]

    def check_long_short_order(self):
        if not self.last_tick:
            return

        if self.long_orderids or self.short_orderids:
            return

        if self.status == GridStatus.WAITINGCLOSE:
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

            if self.grid_direction == GridDirection.SHORT:
                # 看跌网格不允许开多单
                long_open_volume = 0

                # 看跌网格触发停止价格
                if long_price <= self.stop_price:
                    long_close_volume = abs(self.pos)

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

            if self.grid_direction == GridDirection.LONG:
                # 看多网格不允许开空单
                short_open_volume = 0

                # 看涨网格触发停止价格
                if short_price >= self.stop_price:
                    short_close_volume = abs(self.pos)

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

        # 停止价格计算
        cycle_seconds = 60*30
        self.stop_price_timecounter += 1
        if self.stop_price_timecounter >= cycle_seconds or (not self.stop_price_init):
            self.stop_price_timecounter = 0
            self.update_stop_price()
        remain_seconds = cycle_seconds - self.stop_price_timecounter
        minute = int(remain_seconds/60)
        second = int(remain_seconds - minute*60)
        self.stop_price_remain = f'{minute}m {second}s'

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
        # 初始化仓位时每秒更新
        if self.pos_reiniting:
            self.reinit_pos()

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
        contract = self.algo_engine.get_contract(self, self.vt_symbol)
        if contract:
            self.pos = round_to(self.pos, contract.min_volume)

        # 确认初始化仓位完成
        if self.pos == self.pos_reiniting_target:
            self.pos_reiniting = False
            self.pos_reiniting_target = 0

        # 主动CLOSE状态
        if last_pos and self.pos == 0:
            self.status = GridStatus.CLOSE
            self.cancel_all()
            # 向其他网格算法同步状态
            self.algo_engine.on_algo_update(algo_name=self.algo_name)

        # 计算当前盈利
        trade_offset = self.algo_engine.orderid_offset_map.get(trade.vt_orderid, None)
        if trade_offset and trade_offset == Offset.CLOSE or trade_offset == Offset.CLOSEYESTERDAY or trade_offset == Offset.CLOSETODAY:
            count = int(round(trade.volume / self.grid_volume, 0))
            actual_volume = 0
            for i in range(count):
                actual_volume += self.grid_volume * (i + 1)
            self.current_pnl += actual_volume * self.grid_price

        # 自定义模式初始化建仓完成时预估最大准备金和预估最大仓位盈利
        if self.mode == Mode.LONG and not self.init_pos_complete:
            price_array = np.argwhere(self.grid.index < self.guide_price * self.init_line)
            if len(price_array):
                result_index = price_array[-1][-1]
                target_pos = self.grid.values[result_index]
                if abs(self.pos) >= abs(target_pos):
                    self.init_pos_complete = True
                    self.estimate_max_loss(price=trade.price)
                    self.estimate_max_pnl(price=trade.price)

        self.put_variables_event()
        self.saveSyncData()

    """ modify by loe """
    # ======================================================

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

    """
    def cancel_order(self, vt_orderid: str):
        """"""
        if vt_orderid in self.cancel_orderids:
            return
        self.cancel_orderids.append(vt_orderid)
        if len(self.cancel_orderids) >= 10:
            self.cancel_orderids.pop(0)

        super().cancel_order(vt_orderid=vt_orderid)
    """

    def on_algo_update(self, algo):
        if isinstance(algo, GridAlgo):
            if algo.status == GridStatus.WAITINGCLOSE and self.status != GridStatus.CLOSE:
                self.status = GridStatus.WAITINGCLOSE

            if algo.status == GridStatus.CLOSE:
                self.immediate_close()

    def immediate_close(self):
        if self.status != GridStatus.CLOSE:
            self.status = GridStatus.CLOSE
            self.cancel_all()
            self.check_enable = True

            subject = 'GRID 触发清仓'
            if self.grid_direction == GridDirection.LONG:
                content = '价格下跌'
            else:
                content = '价格上涨'
            self.send_ding_talk(subject=subject, content=content)

    def send_ding_talk(self, subject:str, content:str):
        try:
            self.algo_engine.main_engine.send_ding_talk(content=f'主题\n============\n{subject}\n\n内容\n============\n{content}')
        except:
            pass

    def update_stop_price(self):
        if not self.last_tick:
            return

        self.stop_price_init = True
        # 根据当前价格和周期内剩余时间计算最大允许的单向波动幅度，避免价格单向极限拉升导致的大幅亏损
        next_window_datetime = next_window_bar_datetime(current_datetime=self.last_tick.datetime)
        current_timestamp = self.last_tick.datetime.timestamp()
        next_window_timestamp = next_window_datetime.timestamp()
        if next_window_timestamp > current_timestamp:
            remain_second = next_window_timestamp - current_timestamp
            total_second = 8 * 60 * 60

            width = abs(self.gridUp - self.guide_price)
            # 计算可接受的波动幅度
            space = round_to((remain_second / total_second) * width, self.tick_price)
            # 设置最小幅度
            space = max(space, 3*self.grid_price)
            if self.grid_direction == GridDirection.LONG:
                stop_price = max(self.last_tick.last_price, self.gridDown) + space
                stop_price = ceil_to(stop_price, self.grid_price)
                stop_price = min(stop_price, self.guide_price)
                self.stop_price = min(self.stop_price, stop_price)

            elif self.grid_direction == GridDirection.SHORT:
                stop_price = min(self.last_tick.last_price, self.gridUp) - space
                stop_price = floor_to(stop_price, self.grid_price)
                stop_price = max(stop_price, self.guide_price)
                self.stop_price = max(self.stop_price, stop_price)

    # ======================================================

def next_window_bar_datetime(current_datetime:datetime) -> datetime:
    current_datetime = without_timezone(current_datetime)
    the_datetime = current_datetime
    next_datetime = None
    n = 0
    while True:
        n += 1
        if next_datetime or n > 2:
            break

        year = the_datetime.year
        month = the_datetime.month
        day = the_datetime.day
        for x in window_time:
            temp_datetime = datetime.strptime(f'{year}-{month}-{day} {x}', '%Y-%m-%d %H:%M:%S')
            if current_datetime <= temp_datetime:
                next_datetime = temp_datetime
                break
        the_datetime += timedelta(days=1)

    return next_datetime

def without_timezone(target:datetime):
    datetime_str = target.strftime('%Y-%m-%d %H:%M:%S')
    return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

class CustomBarGenerator(BarGenerator):
    def __init__(self,
                 on_bar: Callable,
                 window: int = 0,
                 on_window_bar: Callable = None,
                 interval: Interval = Interval.MINUTE):
        super().__init__(on_bar, window, on_window_bar, interval)
        self.window_start = None
        self.window_end = None
        self.next_datetime = None
        self.window_bar = None

    def update_bar(self, bar: BarData):
        if not self.window_start:
            self.window_start = next_window_bar_datetime(current_datetime=bar.datetime)
            self.window_end = (next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)
            self.next_datetime = self.window_start

        if bar.datetime == self.next_datetime:
            valid = True
            self.next_datetime += timedelta(minutes=1)

        elif bar.datetime > self.next_datetime:
            valid = True
            msg = f'{self.__class__.__name__}：分钟数据缺失【{self.next_datetime} -- {bar.datetime - timedelta(minutes=1)}】'
            print(msg)
            self.next_datetime = bar.datetime + timedelta(minutes=1)

            if not self.window_bar:
                self.window_start = bar.datetime
                self.window_end = (next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)

            if bar.datetime > self.window_end:
                # 提前end
                self.on_window_bar(self.window_bar)
                self.window_bar = None

                self.window_start = bar.datetime
                self.window_end = (next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)

        else:
            if not self.window_bar:
                return
            else:
                raise ('出现异常，检查代码！')

        if valid:
            if bar.datetime == self.window_start:
                self.window_bar = BarData(gateway_name='',
                                          symbol=bar.symbol,
                                          exchange=bar.exchange,
                                          datetime=self.window_start,
                                          endDatetime=self.window_end)
                self.window_bar.open_price = bar.open_price
                self.window_bar.high_price = bar.high_price
                self.window_bar.low_price = bar.low_price
                self.window_bar.close_price = bar.close_price
            else:
                if not self.window_bar:
                    raise ('出现异常，检查代码！')

                self.window_bar.close_price = bar.close_price
                self.window_bar.high_price = max(self.window_bar.high_price, bar.high_price)
                self.window_bar.low_price = min(self.window_bar.low_price, bar.low_price)

        if bar.datetime == self.window_end:
            self.on_window_bar(self.window_bar)
            self.window_bar = None

            self.window_start = next_window_bar_datetime(current_datetime=bar.datetime)
            self.window_end = (next_window_bar_datetime(current_datetime=self.window_start + timedelta(minutes=1))) - timedelta(minutes=1)

class GridParametersGenerator(object):
    def __init__(self, algo_engine:BaseEngine, vt_symbol:str):
        self.algo_engine = algo_engine
        self.vt_symbol = vt_symbol

        self.base_time = None
        self.pivot = 0
        self.long_entry1 = 0
        self.long_entry2 = 0
        self.long_entry3 = 0
        self.short_entry1 = 0
        self.short_entry2 = 0
        self.short_entry3 = 0

    def generate(self):
        self.bg = CustomBarGenerator(on_bar=None,
                                     window=0,
                                     on_window_bar=self.on_generate_bar,
                                     interval=Interval.MINUTE)

        self.algo_engine.load_bar(self.vt_symbol, 2, Interval.MINUTE, self.on_bar)

    # 分钟数据
    def on_bar(self, bar: BarData):
        """"""
        self.bg.update_bar(bar)

    # 周期数据
    def on_generate_bar(self, bar: BarData):
        self.base_time = next_window_bar_datetime(bar.datetime + timedelta(minutes=1))

        high = bar.high_price
        low = bar.low_price
        close = bar.close_price

        self.pivot = (high + low + 2 * close) / 4
        self.long_entry1 = 2 * self.pivot - low
        self.short_entry1 = 2 * self.pivot - high
        self.long_entry2 = self.pivot + (self.long_entry1 - self.short_entry1)
        self.short_entry2 = self.pivot - (self.long_entry1 - self.short_entry1)
        self.long_entry3 = high - (2 * (low - self.pivot))
        self.short_entry3 = low - (2 * (high - self.pivot))

class GridArrayManager(object):
    # 指标数据参数
    window = 60
    def __init__(self, algo_engine:BaseEngine, vt_symbol:str):
        self.algo_engine = algo_engine
        self.vt_symbol = vt_symbol

        self.datetime = None
        self.windowAtr = 0
        self.atr = 0
        self.am = ArrayManager(self.window + 1)


    def generate(self):
        self.algo_engine.load_bar(self.vt_symbol, 1, Interval.MINUTE, self.on_bar)

    # 分钟数据
    def on_bar(self, bar: BarData):
        """"""
        self.am.update_bar(bar=bar)
        if not self.am.inited:
            return

        self.datetime = bar.datetime
        self.windowAtr = self.am.atr(self.window)
        self.atr = self.am.atr(1)
