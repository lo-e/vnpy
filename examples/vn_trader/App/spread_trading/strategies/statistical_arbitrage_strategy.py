from vnpy.trader.utility import BarGenerator, ArrayManager
from vnpy.app.spread_trading import (
    SpreadStrategyTemplate,
    SpreadAlgoTemplate,
    SpreadData,
    OrderData,
    TradeData,
    TickData,
    BarData
)
from vnpy.app.spread_trading.template import SpreadStrategyTemplate, SpreadAlgoTemplate, check_spread_valid, get_night_type, NType
from vnpy.trader.constant import Offset
import datetime

# 数据下载
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from vnpy.trader.utility import floor_to

STOP_OPEN_ALGO_TIME_LIST = [[datetime.time(14, 55, 35), datetime.time(15, 20, 0)]]

STOP_CLOSE_ALGO_TIME_LIST = [[datetime.time(14, 58, 35), datetime.time(15, 20, 0)]]

class StatisticalArbitrageStrategy(SpreadStrategyTemplate):
    """"""

    author = "loe"

    boll_window = 20
    boll_dev = 5
    tick_price = 1.0
    open_value = 2
    max_pos = 30
    payup = 10
    interval = 5

    boll_up = 0.0
    boll_down = 0.0
    boll_mid = 0.0
    current_length = 0.0
    max_open_volume = 0

    parameters = [
        "boll_window",
        "boll_dev",
        "tick_price",
        "open_value",
        "max_pos",
        "payup",
        "interval"
    ]
    variables = [
        "boll_up",
        "boll_down",
        "boll_mid",
        "current_length",
        "max_open_volume"
    ]

    syncs = [
        "boll_up",
        "boll_down",
        "boll_mid"
    ]

    def __init__(
        self,
        strategy_engine,
        strategy_name: str,
        spread: SpreadData,
        setting: dict
    ):
        """"""
        super().__init__(
            strategy_engine, strategy_name, spread, setting
        )

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        self.bg = BarGenerator(self.on_spread_bar)
        self.am = ArrayManager(size=self.boll_window)
        self.load_recent_bar(count=60)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")
        self.trading = True
        self.check_for_trade()

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")
        self.put_timer_event()

    def on_spread_data(self):
        """
        Callback when spread price is updated.
        """
        # 过滤无效spread_data
        if not check_spread_valid(spread=self.spread):
            self.write_log(f'====== 过滤无效spread_data：{self.spread.name}\t{self.spread.datetime} ======')
            return
        tick = self.get_spread_tick()
        self.on_spread_tick(tick)

    def on_spread_tick(self, tick: TickData):
        """
        Callback when new spread tick data is generated.
        """
        self.bg.update_tick(tick)
        self.put_timer_event()

    def on_timer(self):
        super().on_timer()

        now = datetime.datetime.now()
        # 停止新的开仓操作
        self.stop_open = self.check_stop_open_algo_close_time(symbol=self.spread.active_leg.vt_symbol, target_datetime=now)

        # 让新开的平仓算法强行平仓
        self.close_anyway = self.check_stop_close_algo_close_time(symbol=self.spread.active_leg.vt_symbol, target_datetime=now)


    def on_spread_bar(self, bar: BarData):
        """
        Callback when spread bar data is generated.
        """
        self.am.update_bar(bar)
        if self.am.inited:
            self.boll_mid = self.am.sma(self.boll_window)
            self.boll_up, self.boll_down = self.am.boll(self.boll_window, self.boll_dev)
        self.current_length = self.boll_up - self.boll_mid
        # 计算资金能承受的最大开仓量
        self.calculate_max_open_volume()
        # 交易信号判断
        self.check_for_trade()
        # 异步下载最新分钟数据
        if self.trading:
            self.prepare_for_download_recent_data()
        self.put_timer_event()

    def check_for_trade(self):
        if not self.boll_up or not self.boll_mid or not self.boll_down:
            return

        if not self.trading:
            return

        if not self.check_algo_order_finished() or not self.check_algo_hedge_finished():
            # 有算法订单正在进行或者出现断腿情况，保持算法运行
            return

        self.stop_all_algos()
        if not self.spread_pos:
            if self.stop_open:
                return

            # 设置一个开仓阈值
            if self.current_length >= self.open_value * self.tick_price:
                open_volume = self.max_open_volume
                if not open_volume:
                    open_volume = self.max_pos

                self.start_short_algo(
                    self.boll_up,
                    open_volume,
                    payup=self.payup,
                    interval=self.interval,
                    offset=Offset.OPEN
                )

                self.start_long_algo(
                    self.boll_down,
                    open_volume,
                    payup=self.payup,
                    interval=self.interval,
                    offset=Offset.OPEN
                )
        elif self.spread_pos < 0:
            self.start_long_algo(
                self.boll_mid,
                abs(self.spread_pos),
                payup=self.payup,
                interval=self.interval,
                offset=Offset.CLOSE
            )
        else:
            self.start_short_algo(
                self.boll_mid,
                abs(self.spread_pos),
                payup=self.payup,
                interval=self.interval,
                offset = Offset.CLOSE
            )

        self.put_timer_event()

    def on_spread_pos(self):
        """
        Callback when spread position is updated.
        """
        pass

    def on_spread_algo(self, algo: SpreadAlgoTemplate):
        """
        Callback when algo status is updated.
        """
        pass

    def on_order(self, order: OrderData):
        """
        Callback when order status is updated.
        """
        pass

    def on_trade(self, trade: TradeData):
        """
        Callback when new trade data is received.
        """
        pass

    def stop_open_algos(self):
        """"""
        for buy_algoid in self.buy_algoids_list:
            self.stop_algo(buy_algoid)

        for short_algoid in self.short_algoids_list:
            self.stop_algo(short_algoid)

    def stop_close_algos(self):
        """"""
        for sell_algoid in self.sell_algoids_list:
            self.stop_algo(sell_algoid)

        for cover_algoid in self.cover_algoids_list:
            self.stop_algo(cover_algoid)

    def check_stop_open_algo_close_time(self, symbol:str, target_datetime:datetime):
        time_list = copy(STOP_OPEN_ALGO_TIME_LIST)
        night_type = get_night_type(symbol=symbol)
        if night_type == NType.EARLY:
            # 夜盘结束23：00
            stop_time = [datetime.time(22, 55, 35), datetime.time(23, 20, 0)]
            time_list.append(stop_time)

        if night_type == NType.MID:
            # 夜盘结束1：00
            stop_time = [datetime.time(0, 55, 35), datetime.time(1, 20, 0)]
            time_list.append(stop_time)

        if night_type == NType.LATER:
            # 夜盘结束 2：30
            stop_time = [datetime.time(2, 25, 35), datetime.time(2, 50, 0)]
            time_list.append(stop_time)

        for from_end_time in time_list:
            from_time = from_end_time[0]
            end_time = from_end_time[-1]

            if from_time <= target_datetime.time() <= end_time:
                return True
            else:
                return False

    def check_stop_close_algo_close_time(self, symbol:str, target_datetime:datetime):
        time_list = copy(STOP_CLOSE_ALGO_TIME_LIST)
        night_type = get_night_type(symbol=symbol)
        if night_type == NType.EARLY:
            # 夜盘结束23：00
            stop_time = [datetime.time(22, 58, 35), datetime.time(23, 20, 0)]
            time_list.append(stop_time)

        if night_type == NType.MID:
            # 夜盘结束1：00
            stop_time = [datetime.time(0, 58, 35), datetime.time(1, 20, 0)]
            time_list.append(stop_time)

        if night_type == NType.LATER:
            # 夜盘结束 2：30
            stop_time = [datetime.time(2, 28, 35), datetime.time(2, 50, 0)]
            time_list.append(stop_time)

        for from_end_time in time_list:
            from_time = from_end_time[0]
            end_time = from_end_time[-1]

            if from_time <= target_datetime.time() <= end_time:
                return True
            else:
                return False

    def prepare_for_download_recent_data(self):
        thread_executor = ThreadPoolExecutor(max_workers=10)
        thread_executor.submit(self.get_recent_data)

    def get_recent_data(self):
        self.strategy_engine.downloading_recent_data(callback=self.download_callback)

    def download_callback(self, last_datetime, msg):
        now_minute = datetime.datetime.now().replace(second=0, microsecond=0)
        if last_datetime == now_minute:
            self.am = ArrayManager(size=self.boll_window)
            self.load_recent_bar(count=60, callback=self.update_am_bar)

    def update_am_bar(self, bar: BarData):
        self.am.update_bar(bar)

    def on_traded_changed(self, algo: SpreadAlgoTemplate, changed=0):
        if self.spread_pos > 0:
            volume_left = abs(self.spread_pos)
            sell_ids = copy(self.sell_algoids_list)
            for sell_algoid in sell_ids:
                sell_algo = self.strategy_engine.get_algo(algoid=sell_algoid)
                if sell_algo:
                    sell_algo_left = abs(sell_algo.target) - abs(sell_algo.traded)
                    if sell_algo_left > 0:
                        volume_left -= sell_algo_left

            if volume_left > 0:
                # 开sell算法
                self.start_short_algo(
                    self.boll_mid,
                    abs(volume_left),
                    payup=self.payup,
                    interval=self.interval,
                    offset=Offset.CLOSE
                )

        elif self.spread_pos < 0:
            volume_left = abs(self.spread_pos)
            cover_ids = copy(self.cover_algoids_list)
            for cover_algoid in cover_ids:
                cover_algo = self.strategy_engine.get_algo(algoid=cover_algoid)
                if cover_algo:
                    cover_algo_left = abs(cover_algo.target) - abs(cover_algo.traded)
                    if cover_algo_left > 0:
                        volume_left -= cover_algo_left

            if volume_left > 0:
                # 开cover算法
                self.start_long_algo(
                    self.boll_mid,
                    abs(volume_left),
                    payup=self.payup,
                    interval=self.interval,
                    offset=Offset.CLOSE
                )

    def calculate_max_open_volume(self):
        algo_engine = self.strategy_engine.spread_engine.algo_engine
        # 主动腿
        active_vt_symbol = self.spread.active_leg.vt_symbol
        active_price = 0
        active_tick = self.spread.active_leg.tick
        if active_tick:
            active_price = active_tick.last_price

        active_size = 0
        active_contract = algo_engine.get_contract(active_vt_symbol)
        if active_contract:
            active_size = active_contract.size

        active_rate = algo_engine.get_symbol_rate(symbol=active_vt_symbol)

        # 被动腿
        passive_leg = self.spread.passive_legs[0]
        passive_vt_symbol = passive_leg.vt_symbol
        passive_price = 0
        passive_tick = passive_leg.tick
        if passive_tick:
            passive_price = passive_tick.last_price

        passive_size = 0
        passive_contract = algo_engine.get_contract(passive_vt_symbol)
        if passive_contract:
            passive_size = passive_contract.size

        passive_rate = algo_engine.get_symbol_rate(symbol=passive_vt_symbol)

        # 保证金费率
        active_target = active_price * active_size * active_rate
        passive_target = passive_price * passive_size *passive_rate
        if not active_target or not passive_target:
            self.max_open_volume = 0
        else:
            result = algo_engine.portfolio_value / (active_target + passive_target)
            result = floor_to(result, 1)
            self.max_open_volume = result


