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
from vnpy.app.spread_trading.template import SpreadStrategyTemplate, SpreadAlgoTemplate, check_trading_time, check_tick_valid
from vnpy.trader.constant import Offset
from datetime import datetime, timedelta

# 数据下载
from App.Turtle.dataservice import TurtleDataDownloading
from concurrent.futures import ThreadPoolExecutor
import re
from vnpy.app.cta_strategy.base import TRANSFORM_SYMBOL_LIST

CLOSE_TIME_START = '14:55'
CLOSE_TIME_END = '15:05'

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
        "current_length"
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
        self.load_bar(10)

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
        tick = self.get_spread_tick()
        self.on_spread_tick(tick)

    def on_spread_tick(self, tick: TickData):
        """
        Callback when new spread tick data is generated.
        """
        # 过滤无效tick
        if not check_tick_valid(tick=tick):
            self.write_log(f'====== 过滤无效tick：{tick.vt_symbol}\t{tick.datetime} ======')
            return

        self.bg.update_tick(tick)
        self.put_timer_event()

    def on_spread_bar(self, bar: BarData):
        """
        Callback when spread bar data is generated.
        """
        self.am.update_bar(bar)
        if self.am.inited:
            self.boll_mid = self.am.sma(self.boll_window)
            self.boll_up, self.boll_down = self.am.boll(self.boll_window, self.boll_dev)
        self.current_length = self.boll_up - self.boll_mid
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

        if self.check_algo_leg_broken():
            # 有算法出现断腿情况，保持算法运行
            return

        the_symbol = list(self.spread.legs.keys())[0]
        is_trading_time = check_trading_time(symbol=the_symbol, the_datetime=datetime.now())
        if not self.check_algo_order_finished() and not is_trading_time:
            # 非交易时间并且有订单未处理完
            return

        self.stop_all_algos()
        if not self.spread_pos:
            # 设置一个开仓阈值
            if self.current_length >= self.open_value * self.tick_price:
                self.start_short_algo(
                    self.boll_up,
                    self.max_pos,
                    payup=self.payup,
                    interval=self.interval,
                    offset=Offset.OPEN
                )

                self.start_long_algo(
                    self.boll_down,
                    self.max_pos,
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

    def check_close_time(self, target_datetime:datetime):
        now = datetime.now()
        close_start = datetime.strptime(f'{now.year}-{now.month}-{now.day} {CLOSE_TIME_START}', '%Y-%m-%d %H:%M')
        close_end = datetime.strptime(f'{now.year}-{now.month}-{now.day} {CLOSE_TIME_END}', '%Y-%m-%d %H:%M')

        if close_start <= target_datetime <= close_end:
            return True
        else:
            return False

    def prepare_for_download_recent_data(self):
        thread_executor = ThreadPoolExecutor(max_workers=10)
        thread_executor.submit(self.get_recent_data)

    def get_recent_data(self):
        self.strategy_engine.downloading_recent_data(callback=self.download_callback)

    def download_callback(self, last_datetime, msg):
        now_minute = datetime.now().replace(second=0, microsecond=0)
        if last_datetime == now_minute:
            self.am = ArrayManager(size=self.boll_window)
            self.load_bar(days=10, callback=self.update_am_bar)

    def update_am_bar(self, bar: BarData):
        self.am.update_bar(bar)

    def on_traded_changed(self, algo: SpreadAlgoTemplate, changed=0):
        if algo.algoid in self.buy_algoids_list and abs(changed):
            for sell_algoid in self.sell_algoids_list:
                sell_algo = self.strategy_engine.get_algo(algoid=sell_algoid)
                if sell_algo.check_hedge_finished():
                    # 停止当前sell算法
                    self.stop_algo(algoid=sell_algoid)

                    # 开sell算法
                    self.start_short_algo(
                        self.boll_mid,
                        abs(self.spread_pos),
                        payup=self.payup,
                        interval=self.interval,
                        offset=Offset.CLOSE
                    )

        elif algo.algoid in self.short_algoids_list and abs(changed):
            for cover_algoid in self.cover_algoids_list:
                cover_algo = self.strategy_engine.get_algo(algoid=cover_algoid)
                if cover_algo.check_hedge_finished():
                    # 停止当前cover算法
                    self.stop_algo(algoid=cover_algoid)

                    # 开cover算法
                    self.start_long_algo(
                        self.boll_mid,
                        abs(self.spread_pos),
                        payup=self.payup,
                        interval=self.interval,
                        offset=Offset.CLOSE
                    )
