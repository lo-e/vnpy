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
from vnpy.trader.constant import Offset
from datetime import datetime

CLOSE_TIME_START = '14:55'
CLOSE_TIME_END = '15:05'

class StatisticalArbitrageBacktestingStrategy(SpreadStrategyTemplate):
    """"""

    author = "loe"

    boll_window = 20
    boll_dev = 2
    open_value = 2
    max_pos = 30
    payup = 10
    interval = 5

    boll_up = 0.0
    boll_down = 0.0
    boll_mid = 0.0

    parameters = [
        "boll_window",
        "boll_dev",
        "open_value",
        "max_pos",
        "payup",
        "interval"
    ]
    variables = [
        "boll_up",
        "boll_down",
        "boll_mid"
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

        self.bg = BarGenerator(self.on_spread_bar)
        self.am = ArrayManager(size=self.boll_window)

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        self.load_bar(1)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")

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
        self.bg.update_tick(tick)
        self.put_timer_event()

    def on_spread_bar(self, bar: BarData):
        """
        Callback when spread bar data is generated.
        """
        self.stop_all_algos()
        self.am.update_bar(bar)

        if self.am.inited:
            self.boll_mid = self.am.sma(self.boll_window)
            self.boll_up, self.boll_down = self.am.boll(self.boll_window, self.boll_dev)

        if not self.boll_up or not self.boll_mid or not self.boll_down:
            return

        """ fake """
        if bar.datetime >= datetime.strptime('2020-3-20 10:39:0', '%Y-%m-%d %H:%M:%S'):
            a = 2

        if not self.spread_pos:
            # 设置一个开仓阈值
            if self.boll_up - self.boll_mid >= self.open_value * self.strategy_engine.pricetick:
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
            if self.boll_mid:
                self.start_long_algo(
                    self.boll_mid,
                    abs(self.spread_pos),
                    payup=self.payup,
                    interval=self.interval,
                    offset=Offset.CLOSE
                )
        else:
            if self.boll_mid:
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
        # 一旦有算法出现成交，立即停止其他正在运行的算法
        self.check_and_stop_other_algo(algo)
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

    def check_close_time(self, target_datetime:datetime):
        now = datetime.now()
        close_start = datetime.strptime(f'{now.year}-{now.month}-{now.day} {CLOSE_TIME_START}', '%Y-%m-%d %H:%M')
        close_end = datetime.strptime(f'{now.year}-{now.month}-{now.day} {CLOSE_TIME_END}', '%Y-%m-%d %H:%M')

        if close_start <= target_datetime <= close_end:
            return True
        else:
            return False