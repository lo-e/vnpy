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


class StatisticalArbitrageStrategy(SpreadStrategyTemplate):
    """"""

    author = "loe"

    boll_window = 20
    boll_dev = 2
    max_pos = 10
    payup = 10
    interval = 5

    boll_up = 0.0
    boll_down = 0.0
    boll_mid = 0.0

    parameters = [
        "boll_window",
        "boll_dev",
        "max_pos",
        "payup",
        "interval"
    ]
    variables = [
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
        self.am = ArrayManager(size=self.boll_window + 1)

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")

        self.load_bar(10)

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
        if not self.am.inited:
            return

        self.boll_mid = self.am.sma(self.boll_window)
        self.boll_up, self.boll_down = self.am.boll(
            self.boll_window, self.boll_dev)

        if not self.spread_pos:
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
                offset = Offset.OPEN
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

    def stop_open_algos(self):
        """"""
        if self.buy_algoid:
            self.stop_algo(self.buy_algoid)

        if self.short_algoid:
            self.stop_algo(self.short_algoid)

    def stop_close_algos(self):
        """"""
        if self.sell_algoid:
            self.stop_algo(self.sell_algoid)

        if self.cover_algoid:
            self.stop_algo(self.cover_algoid)
