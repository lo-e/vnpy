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
from vnpy.app.spread_trading.template import SpreadStrategyTemplate, SpreadAlgoTemplate, check_trading_time
from vnpy.trader.constant import Offset
from datetime import datetime

# 数据下载
from App.Turtle.dataservice import TurtleDataDownloading

CLOSE_TIME_START = '14:55'
CLOSE_TIME_END = '15:05'

class StatisticalArbitrageStrategy(SpreadStrategyTemplate):
    """"""

    author = "loe"

    boll_window = 20
    boll_dev = 2
    open_value = 4
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

    def download_data(self):
        symbol_list = []
        for vt_symbol in self.spread.legs.keys():
            symbol = vt_symbol.split('.')[0].upper()
            symbol_list.append(symbol)
        msg = TurtleDataDownloading().download_minute_jq(symbol_list=symbol_list)
        self.strategy_engine.send_strategy_email(self, msg=msg)

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
        if not self.check_tick_valid(tick=tick):
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

        self.check_for_trade()

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
            if self.current_length >= self.open_value:
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

    def check_close_time(self, target_datetime:datetime):
        now = datetime.now()
        close_start = datetime.strptime(f'{now.year}-{now.month}-{now.day} {CLOSE_TIME_START}', '%Y-%m-%d %H:%M')
        close_end = datetime.strptime(f'{now.year}-{now.month}-{now.day} {CLOSE_TIME_END}', '%Y-%m-%d %H:%M')

        if close_start <= target_datetime <= close_end:
            return True
        else:
            return False
