from vnpy.trader.utility import ArrayManager
from vnpy.trader.object import BarData
import numpy as np
import talib
from vnpy.trader.constant import Direction


class Rumi(object):
    """
    RUMI指标采用了对均线偏离度平滑处理
    """

    def __init__(
        self,
        fast_window: int = 7,
        slow_window: int = 14,
        rumi_window: int = 3,
        show_window: int = 20,
    ):
        self.fast_window = fast_window
        self.slow_window = slow_window
        self.rumi_window = rumi_window
        self.show_window = show_window
        self.am = ArrayManager(
            self.show_window + self.slow_window + self.rumi_window - 2
        )

        self.inited = False
        self.rma: np.ndarray = np.array([])
        self.cross_count = 0
        self.crossing_direction: Direction = Direction.NET

    def update_bar(self, bar: BarData) -> None:
        self.am.update_bar(bar)
        self.inited = self.am.inited
        if self.inited:
            fast_array: np.ndarray = self.am.sma(n=self.fast_window, array=True)
            slow_array: np.ndarray = self.am.wma(n=self.slow_window, array=True)
            diff_array: np.ndarray = fast_array - slow_array
            rumi_array: np.ndarray = talib.SMA(diff_array, self.rumi_window)
            self.rma = rumi_array[len(rumi_array) - self.show_window :]
            self.generate_signal()

    def generate_signal(self) -> None:
        self.cross_count = 0
        self.crossing_direction = Direction.NET

        last_direction = 0
        current_direction = 0
        index = 0
        while index < len(self.rma):
            value = self.rma[index]
            current_direction = 1 if value > 0 else (-1 if value < 0 else 0)

            if last_direction == 0:
                # 初始方向
                last_direction = current_direction
                index += 1
                continue

            if current_direction == 0:
                # 当前处于0轴
                index += 1
                continue

            if last_direction * current_direction < 0:
                self.cross_count += 1
                if index == len(self.rma) - 1:
                    if current_direction > 0:
                        self.crossing_direction = Direction.LONG

                    elif current_direction < 0:
                        self.crossing_direction = Direction.SHORT

            last_direction = current_direction
            index += 1
