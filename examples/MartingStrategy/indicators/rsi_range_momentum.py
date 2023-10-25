from pyrsistent import v
from vnpy.trader.object import BarData
from vnpy.trader.utility import ArrayManager
from vnpy.trader.constant import Direction
import numpy as np
from pymongo import MongoClient
from vnpy.app.cta_strategy.base import HOUR_DB_NAME, MinuteDataBaseName
from datetime import datetime, timedelta

class RsiRangeMomentum(object):
    """
    RSI区域动量策略（来自：VNPY）
    RSI多头范围：RSI过去N天内在40到100之间波动
    RSI多头动量：RSI过去N天内的极值高点大于70
    RSI空头范围：RSI过去N天内在60到0之间波动
    RSI空头动量：RSI过去N天内的极值低点小于30
    """

    # 趋势做多RSI低阈值
    long_rsi_lower: int = 40
    # 趋势做多RSI高阈值
    long_rsi_upper: int = 100
    # 趋势做多RSI极值阈值
    long_rsi_highest: int = 70

    # 趋势做空RSI低阈值
    short_rsi_lower: int = 0
    # 趋势做空RSI高阈值
    short_rsi_upper: int = 60
    # 趋势做空RSI极值阈值
    short_rsi_lowest: int = 30

    def __init__(
        self,
        back_window:int = 20,
        rsi_window:int = 14
    ) -> None:
        self.back_window = back_window # 统计回望窗口
        self.rsi_window = rsi_window # 计算RSI指标的窗口

        self.long_range_signal: bool = False # 多头区域信号
        self.long_mmt_signal: bool = False # 多头动量信号
        self.short_range_signal: bool = False # 空头区域信号
        self.short_mmt_signal: bool = False # 空头动量信号
        self.back_rsi_array = [] # 统计回望RSI数组
        self.inited = False # back_rsi_array的数量达到back_window要求，初始化完成

        self.array_manager = ArrayManager()

    def update_bar(self, bar: BarData) -> None:
        self.bar = bar
        self.array_manager.update_bar(bar)
        if self.array_manager.inited:
            # 计算rsi
            rsi = self.array_manager.rsi(self.rsi_window, array=False)
            self.back_rsi_array.append(rsi)
            if len(self.back_rsi_array) > self.back_window:
                self.back_rsi_array.pop(0)
            
            # 是否完成初始化
            self.inited = len(self.back_rsi_array) >= self.back_window

            if self.inited:
                rsi_array = np.array(self.back_rsi_array)
                # 多头信号计算
                self.long_range_signal: bool = (np.all(rsi_array > self.long_rsi_lower) and np.all(rsi_array < self.long_rsi_upper))
                self.long_mmt_signal: bool = np.any(rsi_array > self.long_rsi_highest)

                # 空头信号计算
                self.short_range_signal: bool = (np.all(rsi_array > self.short_rsi_lower) and np.all(rsi_array < self.short_rsi_upper))
                self.short_mmt_signal: bool = np.any(rsi_array < self.short_rsi_lowest)

    def generate_signal(self) -> Direction:
        direction = Direction.NET

        # 多头信号判断
        if self.long_range_signal and self.long_mmt_signal:
            direction = Direction.LONG

        # 空头信号判断
        if self.short_range_signal and self.short_mmt_signal:
            direction = Direction.SHORT

        return direction


if __name__ == "__main__":
    indicator = RsiRangeMomentum()
    symbol = f"BTCUSDT.BINANCE"
    start_dt = datetime.strptime(f"2023-10-01 00:00:00", f"%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(f"2023-12-31 00:00:00", f"%Y-%m-%d %H:%M:%S")

    mc = MongoClient()
    db = mc[MinuteDataBaseName(5)]
    flt = {"datetime": {"$gte": start_dt, "$lte": end_dt}}
    collection = db[symbol]
    cursor = collection.find(flt).sort("datetime")
    pre_signal = Direction.NET
    for d in cursor:
        bar = BarData(gateway_name="", symbol="", exchange=None, datetime=None)
        bar.__dict__ = d
        indicator.update_bar(bar)
        signal = indicator.generate_signal()
        if signal != pre_signal:
            print(f"{bar.datetime}\t{signal}")
        pre_signal = signal
