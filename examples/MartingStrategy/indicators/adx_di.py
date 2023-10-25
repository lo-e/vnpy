from rqdatac import init
from vnpy.trader.object import BarData
from vnpy.trader.utility import ArrayManager
from vnpy.trader.constant import Direction
from pymongo import MongoClient
from vnpy.app.cta_strategy.base import HOUR_DB_NAME, MinuteDataBaseName
from datetime import datetime

class AdxDi(object):
    """
    ADX趋势强度和DI趋势方向指标（来自：TraderView）
    """

    # ADX趋势强弱阈值
    threshold: int = 20

    def __init__(
        self,
        back_window:int = 14
    ) -> None:
        self.back_window = back_window # 统计回望窗口

        self.inited = False
        self.adx = 0
        self.plus_di = 0
        self.minus_di = 0

        self.array_manager = ArrayManager()

    def update_bar(self, bar: BarData) -> None:
        self.bar = bar
        self.array_manager.update_bar(bar)
        self.inited = self.array_manager.inited
        if self.inited:
            self.adx = self.array_manager.adx(self.back_window)
            self.plus_di = self.array_manager.plus_di(self.back_window)
            self.minus_di = self.array_manager.minus_di(self.back_window)
        
        # fake
        if bar.datetime >= datetime.strptime("2023-10-25 01:00:00", "%Y-%m-%d %H:%M:%S"):
            a = 2

    def generate_signal(self) -> Direction:
        direction = Direction.NET
        if self.inited:
            if self.adx > self.threshold:
                if self.plus_di > self.minus_di:
                    direction = Direction.LONG
                
                elif self.plus_di < self.minus_di:
                    direction = Direction.SHORT

        return direction


if __name__ == "__main__":
    indicator = AdxDi()
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
