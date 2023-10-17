from pyrsistent import v
from vnpy.trader.object import BarData
from vnpy.trader.utility import ArrayManager
from vnpy.trader.constant import Direction
import talib
import numpy as np
from pymongo import MongoClient
from vnpy.app.cta_strategy.base import HOUR_DB_NAME, MinuteDataBaseName
from datetime import datetime
from enum import Enum


class SqueezeStatus(Enum):
    sqz_on = "挤压"
    sqz_off = "爆发"
    no_sqz = "无"


class SqueezeMomentum(object):
    def __init__(
        self,
        bb_length: int = 20,
        bb_factor: int = 2.0,
        kc_length: int = 20,
        kc_factor: int = 1.5,
    ) -> None:
        self.bb_length: int = bb_length
        self.bb_factor: int = bb_factor
        self.kc_length: int = kc_length
        self.kc_factor: int = kc_factor

        self.sqz = SqueezeStatus.no_sqz
        self.pre_sqz = SqueezeStatus.no_sqz
        self.mmt = 0.0
        self.pre_mmt = 0.0
        self.inited = False
        self.bar = None
        self.array_manager = ArrayManager(max(bb_length, kc_length) + 1)

    def update_bar(self, bar: BarData) -> None:
        self.bar = bar
        self.array_manager.update_bar(bar)
        self.inited = self.array_manager.inited
        if self.inited:
            # 计算布林带通道
            upper_bb, lower_bb = self.array_manager.boll(self.bb_length, self.bb_factor)

            # 计算肯特纳通道
            upper_kc, lower_kc = self.array_manager.keltner(
                self.kc_length, self.kc_factor
            )

            # 挤压状态
            self.pre_sqz = self.sqz
            if (lower_bb > lower_kc) and (upper_bb < upper_kc):
                self.sqz = SqueezeStatus.sqz_on

            elif (lower_bb < lower_kc) and (upper_bb > upper_kc):
                self.sqz = SqueezeStatus.sqz_off

            else:
                self.sqz = SqueezeStatus.no_sqz

            # 动量指标
            high = np.max(self.array_manager.high_array[-self.kc_length :])
            low = np.min(self.array_manager.low_array[-self.kc_length :])
            sma = self.array_manager.sma(self.kc_length)
            avg = (((high + low) / 2.0) + sma) / 2.0
            self.pre_mmt = self.mmt
            self.mmt = talib.LINEARREG(
                self.array_manager.close_array - avg, self.kc_length
            )[-1]

    def generate_signal(self) -> Direction:
        direction = Direction.NET
        if self.pre_sqz == SqueezeStatus.sqz_on and self.sqz == SqueezeStatus.sqz_off:
            if self.mmt > 0 and self.mmt > self.pre_mmt:
                direction = Direction.LONG

            if self.mmt < 0 and self.mmt < self.pre_mmt:
                direction = Direction.SHORT

        return direction


if __name__ == "__main__":
    indicator = SqueezeMomentum(bb_length=20, bb_factor=2, kc_length=20, kc_factor=1.5)
    symbol = f"BTCUSDT.BINANCE"
    start_dt = datetime.strptime(f"2023-10-01 00:00:00", f"%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(f"2023-12-31 00:00:00", f"%Y-%m-%d %H:%M:%S")

    mc = MongoClient()
    db = mc[MinuteDataBaseName(5)]
    flt = {"datetime": {"$gte": start_dt, "$lte": end_dt}}
    collection = db[symbol]
    cursor = collection.find(flt).sort("datetime")
    for d in cursor:
        bar = BarData(gateway_name="", symbol="", exchange=None, datetime=None)
        bar.__dict__ = d
        indicator.update_bar(bar)
        signal = indicator.generate_signal()
        if signal != Direction.NET:
            print(f"{bar.datetime}\t{signal}")
