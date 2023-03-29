#-- coding: utf-8 --

from typing import Callable, Optional
from vnpy.trader.object import BarData, TickData
from vnpy.trader.constant import Exchange, Interval
from pymongo import MongoClient, ASCENDING
from vnpy.app.cta_strategy.base import (MINUTE_DB_NAME, HOUR_DB_NAME, MinuteDataBaseName, HourDataBaseName)
import re
from datetime import datetime

# 将上一级目录添加到模块搜索路径中
import sys
sys.path.append('..')  
from vn_trader.App.Turtle_crypto.dataservice.BybitDataService import bybit_get_symbol_list, BybitSymbolType

class BarGenerator:
    """
    For:
    1. generating 1 minute bar data from tick data
    2. generating x minute bar/x hour bar data from 1 minute data

    Notice:
    1. for x minute bar, x must be able to divide 60: 2, 3, 5, 6, 10, 15, 20, 30
    2. for x hour bar, x can be any number
    """

    def __init__(
        self,
        on_bar: Callable = None,
        window: int = 0,
        on_window_bar: Callable = None,
        interval: Interval = Interval.MINUTE
    ):
        """Constructor"""
        self.bar: BarData = None
        self.on_bar: Callable = on_bar

        self.interval: Interval = interval
        self.interval_count: int = 0

        self.hour_bar: BarData = None

        self.window: int = window
        self.window_bar: BarData = None
        self.on_window_bar: Callable = on_window_bar

        self.last_tick: TickData = None

    def update_tick(self, tick: TickData) -> None:
        """
        Update new tick data into generator.
        """
        new_minute = False

        # Filter tick data with 0 last price
        if not tick.last_price:
            return

        # Filter tick data with older timestamp
        if self.last_tick and tick.datetime < self.last_tick.datetime:
            return

        if not self.bar:
            new_minute = True
        elif (
            (self.bar.datetime.minute != tick.datetime.minute)
            or (self.bar.datetime.hour != tick.datetime.hour)
        ):
            self.bar.datetime = self.bar.datetime.replace(
                second=0, microsecond=0
            )
            self.on_bar(self.bar)

            new_minute = True

        if new_minute:
            self.bar = BarData(
                symbol=tick.symbol,
                exchange=tick.exchange,
                interval=Interval.MINUTE,
                datetime=tick.datetime,
                gateway_name=tick.gateway_name,
                open_price=tick.last_price,
                high_price=tick.last_price,
                low_price=tick.last_price,
                close_price=tick.last_price,
                open_interest=tick.open_interest
            )
        else:
            self.bar.high_price = max(self.bar.high_price, tick.last_price)
            if tick.high_price > self.last_tick.high_price:
                self.bar.high_price = max(self.bar.high_price, tick.high_price)

            self.bar.low_price = min(self.bar.low_price, tick.last_price)
            if tick.low_price < self.last_tick.low_price:
                self.bar.low_price = min(self.bar.low_price, tick.low_price)

            self.bar.close_price = tick.last_price
            self.bar.open_interest = tick.open_interest
            self.bar.datetime = tick.datetime

        if self.last_tick:
            volume_change = tick.volume - self.last_tick.volume
            self.bar.volume += max(volume_change, 0)

            turnover_change = tick.turnover - self.last_tick.turnover
            self.bar.turnover += max(turnover_change, 0)

        self.last_tick = tick

    def update_bar(self, bar: BarData) -> None:
        """
        Update 1 minute bar into generator
        """
        if self.interval == Interval.MINUTE:
            self.update_bar_minute_window(bar)
        else:
            self.update_bar_hour_window(bar)

    def update_bar_minute_window(self, bar: BarData) -> None:
        """"""
        # If not inited, create window bar object
        if not self.window_bar:
            dt = bar.datetime.replace(second=0, microsecond=0)
            self.window_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price
            )
        # Otherwise, update high/low price into window bar
        else:
            self.window_bar.high_price = max(
                self.window_bar.high_price,
                bar.high_price
            )
            self.window_bar.low_price = min(
                self.window_bar.low_price,
                bar.low_price
            )

        # Update close price/volume/turnover into window bar
        self.window_bar.close_price = bar.close_price
        self.window_bar.volume += bar.volume
        self.window_bar.turnover += bar.turnover
        self.window_bar.open_interest = bar.open_interest

        # Check if window bar completed
        if not (bar.datetime.minute + 1) % self.window:
            self.on_window_bar(self.window_bar)
            self.window_bar = None

    def update_bar_hour_window(self, bar: BarData) -> None:
        """"""
        # If not inited, create window bar object
        if not self.hour_bar:
            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)
            self.hour_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price,
                volume=bar.volume,
                turnover=bar.turnover,
                open_interest=bar.open_interest
            )
            return

        finished_bar = None

        # If minute is 59, update minute bar into window bar and push
        if bar.datetime.minute == 59:
            self.hour_bar.high_price = max(
                self.hour_bar.high_price,
                bar.high_price
            )
            self.hour_bar.low_price = min(
                self.hour_bar.low_price,
                bar.low_price
            )

            self.hour_bar.close_price = bar.close_price
            self.hour_bar.volume += bar.volume
            self.hour_bar.turnover += bar.turnover
            self.hour_bar.open_interest = bar.open_interest

            finished_bar = self.hour_bar
            self.hour_bar = None

        # If minute bar of new hour, then push existing window bar
        elif bar.datetime.hour != self.hour_bar.datetime.hour:
            finished_bar = self.hour_bar

            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)
            self.hour_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price,
                volume=bar.volume,
                turnover=bar.turnover,
                open_interest=bar.open_interest
            )
        # Otherwise only update minute bar
        else:
            self.hour_bar.high_price = max(
                self.hour_bar.high_price,
                bar.high_price
            )
            self.hour_bar.low_price = min(
                self.hour_bar.low_price,
                bar.low_price
            )

            self.hour_bar.close_price = bar.close_price
            self.hour_bar.volume += bar.volume
            self.hour_bar.turnover += bar.turnover
            self.hour_bar.open_interest = bar.open_interest

        # Push finished window bar
        if finished_bar:
            self.on_hour_bar(finished_bar)

    def on_hour_bar(self, bar: BarData) -> None:
        """"""
        if self.window == 1:
            self.on_window_bar(bar)
        else:
            if not self.window_bar:
                self.window_bar = BarData(
                    symbol=bar.symbol,
                    exchange=bar.exchange,
                    datetime=bar.datetime,
                    gateway_name=bar.gateway_name,
                    open_price=bar.open_price,
                    high_price=bar.high_price,
                    low_price=bar.low_price
                )
            else:
                self.window_bar.high_price = max(
                    self.window_bar.high_price,
                    bar.high_price
                )
                self.window_bar.low_price = min(
                    self.window_bar.low_price,
                    bar.low_price
                )

            self.window_bar.close_price = bar.close_price
            self.window_bar.volume += bar.volume
            self.window_bar.turnover += bar.turnover
            self.window_bar.open_interest = bar.open_interest

            self.interval_count += 1
            if not self.interval_count % self.window:
                self.interval_count = 0
                self.on_window_bar(self.window_bar)
                self.window_bar = None

    def generate(self) -> Optional[BarData]:
        """
        Generate the bar data and call callback immediately.
        """
        bar = self.bar

        if self.bar:
            bar.datetime = bar.datetime.replace(second=0, microsecond=0)
            self.on_bar(bar)

        self.bar = None
        return bar

class MinuterBarProcessor:
    def __init__(
        self,
        vt_symbol: str = '',
        window: int = 0,
        interval: Interval = Interval.MINUTE,
        start_date: str = '',
        end_date: str = '',
    ):
        self.vt_symbol = vt_symbol
        self.window = window
        self.interval = interval
        if start_date:
            self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            self.start_date = None
        if end_date:
            self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            self.end_date = None

        # minute_bar数据库
        client = MongoClient('localhost', 27017)
        minute_bar_db = client[MINUTE_DB_NAME]
        self.minute_bar_collection = minute_bar_db[self.vt_symbol]

        # window_bar数据库
        if self.interval == Interval.MINUTE:
            window_bar_db = client[MinuteDataBaseName(self.window)]
        else:
            window_bar_db = client[HourDataBaseName(self.window)]
        self.window_bar_collection = window_bar_db[self.vt_symbol]
        self.window_bar_collection.create_index('datetime')

        self.bar_generator = BarGenerator(window=self.window, on_window_bar=self.on_window_bar, interval=self.interval)

    def on_window_bar(self, bar:BarData):
        self.window_bar_collection.update_many({'datetime': bar.datetime}, {'$set': bar.__dict__}, upsert=True)

    def start_work(self):
        start_dt = None
        end_dt = None
        minute_bar = None

        flt = {}
        flt_data = {}
        if self.start_date:
            flt_data['$gte'] = self.start_date
        if self.end_date:
            flt_data['$lte'] = self.end_date

        if flt_data:
            flt['datetime'] = flt_data
        cursor = self.minute_bar_collection.find(flt).sort('datetime', ASCENDING)
        for d in cursor:
            minute_bar = BarData(gateway_name='', symbol='', exchange=None, datetime=None)
            minute_bar.__dict__ = d
            if not start_dt:
                start_dt = minute_bar.datetime
            self.bar_generator.update_bar(minute_bar)

        if minute_bar:
            end_dt = minute_bar.datetime

        interval_ = re.sub("\d", '', self.interval.value)
        print(f'{self.vt_symbol}\n1m -> {self.window}{interval_}\n{start_dt} -> {end_dt}')

if __name__ == '__main__':
    symbol_list = ['BTCUSDT.BYBIT', 'ETHUSDT.BYBIT', 'SOLUSDT.BYBIT', 'GALAUSDT.BYBIT', 'AVAXUSDT.BYBIT', 'XRPUSDT.BYBIT']
    # symbol_list = bybit_get_symbol_list(type=BybitSymbolType.USDT)
    for symbol in symbol_list:
        processor = MinuterBarProcessor(vt_symbol=symbol, window=1, interval=Interval.HOUR, start_date='2023-01-20', end_date='2023-12-31')
        processor.start_work()
        print('\n')