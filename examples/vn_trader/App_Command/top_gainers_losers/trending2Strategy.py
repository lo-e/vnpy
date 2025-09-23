# encoding: UTF-8

"""
打新策略
"""
from .trendingStrategy import TrendingStrategy
from vnpy.trader.constant import Direction, Offset
from vnpy.app.cta_strategy.base import *
from datetime import datetime
from vnpy.trader.object import TickData
from vnpy.trader.constant import Exchange
import time

class Trending2Strategy(TrendingStrategy):
    className = "Trending2Strategy"
    author = "loe"

    def __init__(self, ctaEngine, setting):
        # 完成setting.json参数的配置
        super(Trending2Strategy, self).__init__(
            ctaEngine=ctaEngine, setting=setting
        )

    def check_indicator_inited(self):
        if not self.target_pos:
            if self.direction == Direction.LONG:
                if self.hour_up and self.hour_down and self.minute_recent_up and self.minute_recent_down and self.minute_bar.datetime.timestamp() <= self.hour_up_ts + 60 * 60 and self.hour_down <= self.minute_recent_down <= self.hour_up - abs(self.hour_up - self.hour_down) * 0.7:
                    self.indicator_inited = True
                    self.indicator_inited_dt = self.minute_bar_dt
                
                else:
                    self.indicator_inited = False
                    self.indicator_inited_dt = ""

            if self.direction == Direction.SHORT:
                if self.hour_up and self.hour_down and self.minute_recent_up and self.minute_recent_down and self.minute_bar.datetime.timestamp() <= self.hour_down_ts + 60 * 60 and self.hour_up >= self.minute_recent_up >= self.hour_down + abs(self.hour_up - self.hour_down) * 0.7:
                    self.indicator_inited = True
                    self.indicator_inited_dt = self.minute_bar_dt

                else:
                    self.indicator_inited = False
                    self.indicator_inited_dt = ""

    def on_tick(self, tick: TickData):
        super().on_tick(tick)
        if not self.trading:
            return

        # 1h新高新低
        price_cross = False
        if self.database_loaded and ((self.direction == Direction.LONG and self.hour_up and tick.last_price > self.hour_up) or (self.direction == Direction.SHORT and self.hour_down and tick.last_price < self.hour_down)):
            price_cross = True
            self.hour_up_down_updated = True 

        # 开仓判断
        if not self.target_pos and self.database_loaded and self.indicator_inited and price_cross and not self.stop_open and not self.closed:
            open_allowed = False
            if (self.direction == Direction.LONG and self.history_high_cross) or (self.direction == Direction.SHORT and self.history_low_cross):
                open_allowed = True

            if open_allowed:
                self.add_unit_pos(tick.last_price)

                # 发送订单
                # if not self.pos and self.portfolio.trade_enable and time.time() <= tick.datetime.timestamp() + 3:
                #     open_volume = abs(self.target_pos)
                #     if open_volume:
                #         if self.direction == Direction.LONG:
                #             trade_price = self.tick.last_price * 1.005
                #             self.cancel_all()
                #             if self.exchange == Exchange.BINANCE:
                #                 self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(open_volume), market=True)
                #                 self.send_order(Direction.SHORT, Offset.CLOSE, self.stop_price, abs(open_volume), stop=True)

                #             else:
                #                 self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(open_volume), market=True, stop_loss_price=self.stop_price)
                        
                #         elif self.direction == Direction.SHORT:
                #             trade_price = self.tick.last_price * 0.995
                #             self.cancel_all()
                #             if self.exchange == Exchange.BINANCE:
                #                 self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(open_volume), market=True)
                #                 self.send_order(Direction.LONG, Offset.CLOSE, self.stop_price, abs(open_volume), stop=True)

                #             else:
                #                 self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(open_volume), market=True, stop_loss_price=self.stop_price)

                # 开仓日志
                self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {tick.datetime.replace(microsecond=0)} OPEN {self.leverage:.2f} {tick.last_price}"})
                self.trade_logs_updated = True

                msg = f"{self.vt_symbol} {self.direction.value}\n开仓（{self.open_count}）"
                self.cta_engine.main_engine.send_ding_talk(msg)

        # 止损判断
        if self.target_pos and ((self.direction == Direction.LONG and tick.last_price <= self.stop_price) or (self.direction == Direction.SHORT and tick.last_price >= self.stop_price)):
            self.stop_tick_price = tick.last_price
            self.stop_tick_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
            self.target_pos = 0
            self.portfolio.strategy_status_check_ts[self.strategy_name] = 0
            if not self.stop_open:
                self.stop_open = True
                self.stop_open_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

            stop_pnl = 0
            if self.open_tick_price:
                stop_pnl = ((tick.last_price / self.open_tick_price) - 1) * 100
                if self.direction == Direction.SHORT:
                    stop_pnl *= -1
                stop_pnl -= 0.2
                stop_pnl *= self.leverage
            self.pnl += stop_pnl

            self.on_close(tick)

            # 止损日志
            self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {tick.datetime.replace(microsecond=0)} STOP {self.pnl:.2f}% {tick.last_price}"})
            self.trade_logs_updated = True

            msg = f"{self.vt_symbol} {self.direction.value}\n止损 {self.pnl:.2f}%"
            self.cta_engine.main_engine.send_ding_talk(msg)

        # 平仓判断
        if self.target_pos and self.database_loaded and tick.datetime > self.open_tick_dt + timedelta(hours=6) and ((self.direction == Direction.LONG and tick.last_price < self.hour_down) or (self.direction == Direction.SHORT and tick.last_price > self.hour_up)):
            self.target_pos = 0
            self.portfolio.strategy_status_check_ts[self.strategy_name] = 0
            if not self.stop_open:
                self.stop_open = True
                self.stop_open_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

            close_pnl = 0
            if self.open_tick_price:
                close_pnl = ((tick.last_price / self.open_tick_price) - 1) * 100
                if self.direction == Direction.SHORT:
                    close_pnl *= -1
                close_pnl -= 0.2
                close_pnl *= self.leverage
            self.pnl += close_pnl

            self.on_close(tick)
            
            # 平仓日志
            self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {self.tick.datetime.replace(microsecond=0)} CLOSE {self.pnl:.2f}% {tick.last_price}"})
            self.trade_logs_updated = True

            msg = f"{self.vt_symbol} {self.direction.value}\n平仓 {self.pnl:.2f}%"
            self.cta_engine.main_engine.send_ding_talk(msg)

        # 手动平仓
        if self.manual_close:
            if self.target_pos:
                self.target_pos = 0
                self.portfolio.strategy_status_check_ts[self.strategy_name] = 0
                if not self.stop_open:
                    self.stop_open = True
                    self.stop_open_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

                close_pnl = 0
                if self.open_tick_price:
                    close_pnl = ((tick.last_price / self.open_tick_price) - 1) * 100
                    if self.direction == Direction.SHORT:
                        close_pnl *= -1
                    close_pnl -= 0.2
                    close_pnl *= self.leverage
                self.pnl += close_pnl

                self.on_close(tick)
                
                # 平仓日志
                self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {self.tick.datetime.replace(microsecond=0)} MANUAL_CLOSE {self.pnl:.2f}% {tick.last_price}"})
                self.trade_logs_updated = True

                msg = f"{self.vt_symbol} {self.direction.value}\n手动平仓 {self.pnl:.2f}%"
                self.cta_engine.main_engine.send_ding_talk(msg)
            
            else:
                self.on_close(tick)

        # 无信号退出
        if not self.target_pos and self.database_loaded and self.hour_up and self.hour_down:
            if self.direction == Direction.LONG:
                if tick.last_price < self.hour_down or tick.datetime.timestamp() >= self.hour_up_ts + 6 * 60 * 60:
                    self.on_close(tick)
            
            if self.direction == Direction.SHORT:
                if tick.last_price > self.hour_up or tick.datetime.timestamp() >= self.hour_down_ts + 6 * 60 * 60:
                    self.on_close(tick)

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")