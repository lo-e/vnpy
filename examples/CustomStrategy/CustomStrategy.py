# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset, Exchange
from vnpy.trader.utility import ArrayManager, BarGenerator
from datetime import  datetime, timedelta
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
import re
from vnpy.app.cta_strategy.base import (DAILY_DB_NAME, DOMINANT_DB_NAME)
from vnpy.trader.utility import round_to

class CustomResult(object):
    """一次完整的开平交易"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.unit = 0
        self.entry = 0                  # 开仓均价
        self.exit = 0                   # 平仓均价
        self.pnl = 0                    # 盈亏
    
    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓或者加仓"""
        cost = self.unit * self.entry    # 计算之前的开仓成本
        cost += change * price           # 加上新仓位的成本
        self.unit += change              # 加上新仓位的数量
        self.entry = cost / self.unit    # 计算新的平均开仓成本

    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.exit = price
        self.pnl = self.unit * (self.exit - self.entry)
    
class CustomSignal(object):
    
    def __init__(self, portfolio, symbol_setting):
        self.portfolio = portfolio                                                                          # 投资组合
        self.symbol = symbol_setting["symbol"]                                                              # 合约代码
        self.from_dt =  symbol_setting["from"]          
        self.from_dt = datetime.strptime(self.from_dt, f"%Y-%m-%d %H:%M:%S")                                # 策略开始执行的bar时间
        self.direction = Direction.LONG if symbol_setting["direction"] == "LONG" else Direction.SHORT       # 交易方向
        self.long_window = symbol_setting["long_window"]                                                    # 止损价格参考的窗口数
        self.short_window = symbol_setting["short_window"]                                                  # 开仓价格参考的窗口数
        self.max_loss_count = symbol_setting["max_loss_count"]                                              # 允许的最大止损次数
        self.profit_rate = symbol_setting["profit_rate"]                                                    # 开仓要求的最低预期收益率
        self.loss_rate = symbol_setting["loss_rate"]                                                        # 单次持仓允许的最大亏损率
        self.stop_profit_price = symbol_setting["stop_profit_price"]                                        # 止盈价
        self.stop_price_up = symbol_setting["stop_price_up"]                                                # 向上突破该价格停止尝试
        self.stop_price_down = symbol_setting["stop_price_down"]                                            # 向下突破该价格停止尝试

        self.pos = 0                                                                                        # 持仓
        self.pos_open_price = 0                                                                             # 开仓价格
        self.stop_loss_price = 0                                                                            # 持仓止损价格
        
        self.long_up = 0                                                                                    # 止损最高价
        self.long_down = 0                                                                                  # 止损最低价
        self.short_up = 0                                                                                   # 开仓向上突破价
        self.short_down = 0                                                                                 # 开仓向下突破价

        self.indicator_inited = False                                                                       # 数据初始化状态
        self.profit_stop = False                                                                            # 止盈状态
        self.loss_count = 0                                                                                 # 止损次数
        self.open_stop = False                                                                              # 停止开新的仓位
        self.next_dt = None                                                                                 # 下一bar的时间，为了测试bar是否缺失
        self.bar = None                                                                                     # 当前最新bar
        self.am = ArrayManager(self.long_window)                                                            # K线容器
        self.bar_generator = BarGenerator(on_bar=None, window=5, on_window_bar=self.on_window_bar)          # bar生成工具

    def on_bar(self, bar):
        """ 
        # 测试bar是否缺失
        print(f"{bar.symbol}\t{bar.datetime}")
        if self.next_dt:
            if bar.datetime != self.next_dt:
                raise("error")
        self.next_dt = bar.datetime + timedelta(minutes=1)
        """
        self.bar_generator.update_bar(bar)
    
    def on_window_bar(self, bar):
        self.am.update_bar(bar)
        if not self.am.inited:
            return
        
        self.bar = bar
        self.generate_signal()
        self.calculate_indicator()

    def generate_signal(self):
        if not self.long_up:
            return
        
        if self.profit_stop or self.loss_count >= self.max_loss_count:
            return
        
        if not self.pos:
            if self.open_stop:
                # 停止开新的仓位
                return
            
            if self.direction == Direction.LONG:
                if self.bar.high_price >= self.short_up:
                    # 多头开仓
                    price = max(self.bar.open_price, self.short_up)
                    lever = self.loss_rate / abs(((self.long_down / price) - 1))
                    if ((self.stop_profit_price / price) - 1) * lever >= self.profit_rate:
                        value = self.portfolio.portfolioValue * lever
                        volume = value / price
                        self.send_order(Direction.LONG, Offset.OPEN, price, volume)
                        self.pos_open_price = price
                        self.stop_loss_price = self.long_down
                        return
            
            else:
                if self.bar.low_price <= self.short_down:
                    # 空头开仓
                    price = min(self.bar.open_price, self.short_down)
                    lever = self.loss_rate / abs(((self.long_up / price) - 1))
                    if (1 - (self.stop_profit_price / price)) * lever >= self.profit_rate:
                        value = self.portfolio.portfolioValue * lever
                        volume = value / price
                        self.send_order(Direction.SHORT, Offset.OPEN, price, volume)
                        self.pos_open_price = price
                        self.stop_loss_price = self.long_up
                        return

        else:
            if self.direction == Direction.LONG:
                if self.bar.high_price >= self.stop_profit_price:
                    # 多头止盈
                    price = max(self.bar.open_price, self.stop_profit_price)
                    self.send_order(Direction.SHORT, Offset.CLOSE, price, abs(self.pos))
                    self.pos_open_price = 0
                    self.stop_loss_price = 0
                    self.profit_stop = True
                    return
                
                if self.bar.low_price <= self.stop_loss_price:
                    # 多头止损
                    price = min(self.bar.open_price, self.stop_loss_price)
                    self.send_order(Direction.SHORT, Offset.CLOSE, price, abs(self.pos))
                    self.pos_open_price = 0
                    self.stop_loss_price = 0
                    self.loss_count += 1
                    return
                
                if self.bar.high_price >= self.short_up:
                    # 多头加仓
                    if ((self.long_down / self.pos_open_price) - 1) >= abs((self.stop_loss_price / self.pos_open_price) - 1):
                        price = max(self.bar.open_price, self.short_up)
                        if 1 / abs(((self.long_down / price) - 1)) >= 1.5 / abs(((self.stop_loss_price / self.pos_open_price) - 1)):
                            # 满足多头加仓条件
                            value = self.portfolio.portfolioValue * self.loss_rate / abs(((self.long_down / price) - 1))
                            volume = value / price
                            add_volume = volume - abs(self.pos)
                            if add_volume > 0:
                                self.send_order(Direction.LONG, Offset.OPEN, price, add_volume)
                                self.pos_open_price = price
                                self.stop_loss_price = self.long_down
                                self.max_loss_count += 1
                                return

            else:
                if self.bar.low_price <= self.stop_profit_price:
                    # 空头止盈
                    price = min(self.bar.open_price, self.stop_profit_price)
                    self.send_order(Direction.LONG, Offset.CLOSE, price, abs(self.pos))
                    self.pos_open_price = 0
                    self.stop_loss_price = 0
                    self.profit_stop = True
                    return
                
                if self.bar.high_price >= self.stop_loss_price:
                    # 空头止损
                    price = max(self.bar.open_price, self.stop_loss_price)
                    self.send_order(Direction.LONG, Offset.CLOSE, price, abs(self.pos))
                    self.pos_open_price = 0
                    self.stop_loss_price = 0
                    self.loss_count += 1
                    return
                
                if self.bar.low_price <= self.short_down:
                    # 空头加仓
                    if ((self.long_up / self.pos_open_price) - 1) * -1 >= abs((self.stop_loss_price / self.pos_open_price) - 1):
                        price = min(self.bar.open_price, self.short_down)
                        if 1 / abs(((self.long_up / price) - 1)) >= 1.5 / abs(((self.stop_loss_price / self.pos_open_price) - 1)):
                            # 满足空头加仓条件
                            value = self.portfolio.portfolioValue * self.loss_rate / abs(((self.long_up / price) - 1))
                            volume = value / price
                            add_volume = volume - abs(self.pos)
                            if add_volume > 0:
                                self.send_order(Direction.SHORT, Offset.OPEN, price, add_volume)
                                self.pos_open_price = price
                                self.stop_loss_price = self.long_up
                                self.max_loss_count += 1
                                return

    def send_order(self, direction, offset, price, volume):
        # 精度处理
        price_tick = self.portfolio.engine.pricetick_dict[self.symbol]
        price = round_to(price, price_tick)

        min_volume = self.portfolio.engine.min_volume_dict[self.symbol]
        volume = round_to(volume, min_volume)

        # 当前持仓
        if direction == Direction.LONG:
            self.pos += volume

        else:
            self.pos -= volume

        # 发出订单
        self.portfolio.sendOrder(self, direction, offset, price, volume)

        #"""
        print(f"{self.bar.datetime}\t{direction.value}\t{offset.value}\t{price}\t{volume}")
        if offset == Offset.CLOSE:
            print(f"\n")
        #"""

    def calculate_indicator(self):
        # 开始时间过滤
        if self.bar.datetime < self.from_dt:
            return
        
        # 止盈价格
        self.long_up, self.long_down = self.am.donchian(self.long_window)
        if not self.indicator_inited:
            if self.direction == Direction.LONG:
                if self.long_down == self.bar.low_price:
                    self.indicator_inited = True
                
                else:
                    self.long_up = 0
                    self.long_down = 0
                    return
            
            else:
                if self.long_up == self.bar.high_price:
                    self.indicator_inited = True
                
                else:
                    self.long_up = 0
                    self.long_down = 0
                    return

        # 突破开仓价格
        self.short_up, self.short_down = self.am.donchian(self.short_window)

        # 判断是否价格突破开仓上限/下限，然后停止开新仓位
        if self.bar.high_price >= self.stop_price_up or self.bar.low_price <= self.stop_price_down:
            self.open_stop = True