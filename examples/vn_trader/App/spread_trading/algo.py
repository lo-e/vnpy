from typing import Any

from vnpy.trader.constant import Direction, Offset
from vnpy.trader.object import (TickData, OrderData, TradeData)
from vnpy.trader.utility import round_to

from vnpy.app.spread_trading.template import SpreadAlgoTemplate, check_tick_valid
from vnpy.app.spread_trading.base import SpreadData

""" modify by loe """
from threading import Thread
from time import sleep

class SpreadTakerAlgo(SpreadAlgoTemplate):
    """"""
    algo_name = "SpreadTaker"

    def __init__(
        self,
        algo_engine: Any,
        algoid: str,
        spread: SpreadData,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        payup: int,
        interval: int,
        lock: bool
    ):
        """"""
        super().__init__(
            algo_engine, algoid, spread,
            direction, offset, price, volume,
            payup, interval, lock
        )

        self.cancel_interval: int = 2
        self.timer_count: int = 0

        """ modify by loe """
        self.tick_processing = False
        # 检查Order撤销或者拒单频率，过高则停止算法对应的策略并邮件通知
        self.order_failed_count = 0
        self.check_order_failed_timer = Thread(target=self.check_order_failed)
        self.check_order_failed_timer.start()

    def on_tick(self, tick: TickData):
        """"""
        if self.tick_processing:
            return
        self.tick_processing = True

        if not check_tick_valid(tick=tick):
            self.write_log(f'======算法{self.algo_name} 过滤无效tick：{tick.vt_symbol}\t{tick.datetime} ======')
            self.tick_processing = False
            return

        # Return if tick not inited
        if not self.spread.bid_volume or not self.spread.ask_volume:
            self.tick_processing = False
            return

        # Return if there are any existing orders
        if not self.check_order_finished():
            self.tick_processing = False
            return

        # Hedge if active leg is not fully hedged
        if not self.check_hedge_finished():
            self.hedge_passive_legs()
            self.hedge_active_legs()
            self.tick_processing = False
            return

        # 没有活动订单，没有断腿，ready_open_traded清空
        self.ready_open_traded = 0

        # Otherwise check if should take active leg
        if self.direction == Direction.LONG:
            if self.spread.ask_price <= self.price:
                self.take_active_passive_leg()
                self.tick_processing = False
                return

        elif self.direction == Direction.SHORT:
            if self.spread.bid_price >= self.price:
                self.take_active_passive_leg()
                self.tick_processing = False
                return

        # 强行平仓
        if self.offset == Offset.CLOSE and self.close_anyway:
            self.take_active_passive_leg()

        self.tick_processing = False

    def on_order(self, order: OrderData):
        """"""
        if order.is_failed():
            self.order_failed_count += 1

    """ modify by loe """
    def check_order_failed(self):
        while True:
            if self.order_failed_count > 20:
                # 触发风控机制，撤销或者拒单频率过高，停止算法、停止对应的策略、邮件通知
                strategy_engine = self.algo_engine.spread_engine.strategy_engine
                strategy = strategy_engine.algo_strategy_map.get(self.algoid, None)
                if strategy:
                    strategy_engine.stop_strategy(strategy.strategy_name)
                    self.manual_stop()
                try:
                    msg = f'{self.algoid}\n{self.spread.active_leg.vt_symbol}\nfailed_count：{self.order_failed_count}'
                    self.algo_engine.main_engine.send_email(subject='ALGO_ORDER_FAILED 风控触发', content=msg)
                except:
                    pass
            self.order_failed_count = 0
            sleep(50)


    def on_trade(self, trade: TradeData):
        """"""
        pass

    def on_interval(self):
        """"""
        if not self.check_order_finished():
            self.cancel_all_order()

    def take_active_leg(self):
        """"""
        # Calculate spread order volume of new round trade
        spread_volume_left = self.target - self.traded
        if not spread_volume_left:
            return

        if self.direction == Direction.LONG:
            spread_order_volume = self.spread.ask_volume
            spread_order_volume = min(spread_order_volume, spread_volume_left)
        else:
            spread_order_volume = -self.spread.bid_volume
            spread_order_volume = max(spread_order_volume, spread_volume_left)

        """ modify by loe """
        # 开仓算法只做一次委托执行
        """
        if self.offset == Offset.OPEN:
            self.target = self.traded + spread_order_volume
        """

        # Calculate active leg order volume
        leg_order_volume = self.spread.calculate_leg_volume(
            self.spread.active_leg.vt_symbol,
            spread_order_volume
        )

        # Send active leg order
        self.send_leg_order(
            self.spread.active_leg.vt_symbol,
            leg_order_volume
        )

    """ modify by loe """
    # 主动腿被动腿同时委托，只适用于单条被动腿的策略
    def take_active_passive_leg(self):
        """"""
        # Calculate spread order volume of new round trade
        spread_volume_left = self.target - self.traded
        left_abs = abs(self.target) - abs(self.traded)
        if left_abs <= 0:
            return

        if self.direction == Direction.LONG:
            spread_order_volume = self.spread.ask_volume
            spread_order_volume = min(spread_order_volume, spread_volume_left)
        else:
            spread_order_volume = -self.spread.bid_volume
            spread_order_volume = max(spread_order_volume, spread_volume_left)

        # ======================================
        # 风控，开仓保证金不能超限
        if self.offset == Offset.OPEN:
            direction = spread_order_volume / abs(spread_order_volume)
            abs_actual_volume = 0
            temp = 1
            while temp <= abs(spread_order_volume):
                if self.check_bond_over(spread_volume=temp * direction):
                    msg = f'{self.algoid}\n{self.spread.active_leg.vt_symbol}\nspread_volume：{spread_order_volume}\nactural_volume：{abs_actual_volume * direction}'
                    self.algo_engine.main_engine.send_email(subject='BOND_OVER 风控触发', content=msg)
                    break
                else:
                    abs_actual_volume = temp
                    temp += 1

            if abs_actual_volume:
                spread_order_volume = abs_actual_volume * direction
                self.ready_open_traded += spread_order_volume
            else:
                return
        # ======================================

        # Calculate active leg order volume
        active_leg_order_volume = self.spread.calculate_leg_volume(
            self.spread.active_leg.vt_symbol,
            spread_order_volume
        )

        passive_leg = self.spread.passive_legs[0]
        passive_leg_order_volume = self.spread.calculate_leg_volume(
            passive_leg.vt_symbol,
            spread_order_volume
        )

        #======================================
        # 风控，价格接近涨跌停时阻止发单
        active_leg_tick = self.get_tick(self.spread.active_leg.vt_symbol)
        if active_leg_order_volume > 0 and active_leg_tick.limit_up and active_leg_tick.ask_price_1 >= active_leg_tick.limit_up:
            return
        elif active_leg_order_volume < 0 and active_leg_tick.limit_down and active_leg_tick.bid_price_1 <= active_leg_tick.limit_down:
            return

        passice_leg_tick = self.get_tick(passive_leg.vt_symbol)
        if passive_leg_order_volume > 0 and passice_leg_tick.limit_up and passice_leg_tick.ask_price_1 >= passice_leg_tick.limit_up:
            return
        elif passive_leg_order_volume < 0 and passice_leg_tick.limit_down and passice_leg_tick.bid_price_1 <= passice_leg_tick.limit_down:
            return
        #======================================

        # Send active leg order
        self.send_leg_order(
            self.spread.active_leg.vt_symbol,
            active_leg_order_volume
        )

        self.send_leg_order(
            passive_leg.vt_symbol,
            passive_leg_order_volume
        )

    """ modify by loe """
    # 风控，粗略计算持仓占用的保证金，下单前确认是否超出资金容量【只适用于一条被动腿的策略】
    def check_bond_over(self, spread_volume):
        if self.offset == Offset.CLOSE:
            return False

        # 需要确定的保证金
        current_bond = self.calculate_bond(self.spread, spread_volume)

        # 当前活动开仓算法的预备持仓需要的保证金
        algos_ready_bond = 0
        for algo in self.algo_engine.algos.values():
            if algo.is_active() and algo.offset == Offset.OPEN:
                the_algo_bond = self.calculate_bond(algo.spread, algo.ready_open_traded)
                algos_ready_bond += the_algo_bond

        # 所有策略持仓占用的保证金
        strategys_bond = 0
        for strategy in self.algo_engine.spread_engine.strategy_engine.strategies.values():
            strategy_spread = self.algo_engine.spreads.get(strategy.spread_name, None)
            the_strategy_bond = self.calculate_bond(strategy_spread, strategy.spread_pos)
            strategys_bond += the_strategy_bond

        # 判断是否保证金超限
        total_bond = current_bond + algos_ready_bond + strategys_bond
        if total_bond >= self.algo_engine.portfolio_value:
            return True
        else:
            return False

    # 计算保证金
    def calculate_bond(self, spread, spread_volume):
        # 主动腿保证金
        active_vt_symbol = spread.active_leg.vt_symbol
        active_leg_volume = spread.calculate_leg_volume(
            active_vt_symbol,
            spread_volume
        )
        active_tick = self.get_tick(active_vt_symbol)
        active_contract = self.get_contract(active_vt_symbol)
        active_symbol_rate = self.algo_engine.get_symbol_rate(symbol=active_vt_symbol)
        active_bond = active_tick.last_price * active_contract.size * active_symbol_rate * abs(active_leg_volume)

        # 被动腿保证金
        passive_leg = spread.passive_legs[0]
        passive_vt_symbol = passive_leg.vt_symbol
        passive_leg_volume = spread.calculate_leg_volume(
            passive_vt_symbol,
            spread_volume
        )
        passive_tick = self.get_tick(passive_vt_symbol)
        passive_contract = self.get_contract(passive_vt_symbol)
        passive_symbol_rate = self.algo_engine.get_symbol_rate(symbol=active_vt_symbol)
        passive_bond = passive_tick.last_price * passive_contract.size * passive_symbol_rate * abs(passive_leg_volume)

        # 总保证金
        total_spread_bond = active_bond + passive_bond
        return total_spread_bond

    def hedge_passive_legs(self):
        """
        Send orders to hedge all passive legs.
        """
        # Calcualte spread volume to hedge
        active_leg = self.spread.active_leg
        active_traded = self.leg_traded[active_leg.vt_symbol]
        active_traded = round_to(active_traded, self.spread.min_volume)

        hedge_volume = self.spread.calculate_spread_volume(
            active_leg.vt_symbol,
            active_traded
        )

        # Calculate passive leg target volume and do hedge
        for leg in self.spread.passive_legs:
            passive_traded = self.leg_traded[leg.vt_symbol]
            passive_traded = round_to(passive_traded, self.spread.min_volume)

            passive_target = self.spread.calculate_leg_volume(
                leg.vt_symbol,
                hedge_volume
            )

            """ modify by loe """
            if abs(passive_traded) >= abs(passive_target):
                return

            leg_order_volume = passive_target - passive_traded
            if leg_order_volume:
                self.send_leg_order(leg.vt_symbol, leg_order_volume)

    def hedge_active_legs(self):
        # 只适用于一条被动腿的策略
        """
        Send orders to hedge active legs.
        """
        # Calcualte spread volume to hedge
        passive_leg = self.spread.passive_legs[0]
        passive_traded = self.leg_traded[passive_leg.vt_symbol]
        passive_traded = round_to(passive_traded, self.spread.min_volume)

        hedge_volume = self.spread.calculate_spread_volume(
            passive_leg.vt_symbol,
            passive_traded
        )

        # Calculate passive leg target volume and do hedge
        active_leg = self.spread.active_leg
        active_traded = self.leg_traded[active_leg.vt_symbol]
        active_traded = round_to(active_traded, self.spread.min_volume)

        active_target = self.spread.calculate_leg_volume(
            active_leg.vt_symbol,
            hedge_volume
        )

        """ modify by loe """
        if abs(active_traded) >= abs(active_target):
            return

        leg_order_volume = active_target - active_traded
        if leg_order_volume:
            self.send_leg_order(active_leg.vt_symbol, leg_order_volume)

    def send_leg_order(self, vt_symbol: str, leg_volume: float):
        """"""
        leg = self.spread.legs[vt_symbol]
        leg_tick = self.get_tick(vt_symbol)
        leg_contract = self.get_contract(vt_symbol)

        if leg_volume > 0:
            price = leg_tick.ask_price_1 + leg_contract.pricetick * self.payup
            if leg_tick.limit_up:
                # 多头委托价格不能高于涨停价
                price = min(leg_tick.limit_up, price)
            self.send_long_order(leg.vt_symbol, price, abs(leg_volume))

        elif leg_volume < 0:
            price = leg_tick.bid_price_1 - leg_contract.pricetick * self.payup
            if leg_tick.limit_down:
                # 空头委托价格不能低于跌停价
                price = max(leg_tick.limit_down, price)
            self.send_short_order(leg.vt_symbol, price, abs(leg_volume))
