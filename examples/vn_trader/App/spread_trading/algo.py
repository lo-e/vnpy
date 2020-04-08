from typing import Any

from vnpy.trader.constant import Direction, Offset
from vnpy.trader.object import (TickData, OrderData, TradeData)
from vnpy.trader.utility import round_to

from vnpy.app.spread_trading.template import SpreadAlgoTemplate, check_tick_valid
from vnpy.app.spread_trading.base import SpreadData

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

        self.tick_processing = False

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

        # Otherwise check if should take active leg
        if self.direction == Direction.LONG:
            if self.spread.ask_price <= self.price:
                self.take_active_passive_leg()

        elif self.direction == Direction.SHORT:
            if self.spread.bid_price >= self.price:
                self.take_active_passive_leg()

        self.tick_processing = False

    def on_order(self, order: OrderData):
        """"""
        pass
        """
        # Only care active leg order update
        if order.vt_symbol != self.spread.active_leg.vt_symbol:
            return

        # Do nothing if still any existing orders
        if not self.check_order_finished():
            return

        # Hedge passive legs if necessary
        if not self.check_hedge_finished():
            self.hedge_passive_legs()
            self.hedge_active_legs()
        """

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
    # 主动退被动腿同时委托，只适用于单条被动腿的策略
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

        # Send active leg order
        self.send_leg_order(
            self.spread.active_leg.vt_symbol,
            active_leg_order_volume
        )

        self.send_leg_order(
            passive_leg.vt_symbol,
            passive_leg_order_volume
        )

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
            self.send_long_order(leg.vt_symbol, price, abs(leg_volume))
        elif leg_volume < 0:
            price = leg_tick.bid_price_1 - leg_contract.pricetick * self.payup
            self.send_short_order(leg.vt_symbol, price, abs(leg_volume))
