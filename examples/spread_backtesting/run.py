# encoding: UTF-8

from vnpy.app.spread_trading.backtesting import BacktestingEngine
from statistical_arbitrage_backtesting_strategy import StatisticalArbitrageBacktestingStrategy
from vnpy.app.spread_trading.base import LegData, SpreadData
from datetime import datetime

def one():
    spread = SpreadData(
        name="RB",
        legs=[LegData("RB2001.SHFE"), LegData("RB2005.SHFE")],
        price_multipliers={"RB2001.SHFE": 1, "RB2005.SHFE": -1},
        trading_multipliers={"RB2001.SHFE": 1, "RB2005.SHFE": -1},
        active_symbol="RB2001.SHFE",
        inverse_contracts={"RB2001.SHFE": False, "RB2005.SHFE": False},
        min_volume=1
    )

    engine = BacktestingEngine()
    engine.set_parameters(
        spread=spread,
        interval="1m",
        start=datetime(2019, 1, 1),
        end=datetime(2019, 12, 31),
        rate=0.0001,
        slippage=0,
        size=20,
        pricetick=1,
        capital=200000,
    )
    engine.add_strategy(StatisticalArbitrageBacktestingStrategy, {})

    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()
    engine.show_chart()

    """
    for trade in engine.trades.values():
        print(trade)
    """

if __name__ == '__main__':
    one()