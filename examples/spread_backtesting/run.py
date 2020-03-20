# encoding: UTF-8

from backtesting import BacktestingEngine
from statistical_arbitrage_backtesting_strategy import StatisticalArbitrageBacktestingStrategy
from vnpy.app.spread_trading.base import LegData, SpreadData
from datetime import datetime

""" modify by loe """
from vnpy.trader.constant import Direction, Offset
import csv

def one():
    name = 'CF'
    symbol_up = 'CF2009'
    symbol_down = 'CF2005'
    symbol_active = 'CF2009'
    min_volume = 1

    size = 5
    price_tick = 5
    commission_rate = 0.000
    slippage = 0
    capital = 200000

    strategy_params = {'boll_dev':4}

    start = datetime(2019, 12, 16)
    end = datetime(2020, 12, 31)

    spread = SpreadData(
        name=name,
        legs=[LegData(symbol_up), LegData(symbol_down)],
        price_multipliers={symbol_up: 1, symbol_down: -1},
        trading_multipliers={symbol_up: 1, symbol_down: -1},
        active_symbol=symbol_active,
        inverse_contracts={symbol_up: False, symbol_down: False},
        min_volume=min_volume
    )

    engine = BacktestingEngine()
    engine.set_parameters(
        spread=spread,
        interval="1m",
        start=start,
        end=end,
        rate=commission_rate,
        slippage=slippage,
        size=size,
        pricetick=price_tick,
        capital=capital,
    )
    engine.add_strategy(StatisticalArbitrageBacktestingStrategy, strategy_params)

    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()
    engine.show_chart()

    save_trade_detail = True
    if save_trade_detail:
        resultList = []
        totalPnl = 0
        calculateDic = {}

        for trade in engine.trades.values():
            print('%s\t\t%s %s\t\t%s\t\t%s\t%s@%s' % (
            trade.datetime, trade.symbol, trade.direction.value, trade.offset.value,
            engine.size, trade.volume, trade.price))

            tOpen = False
            pnl = 0
            offset = ''
            direction = 0

            symbolDic = calculateDic.get(trade.symbol, {})

            if trade.offset == Offset.OPEN:
                offset = '开仓'
                tOpen = True
            elif trade.offset == Offset.CLOSE:
                offset = '平仓'
                tOpen = False

            if trade.direction == Direction.LONG:
                direction = '多'
                if tOpen:
                    symbolDic['direction'] = 1
            elif trade.direction == Direction.SHORT:
                direction = '空'
                if tOpen:
                    symbolDic['direction'] = -1

            if trade.volume:
                if tOpen:
                    symbolDic['size'] = engine.size
                    vol = symbolDic.get('volume', 0)
                    pri = symbolDic.get('price', 0)
                    pri = vol * pri + trade.volume * trade.price

                    vol += trade.volume
                    symbolDic['volume'] = vol
                    pri = pri / vol
                    symbolDic['price'] = pri
                    calculateDic[trade.symbol] = symbolDic
                else:
                    if symbolDic['volume'] != trade.volume:
                        raise ('平仓数量有误！')
                    pnl = symbolDic['direction'] * (trade.price - symbolDic['price']) * trade.volume * symbolDic[
                        'size']
                    totalPnl += pnl
                    calculateDic[trade.symbol] = {}

            dic = {'datetime': trade.datetime,
                   'symbol': trade.symbol,
                   'direction': direction,
                   'offset': offset,
                   'size': engine.size,
                   'volume': trade.volume,
                   'price': trade.price}
            if pnl:
                dic['pnl'] = str(pnl)
                dic['totalPnl'] = str(totalPnl)
            else:
                dic['pnl'] = ''
                dic['totalPnl'] = ''

            resultList.append(dic)
        print('\n\n')
        if len(resultList):
            fieldNames = ['datetime', 'symbol', 'direction', 'offset', 'size', 'volume', 'price', 'pnl', 'totalPnl']
            # 文件路径
            filePath = 'result.csv'
            with open(filePath, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=fieldNames)
                writer.writeheader()
                # 写入csv文件
                writer.writerows(resultList)

if __name__ == '__main__':
    one()