# encoding: UTF-8

from backtesting import BacktestingEngine
from statistical_arbitrage_backtesting_strategy import StatisticalArbitrageBacktestingStrategy
from vnpy.app.spread_trading.base import LegData, SpreadData
from datetime import datetime

""" modify by loe """
from vnpy.trader.constant import Direction, Offset
import csv

def one():
    name = 'CU'
    symbol_up, symbol_down, symbol_active, size, price_tick, commission_rate = get_backtesting_params(name=name)

    min_volume = 1
    slippage = 0
    capital = 200000

    strategy_params = {'boll_dev':5}

    start = datetime(2020, 3, 20)
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

def get_backtesting_params(name:str):
    symbol_up = ''
    symbol_down = ''
    symbol_active = ''
    size = 0
    price_tick = 0
    commission_rate = 0.000
    if name == 'CF':
        symbol_up = 'CF2009'
        symbol_down = 'CF2005'
        symbol_active = 'CF2009'
        size = 5
        price_tick = 5
        commission_rate = 0.000

    elif name == 'CJ':
        symbol_up = 'CJ2009'
        symbol_down = 'CJ2005'
        symbol_active = 'CJ2009'
        size = 5
        price_tick = 5
        commission_rate = 0.000

    elif name == 'CS':
        symbol_up = 'CS2009'
        symbol_down = 'CS2005'
        symbol_active = 'CS2009'
        size = 10
        price_tick = 1
        commission_rate = 0.000

    elif name == 'CU':
        symbol_up = 'CU2005'
        symbol_down = 'CU2004'
        symbol_active = 'CU2005'
        size = 5
        price_tick = 10
        commission_rate = 0.000

    elif name == 'CY':
        symbol_up = 'CY2009'
        symbol_down = 'CY2005'
        symbol_active = 'CY2009'
        size = 5
        price_tick = 5
        commission_rate = 0.000

    elif name == 'EG':
        symbol_up = 'EG2009'
        symbol_down = 'EG2005'
        symbol_active = 'EG2009'
        size = 10
        price_tick = 1
        commission_rate = 0.000

    elif name == 'OI':
        symbol_up = 'OI2005'
        symbol_down = 'OI2009'
        symbol_active = 'OI2009'
        size = 10
        price_tick = 1
        commission_rate = 0.000

    elif name == 'PB':
        symbol_up = 'PB2005'
        symbol_down = 'PB2006'
        symbol_active = 'PB2006'
        size = 5
        price_tick = 5
        commission_rate = 0.000

    elif name == 'RM':
        symbol_up = 'RM2009'
        symbol_down = 'RM2005'
        symbol_active = 'RM2009'
        size = 10
        price_tick = 1
        commission_rate = 0.000

    elif name == 'SC':
        symbol_up = 'SC2006'
        symbol_down = 'SC2005'
        symbol_active = 'SC2006'
        size = 1000
        price_tick = 0.1
        commission_rate = 0.000

    elif name == 'SF':
        symbol_up = 'SF2009'
        symbol_down = 'SF2005'
        symbol_active = 'SF2009'
        size = 5
        price_tick = 2
        commission_rate = 0.000

    elif name == 'SM':
        symbol_up = 'SM2009'
        symbol_down = 'SM2005'
        symbol_active = 'SM2009'
        size = 5
        price_tick = 2
        commission_rate = 0.000

    elif name == 'SN':
        symbol_up = 'SN2009'
        symbol_down = 'SN2005'
        symbol_active = 'SN2009'
        size = 1
        price_tick = 10
        commission_rate = 0.000

    elif name == 'SP':
        symbol_up = 'SP2009'
        symbol_down = 'SP2005'
        symbol_active = 'SP2009'
        size = 10
        price_tick = 2
        commission_rate = 0.000

    elif name == 'SR':
        symbol_up = 'SR2009'
        symbol_down = 'SR2005'
        symbol_active = 'SR2005'
        size = 10
        price_tick = 1
        commission_rate = 0.000

    elif name == 'TA':
        symbol_up = 'TA2009'
        symbol_down = 'TA2005'
        symbol_active = 'TA2009'
        size = 5
        price_tick = 2
        commission_rate = 0.000

    elif name == 'WR':
        symbol_up = 'WR2010'
        symbol_down = 'WR2005'
        symbol_active = 'WR2010'
        size = 10
        price_tick = 1
        commission_rate = 0.000

    elif name == 'ZC':
        symbol_up = 'ZC2009'
        symbol_down = 'ZC2005'
        symbol_active = 'ZC2009'
        size = 100
        price_tick = 0.2
        commission_rate = 0.000

    elif name == 'ZN':
        symbol_up = 'ZN2005'
        symbol_down = 'ZN2004'
        symbol_active = 'ZN2005'
        size = 5
        price_tick = 5
        commission_rate = 0.000

    elif name == 'TF':
        symbol_up = 'TF2009'
        symbol_down = 'TF2006'
        symbol_active = 'TF2009'
        size = 10000
        price_tick = 0.005
        commission_rate = 0.000

    elif name == 'TS':
        symbol_up = 'TS2009'
        symbol_down = 'TS2006'
        symbol_active = 'TS2009'
        size = 20000
        price_tick = 0.005
        commission_rate = 0.000

    elif name == 'FG':
        symbol_up = 'FG2009'
        symbol_down = 'FG2005'
        symbol_active = 'FG2009'
        size = 20
        price_tick = 1
        commission_rate = 0.000

    elif name == 'JM':
        symbol_up = 'JM2009'
        symbol_down = 'JM2005'
        symbol_active = 'JM2009'
        size = 60
        price_tick = 0.5
        commission_rate = 0.000

    elif name == 'RU':
        symbol_up = 'RU2009'
        symbol_down = 'RU2005'
        symbol_active = 'RU2009'
        size = 10
        price_tick = 5
        commission_rate = 0.000

    elif name == 'SA':
        symbol_up = 'SA2009'
        symbol_down = 'SA2005'
        symbol_active = 'SA2009'
        size = 20
        price_tick = 1
        commission_rate = 0.000

    elif name == 'NR':
        symbol_up = 'NR2006'
        symbol_down = 'NR2005'
        symbol_active = 'NR2006'
        size = 10
        price_tick = 5
        commission_rate = 0.000

    elif name == '':
        symbol_up = ''
        symbol_down = ''
        symbol_active = ''
        size = 0
        price_tick = 0
        commission_rate = 0.000

    return symbol_up, symbol_down, symbol_active, size, price_tick, commission_rate

if __name__ == '__main__':
    one()