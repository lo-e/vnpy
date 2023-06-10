from dataservice import TurtleCryptoDataDownloading, Binancetype, bybit_get_symbol_list, BybitSymbolType
from dataservice.BinanceDataService import binance_get_symbol_list
from vnpy.trader.constant import Interval
from datetime import datetime, timedelta

if __name__ == '__main__':
    """ 1TOKEN"""
    """
    contract_list = ['okef/btc.usd.q', 'okef/eth.usd.q', 'okef/eos.usd.q', 'okswap/btc.usd.td', 'okswap/eth.usd.td',
                    'okswap/eos.usd.td', 'okex/btc.usdt', 'okex/eth.usdt', 'okex/eos.usdt']
    contract_list = ['okef/btc.usd.t', 'okef/btc.usd.n']
    days = 20
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_onetoken(contract_list=contract_list, days=days)
    result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_onetoken(contract_list=contract_list, days=days)
    print('\n\n' + lost_msg + back_msg)
    """

    """ BYBIT """
    #"""
    exchange = input('选择交易所【Bybit：1  Binance：2】')
    if exchange == "1":
        mode = input('选择模式【反向：1  正向：2 接口获取：3】')
        if mode == '1':
            contract_list = ['BTCUSD', 'ETHUSD']

        elif mode == '2':
            contract_list = ['BTCUSDT', 'ETHUSDT']
        
        else:
            contract_list = bybit_get_symbol_list(type=BybitSymbolType.USDT)
            for symbol in contract_list:
                print(symbol)
            print(f"即将下载总计：{len(contract_list)}")
    
    elif exchange == "2":
        mode = input('选择模式【反向：1  正向：2 接口获取：3】')
        if mode == '1':
            contract_list = ['BTCUSD', 'ETHUSD']

        elif mode == '2':
            contract_list = ['BTCUSDT', 'ETHUSDT']
           
        else:
            contract_list = binance_get_symbol_list()
            for symbol in contract_list:
                print(symbol)
            print(f"即将下载总计：{len(contract_list)}")
    
    else:
        exit(f"交易所选择错误")

    # 起止日期
    # days = 6
    # to_date = datetime.now() + timedelta(days=2)
    days = (datetime.now() - datetime.strptime('2023-05-01', '%Y-%m-%d')).days
    to_date = datetime.strptime('2023-12-31', '%Y-%m-%d')

    # 是否从数据库最新数据日期开始
    from_data_base = True

    # 开始下载
    dataDownload = TurtleCryptoDataDownloading()
    if exchange == "1":
        dataDownload.download_from_bybit(contract_list=contract_list, days=days, to_date=to_date, from_data_base=from_data_base)
        # result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_bybit(contract_list=contract_list, days=days)
        # print('\n\n' + lost_msg + back_msg)

    elif exchange == "2":
        dataDownload.download_from_binance(contract_list=contract_list, days=days, to_date=to_date, from_data_base=from_data_base)

    
    #"""

    """ OKEX """
    """
    contract_list = ['BTC-USD-21']
    days = 1200
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_okex(contract_list=contract_list, days=days)
    """

    """ FTX """
    """
    contract_list = ['BTC-PERP']
    interval = Interval.MINUTE
    days = 1200
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_ftx(contract_list=contract_list, interval=interval, days=days)
    """

    """ BINANCE """
    """
    contract_list = ['BTCUSDT']
    days = 3000
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_binance(contract_list=contract_list, days=days, type=Binancetype.USDT)
    """

    """ 【分钟】K合成【8H】K"""
    """
    contract_list = ['ETHUSDT']
    days = 100
    dataDownload = TurtleCryptoDataDownloading()
    result, complete_msg, back_msg, lost_msg = dataDownload.generate_8h_for_bybit(contract_list=contract_list, days=days)
    print('\n\n' + lost_msg + back_msg)
    """