from dataservice import TurtleCryptoDataDownloading, Binancetype
from vnpy.trader.constant import Interval
from datetime import datetime

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
    mode = input('选择模式【反向：1  正向：2】')
    if mode == '1':
        contract_list = ['BTCUSD', 'ETHUSD']
    else:
        contract_list = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    days = 6
    # days = (datetime.now() - datetime.strptime('2021-07-20', '%Y-%m-%d')).days
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_bybit(contract_list=contract_list, days=days)
    result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_bybit(contract_list=contract_list, days=days)
    print('\n\n' + lost_msg + back_msg)
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