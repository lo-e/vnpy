from dataservice import TurtleCryptoDataDownloading, Binancetype, bybit_get_symbol_list, BybitSymbolType
from dataservice.BinanceDataService import binance_get_symbol_list
from vnpy.trader.constant import Interval
from datetime import datetime, timedelta
from time import sleep

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
        exchange = "BYBIT"
        mode = input('选择模式【反向：1  正向：2 接口获取：3】')
        if mode == '1':
            contract_list = ['BTCUSD', 'ETHUSD']

        elif mode == '2':
            contract_list = ['BTCUSDT', 'ETHUSDT']
        
        else:
            contract_list = bybit_get_symbol_list(type=BybitSymbolType.USDT)
    
    elif exchange == "2":
        exchange = "BINANCE"
        mode = input('选择模式【反向：1  正向：2 接口获取：3】')
        if mode == '1':
            contract_list = ['BTCUSD', 'ETHUSD']

        elif mode == '2':
            contract_list = ['BTCUSDT', 'ETHUSDT']
            contract_list = ['BCHUSDT', 'ADAUSDT', 'ATOMUSDT', 'BATUSDT', 'ALGOUSDT', 'BANDUSDT', 'BALUSDT', 'AVAXUSDT', 'AAVEUSDT', 'BELUSDT', 'AXSUSDT', 'ALPHAUSDT', '1INCHUSDT', 'ANKRUSDT', 'ALICEUSDT', '1000SHIBUSDT', 'BAKEUSDT', 'AUDIOUSDT', 'ATAUSDT', '1000XECUSDT', 'ARUSDT', 'ARPAUSDT', 'OCEANUSDT', 'MTLUSDT', 'NEARUSDT', 'NEOUSDT', 'GALAUSDT', 'NKNUSDT', 'EGLDUSDT', 'ENJUSDT', 'GRTUSDT', 'ENSUSDT', 'EOSUSDT', 'ETCUSDT', 'ETHUSDT', 'OMGUSDT', 'FILUSDT', 'FLMUSDT', 'OGNUSDT', 'FTMUSDT', 'GTCUSDT', 'CTKUSDT', 'MKRUSDT', 'KLAYUSDT', 'KAVAUSDT', 'LINAUSDT', 'LINKUSDT', 'IOTXUSDT', 'IOTAUSDT', 'IOSTUSDT', 'LITUSDT', 'LPTUSDT', 'LRCUSDT', 'LTCUSDT', 'ONEUSDT', 'ICXUSDT', 'MANAUSDT', 'HOTUSDT', 'MASKUSDT', 'MATICUSDT', 'HBARUSDT', 'DYDXUSDT', 'KSMUSDT', 'DOTUSDT', 'CELOUSDT', 'ONTUSDT', 'BTCUSDT', 'BTCDOMUSDT', 'RSRUSDT', 'BNBUSDT', 'CELRUSDT', 'BLZUSDT', 'RVNUSDT', 'SANDUSDT', 'SFPUSDT', 'SKLUSDT', 'SNXUSDT', 'SOLUSDT', 'RUNEUSDT', 'RLCUSDT', 'C98USDT', 'CHZUSDT', 'DOGEUSDT', 'DGBUSDT', 'DENTUSDT', 'CHRUSDT', 'DASHUSDT', 'QTUMUSDT', 'DEFIUSDT', 'KNCUSDT', 'CRVUSDT', 'COTIUSDT', 'COMPUSDT', 'REEFUSDT', 'RENUSDT', 'CTSIUSDT', 'UNIUSDT', 'STMXUSDT', 'STORJUSDT', 'UNFIUSDT', 'SUSHIUSDT', 'SXPUSDT', 'VETUSDT', 'THETAUSDT', 'ZRXUSDT', 'TRXUSDT', 'WAVESUSDT', 'XTZUSDT', 'YFIUSDT', 'XMRUSDT', 'XRPUSDT', 'ZILUSDT', 'XEMUSDT', 'XLMUSDT', 'ZENUSDT', 'ZECUSDT', 'PEOPLEUSDT', 'ANTUSDT', 'ROSEUSDT']

        else:
            contract_list = binance_get_symbol_list()
    
    else:
        exit(f"交易所选择错误")

    print("\n")
    for symbol in contract_list:
        print(symbol)
    print(f"\n交易所：{exchange}\n合约总数：{len(contract_list)}")
    sleep(2)

    # 起止日期
    days = 6
    to_date = datetime.now() + timedelta(days=2)
    days = (datetime.now() - datetime.strptime('2020-12-01', '%Y-%m-%d')).days
    to_date = datetime.strptime('2022-01-01', '%Y-%m-%d')

    # 是否从数据库最新数据日期开始
    from_data_base = True

    # 开始下载
    dataDownload = TurtleCryptoDataDownloading()
    if exchange == "BYBIT":
        dataDownload.download_from_bybit(contract_list=contract_list, days=days, to_date=to_date, from_data_base=from_data_base, api_check=True)
        # result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_bybit(contract_list=contract_list, days=days)
        # print('\n\n' + lost_msg + back_msg)

    elif exchange == "BINANCE":
        dataDownload.download_from_binance(contract_list=contract_list, days=days, to_date=to_date, from_data_base=from_data_base, api_check=True)

    
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