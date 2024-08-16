from dataservice import (
    TurtleCryptoDataDownloading,
    BinanceType,
    bybit_get_symbol_list,
    BybitSymbolType,
)
from dataservice.BinanceDataService import binance_get_symbol_list
from dataservice.OKXDataService import okx_get_symbol_list, OKXType
from vnpy.trader.constant import Interval
from datetime import datetime, timedelta
from time import sleep
from dataservice.utility import get_instruments_list

if __name__ == "__main__":
    """
    get_instruments_list()
    """

    # """
    exchange = input("选择交易所（默认1）【Binance：1 OKX：2 Bybit：3】")
    if exchange == "2":
        exchange = "OKX"
        mode = input("选择模式（默认1）【接口获取：1 正向：2 反向：3】")
        if mode == "2":
            contract_list = ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
            contract_list = ['AEVO-USDT-SWAP', 'BCH-USDT-SWAP', 'BIGTIME-USDT-SWAP', 'BNB-USDT-SWAP', 'BTC-USDT', 'BTC-USDT-SWAP', 'CEL-USDT-SWAP', 'CORE-USDT-SWAP', 'CRV-USDT-SWAP', 'CVC-USDT-SWAP', 'DOGE-USDT-SWAP', 'ENS-USDT-SWAP', 'ETC-USDT-SWAP', 'ETH-USDT-SWAP', 'ETHFI-USDT-SWAP', 'FIL-USDT-SWAP', 'FLM-USDT-SWAP', 'FLOKI-USDT-SWAP', 'ID-USDT-SWAP', 'JUP-USDT-SWAP', 'LTC-USDT-SWAP', 'MEME-USDT-SWAP', 'MEW-USDT-SWAP', 'MSN-USDT-SWAP', 'NOT-USDT-SWAP', 'ONT-USDT-SWAP', 'OP-USDT-SWAP', 'PEOPLE-USDT-SWAP', 'PEPE-USDT-SWAP', 'SHIB-USDT-SWAP', 'SLP-USDT-SWAP', 'SOL-USDT-SWAP', 'STRK-USDT-SWAP', 'SUI-USDT-SWAP', 'TON-USDT-SWAP', 'TRB-USDT-SWAP', 'TURBO-USDT-SWAP', 'UMA-USDT-SWAP', 'UNI-USDT-SWAP', 'W-USDT-SWAP', 'WLD-USDT-SWAP', 'YGG-USDT-SWAP', 'ZETA-USDT-SWAP', 'ZRO-USDT-SWAP', 'SATS-USDT-SWAP']
            
        elif mode == "3":
            contract_list = ["BTC-USD-SWAP", "ETH-USD-SWAP"]

        else:
            contract_list = okx_get_symbol_list(type=OKXType.USDT)

    elif exchange == "3":
        exchange = "BYBIT"
        mode = input("选择模式（默认1）【接口获取：1 正向：2 反向：3】")
        if mode == "2":
            contract_list = ["BTCUSDT", "ETHUSDT"]

        elif mode == "3":
            contract_list = ["BTCUSD", "ETHUSD"]

        else:
            contract_list = bybit_get_symbol_list(type=BybitSymbolType.USDT)

    else:
        exchange = "BINANCE"
        mode = input("选择模式（默认1）【接口获取：1 正向：2 反向：3】")
        if mode == "2":
            contract_list = ["BTCUSDT", "1000PEPEUSDT"]

        elif mode == "3":
            contract_list = ["BTCUSD", "ETHUSD"]

        else:
            contract_list = binance_get_symbol_list()

    print("\n")
    for symbol in contract_list:
        print(symbol)
    print(f"\n交易所：{exchange}\n合约总数：{len(contract_list)}")
    sleep(2)

    # fake
    # start_ = 60
    # end_ = 80
    # print(f"\n本次下载起止合约：{contract_list[start_]} -> {contract_list[end_-1]}")
    # contract_list = contract_list[start_:end_]
    # print(contract_list)
    # print(f"总计：{len(contract_list)}\n")
    # sleep(2)

    # 起止日期
    # days = 200
    # to_date = datetime.now() + timedelta(days=2)
    days = (datetime.now() - datetime.strptime("2024-01-01", "%Y-%m-%d")).days
    to_date = datetime.strptime("2024-12-31", "%Y-%m-%d")

    # 是否从数据库最新数据日期开始
    from_data_base = True

    # 开始下载
    dataDownload = TurtleCryptoDataDownloading()
    if exchange == "BINANCE":
        dataDownload.download_from_binance(
            contract_list=contract_list,
            days=days,
            to_date=to_date,
            from_data_base=from_data_base,
            api_check=True,
        )

    elif exchange == "OKX":
        dataDownload.download_from_okx(
            contract_list=contract_list,
            days=days,
            to_date=to_date,
            from_data_base=from_data_base,
            api_check=False,
        )

    elif exchange == "BYBIT":
        dataDownload.download_from_bybit(
            contract_list=contract_list,
            days=days,
            to_date=to_date,
            from_data_base=from_data_base,
            api_check=True,
        )
        # result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_bybit(contract_list=contract_list, days=days)
        # print('\n\n' + lost_msg + back_msg)

    # """

    """ OKX """
    """
    contract_list = ['BTC-USD-21']
    days = 1200
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_okx(contract_list=contract_list, days=days)
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
    dataDownload.download_from_binance(contract_list=contract_list, days=days, type=BinanceType.USDT)
    """

    """ 【分钟】K合成【8H】K"""
    """
    contract_list = ['ETHUSDT']
    days = 100
    dataDownload = TurtleCryptoDataDownloading()
    result, complete_msg, back_msg, lost_msg = dataDownload.generate_8h_for_bybit(contract_list=contract_list, days=days)
    print('\n\n' + lost_msg + back_msg)
    """
