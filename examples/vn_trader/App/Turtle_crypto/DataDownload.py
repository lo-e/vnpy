from dataservice import (
    TurtleCryptoDataDownloading,
    BinanceType,
    bybit_get_symbol_list,
    BybitSymbolType,
)
from dataservice.BinanceDataService import binance_get_symbol_list, binance_get_first_bar_datetime
from dataservice.OKXDataService import okx_get_symbol_list, OKXType, okx_get_first_bar_datetime
from vnpy.trader.constant import Interval
from datetime import datetime, timedelta
from time import sleep
from threading import Thread
from pymongo import MongoClient
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
from vnpy.trader.constant import Exchange
from vnpy.trader.object import BarData

class DownloadUtility(object):
    def __init__(self) -> None:
        self.update_data_hour = -1              # 上次更新数据的时间
    
    def get_instruments_list(self):
        # 获取OKX合约列表
        symbol_list_ok = okx_get_symbol_list(type=OKXType.USDT)
        pure_symbol_list_ok = set()
        for symbol in symbol_list_ok:
            pure_symbol = symbol.split("-")[0]
            pure_symbol_list_ok.add(pure_symbol)

        # 获取BINANCE合约列表
        symbol_list_binance = binance_get_symbol_list(need_data=False)
        pure_symbol_list_binance = set()
        for symbol in symbol_list_binance:
            pure_symbol = symbol.split("USDT")[0]
            pure_symbol_list_binance.add(pure_symbol)

        # OKX和BINANCE共同合约
        pure_symbol_list_common = pure_symbol_list_ok.intersection(pure_symbol_list_binance)

        # OKX独占合约
        pure_symbol_list_okx_only = pure_symbol_list_ok - pure_symbol_list_common

        # BINANCE独占合约
        pure_symbol_list_binance_only = pure_symbol_list_binance - pure_symbol_list_common

        for symbol in pure_symbol_list_common:
            print(symbol)
        print(f"OKX和BINANCE共同合约总计：{len(pure_symbol_list_common)}")
        print(f"\n" + f"-"*20 + "\n")

        for symbol in pure_symbol_list_okx_only:
            print(symbol)
        print(f"OKX独占合约总计：{len(pure_symbol_list_okx_only)} 所有合约总计：{len(pure_symbol_list_ok)}")
        print(f"\n" + f"-"*20 + "\n")

        for symbol in pure_symbol_list_binance_only:
            print(symbol)
        print(f"BINANCE独占合约总计：{len(pure_symbol_list_binance_only)} 所有合约总计：{len(pure_symbol_list_binance)}")

        # 筛选新币种
        print(f"\n------ OKX符合筛选条件的合约 ------")
        flt_symbol_dt_dict_ok = {}
        for symbol in symbol_list_ok:
            first_bar_dt = None
            trying = True
            while trying:
                try:
                    first_bar_dt = okx_get_first_bar_datetime(symbol=symbol)
                    trying = False

                except Exception:
                    trying = True

            if first_bar_dt:
                flt_from = datetime.now().replace(second=0) - timedelta(days=90)
                if first_bar_dt >= flt_from:
                    pure_symbol = symbol.split("-")[0]
                    flt_symbol_dt_dict_ok[pure_symbol] = first_bar_dt
                    print(f"{pure_symbol}\t\t{first_bar_dt}")
            
            else:
                print(f"无法获取合约上市日期：{symbol}")

        print(f"\n------ BINANCE符合筛选条件的合约 ------")
        flt_symbol_dt_dict_binance = {}
        for symbol in symbol_list_binance:
            first_bar_dt = None
            trying = True
            while trying:
                try:
                    first_bar_dt = binance_get_first_bar_datetime(symbol=symbol, interval="1m", symbol_type=BinanceType.USDT, start_time="2020-12-01 00:00:00")
                    trying = False

                except Exception:
                    trying = True

            if first_bar_dt:
                flt_from = datetime.now().replace(second=0) - timedelta(days=90)
                if first_bar_dt >= flt_from:
                    pure_symbol = symbol.split("USDT")[0]
                    flt_symbol_dt_dict_binance[pure_symbol] = first_bar_dt
                    print(f"{pure_symbol}\t\t{first_bar_dt}")
            
            else:
                print(f"无法获取合约上市日期：{symbol}")
        print(f"\n" + f"-"*20 + "\n")

        # OKX和BINANCE筛选合约
        flt_symbol_list_ok = set(flt_symbol_dt_dict_ok.keys())
        flt_symbol_list_binance = set(flt_symbol_dt_dict_binance.keys())

        # OKX和BINANCE共同筛选合约
        flt_symbol_list_common = flt_symbol_list_ok.intersection(flt_symbol_list_binance)

        # OKX独占帅选合约
        flt_symbol_list_okx_only = flt_symbol_list_ok - flt_symbol_list_common

        # BINANCE独占合约
        flt_symbol_list_binance_only = flt_symbol_list_binance - flt_symbol_list_common

        for symbol in flt_symbol_list_okx_only:
            print(f"{symbol}\t{flt_symbol_dt_dict_ok[symbol]}")
        print(f"OKX独占筛选合约总计：{len(flt_symbol_list_okx_only)} 所有合约总计：{len(flt_symbol_dt_dict_ok)}")
        print(f"\n" + f"-"*20 + "\n")

        for symbol in flt_symbol_list_binance_only:
            print(f"{symbol}\t{flt_symbol_dt_dict_binance[symbol]}")
        print(f"BINANCE独占筛选合约总计：{len(flt_symbol_list_binance_only)} 所有合约总计：{len(flt_symbol_dt_dict_binance)}")
        print(f"\n" + f"-"*20 + "\n")

        for symbol in flt_symbol_list_common:
            print(f"{symbol}\t{flt_symbol_dt_dict_ok[symbol]}(OKX)\t{flt_symbol_dt_dict_binance[symbol]}(BINANCE)")
        print(f"OKX和BINANCE共同筛选合约总计：{len(flt_symbol_list_common)}")

    def download_data(self):
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

    def update_data_signal(self):
        # 下载引擎
        dataDownload = TurtleCryptoDataDownloading()
        mc = MongoClient()
        db = mc[MINUTE_DB_NAME]

        while True:
            current_minute = datetime.now().minute
            current_hour = datetime.now().hour
            if current_hour != self.update_data_hour and current_minute >= 1 and not len(dataDownload.threads):
                self.update_data_hour = current_hour

                # OKX合约列表
                okx_contract_list = okx_get_symbol_list(type=OKXType.USDT)
                print(f"OKX合约总数：{len(okx_contract_list)}")
                
                # BINANCE合约列表
                binance_contract_list = binance_get_symbol_list()
                print(f"BINANCE合约总数：{len(binance_contract_list)}")

                # 下载起止日期
                days = 5
                to_date = datetime.now() + timedelta(days=2)

                # 是否从数据库最新数据日期开始
                from_data_base = True

                # 先清空历史下载数据 
                dataDownload.delete_history_data()

                # 开始下载
                thread = Thread(target=dataDownload.download_from_okx, args=(okx_contract_list, days, to_date, from_data_base, False, "", False))
                thread.start()

                thread = Thread(target=dataDownload.download_from_binance, args=(binance_contract_list, days, to_date, from_data_base, False, "", False))
                thread.start()

                # 检查数据更新完成
                complete_check_time = 0
                while True:
                    sleep(1)
                    if not len(dataDownload.threads):
                        complete_check_time += 1
                        if complete_check_time >= 5:
                            print(f"更新数据完成：{datetime.now()}")
                            break
                
                # 每隔4h分析市场行情
                current_hour = datetime.now().hour
                if not current_hour % 4:
                    for okx_contract in okx_contract_list:
                        vt_symbol = f"{okx_contract}.OKX"
                        start_dt = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)
                        end_dt = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(minutes=1)
                        flt = {'datetime':{'$gte':self.startDt,
                                           '$lte':self.endDt}}
                        collection = db[vt_symbol]
                        cursor = collection.find(flt).sort('datetime')
                        open_price = 0
                        close_price = 0
                        for d in cursor:
                            exchange = Exchange.NONE
                            bar = BarData(gateway_name = '', symbol = '', exchange = exchange, datetime = None, endDatetime = None)
                            bar.__dict__ = d
                            if not open_price:
                                open_price = bar.open_price
                            close_price = bar.close_price

            sleep(10)
            
if __name__ == "__main__":
    utility = DownloadUtility()

    # 获取OKX、BINANCE合约列表信息
    # utility.get_instruments_list()

    # 下载数据
    # utility.download_data()
    
    # 更新数据并生成市场信号
    utility.update_data_signal()
