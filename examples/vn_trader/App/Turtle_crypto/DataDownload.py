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
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
from vnpy.trader.constant import Exchange
from vnpy.trader.object import BarData
import pandas as pd
from vnpy.trader.utility import DIR_SYMBOL
import os
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph
from reportlab.platypus import Spacer
from vnpy.trader.engine import EmailEngine
from dataservice.utility import get_csv_path, save_df_data
from copy import copy
import json

class DownloadUtility(object):
    def __init__(self) -> None:
        self.update_data_hour = -1              # 上次更新数据的时间
    
    # 获取各大交易所所有合约列表
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

    # 获取新上市的合约列表
    def get_new_instruments_list(self, exchange: Exchange):
        # .csv导入历史合约列表数据
        csv_dir = get_csv_path()
        symbol_instruments_data = {}
        file_path = f"{csv_dir}{exchange.value}{DIR_SYMBOL}instruments.csv"
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            for _, row in df.iterrows():
                instrument = dict(row)
                symbol = instrument["symbol"]
                symbol_instruments_data[symbol] = instrument

        dataDownload = TurtleCryptoDataDownloading()
        dataDownload.download_instruments_list(exchange, symbol_instruments_data)

    def download_instruments_bar_data(self, exchanges: list):
        download_bar_time = None
        downloading = False
        while True:
            try:
                # 请求下载判断
                download_setting = self.get_download_setting()
                request = download_setting.get("request", False)

                # 间隔下载判断
                current_hour_time = datetime.now().replace(minute=0, second=0, microsecond=0)
                download_bar_hour_time = download_bar_time.replace(minute=0, second=0, microsecond=0) if download_bar_time else None
                if (request or download_bar_hour_time != current_hour_time) and not downloading:
                    download_bar_time = datetime.now()

                    # 开始下载
                    downloading = True
                    try:
                        # 更新交易所合约
                        for exchange in exchanges:
                            utility.get_new_instruments_list(exchange)

                        # 文件获取交易所合约列表
                        exchange_instruments_data = {}
                        csv_dir = get_csv_path()
                        for exchange in exchanges:
                            symbol_instruments_data = {}
                            file_path = f"{csv_dir}{exchange.value}{DIR_SYMBOL}instruments.csv"
                            if not os.path.exists(file_path):
                                continue

                            df = pd.read_csv(file_path)
                            for _, row in df.iterrows():
                                instrument = dict(row)
                                symbol = instrument["symbol"]
                                symbol_instruments_data[symbol] = instrument
                            exchange_instruments_data[exchange.value] = symbol_instruments_data

                        # 添加合约
                        vt_symbols = set()
                        coins = set()

                        okx_symbols = list(exchange_instruments_data.get("OKX", {}).keys())
                        for symbol in okx_symbols:
                            coin = symbol.split("-USDT")[0]
                            if coin not in coins:
                                coins.add(coin)
                                vt_symbols.add(f"{symbol}.OKX")

                        bybit_symbols = list(exchange_instruments_data.get("BYBIT", {}).keys())
                        for symbol in bybit_symbols:
                            coin = symbol.split("USDT")[0]
                            if coin not in coins:
                                coins.add(coin)
                                vt_symbols.add(f"{symbol}.BYBIT")

                        binance_symbols = list(exchange_instruments_data.get("BINANCE", {}).keys())
                        for symbol in binance_symbols:
                            coin = symbol.split("USDT")[0]
                            if coin not in coins:
                                coins.add(coin)
                                vt_symbols.add(f"{symbol}.BINANCE")
                        
                        # 下载Bar数据
                        download_engine = TurtleCryptoDataDownloading()
                        dir_name = "TEMP"
                        print_(f"Bar数据下载中..")
                        result_bar_list = []
                        try_count = 0
                        while try_count < 5:
                            try_count += 1
                            try:
                                # 按交易所分类合约
                                contract_exchange_dict = {}
                                for symbol in vt_symbols.copy():
                                    exchange = symbol.split(".")[-1]
                                    exchange_symbols = contract_exchange_dict.get(exchange, set())
                                    exchange_symbols.add(symbol.split(".")[0])
                                    contract_exchange_dict[exchange] = exchange_symbols

                                # 先清空历史下载数据 
                                download_engine.delete_history_data(target_dir=dir_name)

                                # 开始下载
                                for exchange, exchange_symbols in contract_exchange_dict.items():
                                    if exchange == "BINANCE":
                                        download_engine.download_from_binance(
                                            contract_list=exchange_symbols, days=1, from_data_base=True, save_to=dir_name, delete_history_data=False, show_progress=False
                                        )

                                    elif exchange == "OKX":
                                        download_engine.download_from_okx(
                                            contract_list=exchange_symbols, days=1, from_data_base=True, save_to=dir_name, delete_history_data=False, show_progress=False
                                        )
                                    
                                    elif exchange == "BYBIT":
                                        download_engine.download_from_bybit(
                                            contract_list=exchange_symbols, days=1, from_data_base=True, save_to=dir_name, delete_history_data=False, show_progress=False
                                        )

                                # 获取下载结果
                                for symbol in vt_symbols.copy():
                                    client = MongoClient("localhost", 27017)
                                    db = client[MINUTE_DB_NAME]
                                    collection = db[symbol]

                                    now = datetime.now().replace(second=0, microsecond=0)
                                    dt_from = now - timedelta(hours=1)
                                    flt = {"datetime": {"$gte": dt_from}}
                                    bar_list = list(collection.find(flt).sort("datetime", DESCENDING))
                                    if bar_list:
                                        data = bar_list[0]
                                        bar = BarData(
                                            gateway_name="",
                                            symbol="",
                                            exchange=Exchange.NONE,
                                            datetime=None,
                                            endDatetime=None)
                                        bar.__dict__ = data
                                        result_bar_list.append(copy(bar))

                                break

                            except Exception as e:
                                msg = f"下载Bar数据出错\n\n{e}"
                                print_(msg)

                        # 输出结果
                        result_bar_list.sort(key=lambda bar: bar.datetime)
                        for bar in result_bar_list[:5]:
                            print(f"{bar.datetime}\t{bar.vt_symbol}\t{bar.open_price}\t{bar.high_price}\t{bar.low_price}\t{bar.close_price}")
                        print("------")
                        for bar in result_bar_list[-5:]:
                            print(f"{bar.datetime}\t{bar.vt_symbol}\t{bar.open_price}\t{bar.high_price}\t{bar.low_price}\t{bar.close_price}")

                        msg = f"Bar数据已更新！（{len(result_bar_list)}）\n"
                        print_(msg)

                        # 更新下载历史
                        try_count = 0
                        while try_count < 3:
                            try:
                                download_setting = self.get_download_setting()
                                download_setting["download_at"] = datetime.now().strftime(f"%Y-%m-%d %H:%M:%S")
                                download_setting["request"] = False
                                self.save_download_setting(download_setting)
                                break
                            
                            except Exchange as e:
                                pass
                            
                            try_count += 1

                    except Exception as e:
                        pass
                    
                    # 结束下载
                    downloading = False

            except Exception as e:
                msg = f"循环下载Bar数据出错\n\n{e}"
                print_(msg)

            sleep(60)

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
                contract_list = ["MYXUSDT"]

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
        # days = 3
        # to_date = datetime.now() + timedelta(days=2)
        days = (datetime.now() - datetime.strptime("2025-08-05", "%Y-%m-%d")).days
        to_date = datetime.strptime("2025-12-31", "%Y-%m-%d")

        # 是否从数据库最新数据日期开始
        from_data_base = True

        # 保存位置
        save_to = "DOWNLOAD"

        # 开始下载
        dataDownload = TurtleCryptoDataDownloading()
        if exchange == "BINANCE":
            dataDownload.download_from_binance(
                contract_list=contract_list,
                days=days,
                to_date=to_date,
                from_data_base=from_data_base,
                api_check=True,
                save_to=save_to
            )

        elif exchange == "OKX":
            dataDownload.download_from_okx(
                contract_list=contract_list,
                days=days,
                to_date=to_date,
                from_data_base=from_data_base,
                api_check=False,
                save_to=save_to
            )

        elif exchange == "BYBIT":
            dataDownload.download_from_bybit(
                contract_list=contract_list,
                days=days,
                to_date=to_date,
                from_data_base=from_data_base,
                api_check=True,
                save_to=save_to
            )
            # result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_bybit(contract_list=contract_list, days=days)
            # print('\n\n' + lost_msg + back_msg)

    def update_data_signal(self):
        # 下载引擎
        dataDownload = TurtleCryptoDataDownloading()
        email_engine = EmailEngine(main_engine=None, event_engine=None)
        mc = MongoClient()
        db = mc[MINUTE_DB_NAME]

        while True:
            current_minute = datetime.now().minute
            current_hour = datetime.now().hour
            if current_hour != self.update_data_hour and current_minute >= 1 and dataDownload.loading_complete:
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
                    if dataDownload.loading_complete:
                        complete_check_time += 1
                        if complete_check_time >= 5:
                            print(f"更新数据完成：{datetime.now()}")
                            break
                
                # 分析市场行情
                for exchange in ["OKX", "BINANCE"]:
                    contract_rate_dict = {}
                    contract_list = []
                    continuous_rise_list = set()
                    continuous_fall_list = set()

                    if exchange == "OKX":
                        contract_list = okx_contract_list

                    elif exchange == "BINANCE":
                        contract_list = binance_contract_list

                    for contract in contract_list:
                        if exchange == "OKX":
                            vt_symbol = f"{contract}.OKX"

                        elif exchange == "BINANCE":
                            vt_symbol = f"{contract}.BINANCE"
                        
                        start_dt = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)
                        end_dt = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(minutes=1)
                        flt = {'datetime':{'$gte':start_dt,
                                        '$lte':end_dt}}
                        collection = db[vt_symbol]
                        cursor = collection.find(flt).sort('datetime')
                        open_price = 0
                        close_price = 0
                        continuous_rise = True
                        continuous_fall = True
                        hour_open = 0
                        for d in cursor:
                            bar_exchange = Exchange.NONE
                            bar = BarData(gateway_name = '', symbol = '', exchange = bar_exchange, datetime = None, endDatetime = None)
                            bar.__dict__ = d

                            # 记录open_close_price
                            if not open_price:
                                open_price = bar.open_price
                            close_price = bar.close_price
                            
                            # 记录每小时的连续涨跌
                            if bar.datetime.minute == 0:
                                hour_open = bar.open_price

                            if bar.datetime.minute == 59:
                                if hour_open:
                                    if bar.close_price > hour_open:
                                        continuous_fall = False
                                    
                                    elif bar.close_price < hour_open:
                                        continuous_rise = False

                                    else:
                                        continuous_rise = False
                                        continuous_fall = False
                                hour_open = 0

                        if open_price:
                            if exchange == "OKX":
                                pure_symbol = vt_symbol.split("-")[0]

                            elif exchange == "BINANCE":
                                pure_symbol = vt_symbol.split("USDT")[0]
                            
                            # 计算涨跌幅
                            rate = close_price / open_price - 1
                            contract_rate_dict[pure_symbol] = rate
                            
                            # 筛选连续涨跌的合约
                            if continuous_rise:
                                continuous_rise_list.add(pure_symbol)
                            
                            if continuous_fall:
                                continuous_fall_list.add(pure_symbol)

                    if contract_rate_dict:
                        # BTC、ETH涨跌幅
                        btc_rate = contract_rate_dict["BTC"]
                        contract_rate_dict.pop("BTC")
                        eth_rate = contract_rate_dict["ETH"]
                        contract_rate_dict.pop("ETH")

                        # 转换成dataframe
                        df_rates = pd.DataFrame.from_dict(contract_rate_dict, orient='index', columns=['rate'])
                        df_rates.index.name = 'contract'
                        df_rates.reset_index(inplace=True)
                        
                        # 筛选并排序
                        filtered_df_rise1 = df_rates[(df_rates['rate'] >= 0.09)].copy()
                        filtered_df_rise1 = filtered_df_rise1.sort_values(by='rate', ascending=False)
                        filtered_df_rise1.loc[:, 'rate'] = filtered_df_rise1['rate'].apply(lambda x: f"{x*100:.2f}%")

                        filtered_df_rise2 = df_rates[(df_rates['rate'] >= 0.06) & (df_rates['rate'] < 0.09)].copy()
                        filtered_df_rise2 = filtered_df_rise2.sort_values(by='rate', ascending=False)
                        filtered_df_rise2.loc[:, 'rate'] = filtered_df_rise2['rate'].apply(lambda x: f"{x*100:.2f}%")

                        filtered_df_rise3 = df_rates[(df_rates['rate'] >= 0.03) & (df_rates['rate'] < 0.06)].copy()
                        filtered_df_rise3 = filtered_df_rise3.sort_values(by='rate', ascending=False)
                        filtered_df_rise3.loc[:, 'rate'] = filtered_df_rise3['rate'].apply(lambda x: f"{x*100:.2f}%")

                        filtered_df_fall1 = df_rates[(df_rates['rate'] <= -0.09)].copy()
                        filtered_df_fall1 = filtered_df_fall1.sort_values(by='rate', ascending=False)
                        filtered_df_fall1.loc[:, 'rate'] = filtered_df_fall1['rate'].apply(lambda x: f"{x*100:.2f}%")

                        filtered_df_fall2 = df_rates[(df_rates['rate'] <= -0.06) & (df_rates['rate'] > -0.09)].copy()
                        filtered_df_fall2 = filtered_df_fall2.sort_values(by='rate', ascending=False)
                        filtered_df_fall2.loc[:, 'rate'] = filtered_df_fall2['rate'].apply(lambda x: f"{x*100:.2f}%")

                        filtered_df_fall3 = df_rates[(df_rates['rate'] <= -0.03) & (df_rates['rate'] > -0.06)].copy()
                        filtered_df_fall3 = filtered_df_fall3.sort_values(by='rate', ascending=False)
                        filtered_df_fall3.loc[:, 'rate'] = filtered_df_fall3['rate'].apply(lambda x: f"{x*100:.2f}%")

                        # 创建PDF文档
                        dir_path = f"dataservice{DIR_SYMBOL}PDFs"
                        if not os.path.exists(dir_path):
                            os.makedirs(dir_path)

                        if exchange == "OKX":
                            pdf_filename = f"{dir_path}{DIR_SYMBOL}okx_contract_rates.pdf"

                        elif exchange == "BINANCE":
                            pdf_filename = f"{dir_path}{DIR_SYMBOL}binance_contract_rates.pdf"

                        doc = SimpleDocTemplate(pdf_filename, pagesize=letter)
                        elements = []

                        # 标题样式设置
                        styles = getSampleStyleSheet()
                        title_style = styles['Italic']
                        title_style.alignment = 1
                        title_style.textColor = colors.lightgrey

                        title_style = styles['Heading1']
                        title_style.alignment = 1
                        title_style.textColor = colors.goldenrod

                        title_style = styles['Heading2']
                        title_style.alignment = 1
                        title_style.textColor = colors.red
                        title_style.fontSize = 12

                        title_style = styles['Heading3']
                        title_style.alignment = 1
                        title_style.textColor = colors.green
                        title_style.fontSize = 12

                        title_style = styles['Bullet']
                        title_style.alignment = 1
                        title_style.textColor = colors.lightgrey
                        title_style.fontSize = 9

                        """ BTC、ETH """
                        elements.append(Paragraph(f"{datetime.now().replace(minute=0, second=0, microsecond=0)}", styles['Italic']))
                        elements.append(Spacer(1, 12))
                        elements.append(Spacer(1, 12))
                        elements.append(Paragraph(f"BTC {btc_rate*100:.2f}%", styles['Heading1']))
                        elements.append(Paragraph(f"ETH {eth_rate*100:.2f}%", styles['Heading1']))
                        elements.append(Spacer(1, 12))
                        
                        """ RISE_1 """
                        if len(filtered_df_rise1):
                            data = filtered_df_rise1.values.tolist()
                            table = Table(data)
                            table.setStyle(TableStyle([
                                ('GRID', (0, 0), (-1, -1), 1, colors.red)
                            ]))
                            elements.append(table)
                            elements.append(Spacer(1, 12))

                        """ RISE_2 """
                        if len(filtered_df_rise2):
                            data = filtered_df_rise2.values.tolist()
                            table = Table(data)
                            table.setStyle(TableStyle([
                                ('GRID', (0, 0), (-1, -1), 1, colors.red)
                            ]))
                            elements.append(table)
                            elements.append(Spacer(1, 12))

                        """ RISE_3 """
                        if len(filtered_df_rise3):
                            data = filtered_df_rise3.values.tolist()
                            table = Table(data)
                            table.setStyle(TableStyle([
                                ('GRID', (0, 0), (-1, -1), 1, colors.red)
                            ]))
                            elements.append(table)
                            elements.append(Spacer(1, 12))

                        """ FALL_1 """
                        if len(filtered_df_fall1):
                            data = filtered_df_fall1.values.tolist()
                            table = Table(data)
                            table.setStyle(TableStyle([
                                ('GRID', (0, 0), (-1, -1), 1, colors.green)
                            ]))
                            elements.append(table)
                            elements.append(Spacer(1, 12))

                        """ FALL_2 """
                        if len(filtered_df_fall2):
                            data = filtered_df_fall2.values.tolist()
                            table = Table(data)
                            table.setStyle(TableStyle([
                                ('GRID', (0, 0), (-1, -1), 1, colors.green)
                            ]))
                            elements.append(table)
                            elements.append(Spacer(1, 12))

                        """ FALL_3 """
                        if len(filtered_df_fall3):
                            data = filtered_df_fall3.values.tolist()
                            table = Table(data)
                            table.setStyle(TableStyle([
                                ('GRID', (0, 0), (-1, -1), 1, colors.green)
                            ]))
                            elements.append(table)
                            elements.append(Spacer(1, 12))

                        if len(continuous_rise_list):
                            elements.append(Paragraph(f"continuous rise", styles['Bullet']))
                            content = " ".join(continuous_rise_list)
                            elements.append(Paragraph(content, styles['Heading2']))
                            elements.append(Spacer(1, 12))

                        if len(continuous_fall_list):
                            elements.append(Paragraph(f"continuous fall", styles['Bullet']))
                            content = " ".join(continuous_fall_list)
                            elements.append(Paragraph(content, styles['Heading3']))

                        # 生成PDF
                        doc.build(elements)
                        print(f"PDF 文档 '{pdf_filename}' 创建成功")

                        # 发送email
                        if not current_hour % 1:
                            pdf_full_path = os.path.abspath(pdf_filename)
                            email_engine.send_email(subject=f"{exchange}行情推送", content=f"点击附件查看", pdf_file_path=pdf_full_path)

            sleep(10)

    def get_download_setting(self):
        setting = {}
        file_path = "download_setting.json"
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                setting = json.load(f)
        return setting
    
    def save_download_setting(self, setting: dict):
        file_path = "download_setting.json"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(setting, ensure_ascii=False))

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")
    
if __name__ == "__main__":
    utility = DownloadUtility()

    # 获取各大交易所所有合约列表
    # utility.get_instruments_list()

    # 获取新上市的合约列表（附上上市日期并保存到.csv文件）
    # utility.get_new_instruments_list(Exchange.OKX)
    # utility.get_new_instruments_list(Exchange.BINANCE)
    # utility.get_new_instruments_list(Exchange.BYBIT)

    # 下载交易所所有合约分钟Bar数据（相同代币优先级OKX > BYBIT > BINANCE）
    # utility.download_instruments_bar_data([Exchange.OKX, Exchange.BYBIT, Exchange.BINANCE])

    # 下载数据
    utility.download_data()
    
    # 更新数据并生成市场信号
    # utility.update_data_signal()
