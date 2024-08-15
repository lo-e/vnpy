# encoding: UTF-8

import os
from vnpy.trader.utility import DIR_SYMBOL
from .OKXDataService import okx_get_symbol_list, OKXType, okx_get_first_bar_datetime
from .BinanceDataService import binance_get_symbol_list, BinanceType, binance_get_first_bar_datetime
from datetime import datetime, timedelta

def get_csv_path(target_dir: str = ""):
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    if target_dir:
        csv_path = path.rstrip(file_name) + f"CSVs_{DIR_SYMBOL}" + f"{target_dir}{DIR_SYMBOL}"
    else:
        csv_path = path.rstrip(file_name) + f"CSVs{DIR_SYMBOL}"
    return csv_path

def get_instruments_list():
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