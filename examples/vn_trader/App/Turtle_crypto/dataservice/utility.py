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