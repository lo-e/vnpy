# encoding: UTF-8

import os
from vnpy.trader.utility import DIR_SYMBOL

def get_csv_path():
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    csv_path = path.rstrip(file_name) + f'CSVs{DIR_SYMBOL}'
    return csv_path