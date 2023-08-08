# encoding: UTF-8

import os
from vnpy.trader.utility import DIR_SYMBOL


def get_csv_path(target_dir: str = ""):
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    if target_dir:
        csv_path = path.rstrip(file_name) + f"CSVs_{DIR_SYMBOL}" + f"{target_dir}{DIR_SYMBOL}"
    else:
        csv_path = path.rstrip(file_name) + f"CSVs{DIR_SYMBOL}"
    return csv_path
