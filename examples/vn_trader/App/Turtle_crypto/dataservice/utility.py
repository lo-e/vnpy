# encoding: UTF-8

import os
from vnpy.trader.utility import DIR_SYMBOL
import pandas as pd
import shutil

def get_csv_path(target_dir: str = ""):
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    if target_dir:
        csv_path = path.rstrip(file_name) + f"CSVs_temp{DIR_SYMBOL}" + f"{target_dir}{DIR_SYMBOL}"
    else:
        csv_path = path.rstrip(file_name) + f"CSVs{DIR_SYMBOL}"
    return csv_path

def save_df_data(df: pd.DataFrame, file_path:str):
    # 确保文件夹存在
    file_elements = file_path.split(DIR_SYMBOL)
    dir_path = DIR_SYMBOL.join(file_elements[:-1])
    os.makedirs(dir_path, exist_ok=True)

    # 保存到临时csv文件
    temp_file_path = file_path.split(".csv")[0] + f"_temp.csv"
    df.to_csv(temp_file_path, index=False)

    # 将临时文件替换为目标文件
    shutil.move(temp_file_path, file_path)