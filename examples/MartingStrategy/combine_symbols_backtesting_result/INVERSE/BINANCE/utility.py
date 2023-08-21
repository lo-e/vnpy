import os
import pandas as pd
import csv

def filter():
    resultList = []
    target_symbols = ['AAVEUSDT.BINANCE', 'AXSUSDT.BINANCE', 'LINKUSDT.BINANCE']

    # 筛选csv数据
    file_path = f"target_1.csv"
    if os.path.exists(file_path):
        csv_data = pd.read_csv(file_path)
        for _, row in csv_data.iterrows():
            row_dict = dict(row)
            row_symbols = row_dict["symbols"].split(",")
            count = 0
            for symbol in row_symbols:
                symbol = symbol.replace(" ", "")
                if symbol in target_symbols:
                    count += 1
            if count <= 0:
                resultList.append(row_dict)
    
    # 结果保存到csv
    if len(resultList):
        fieldNames = ["symbols", "total_pnl", "close_trade", "max_drawdown", "over_drawdown", "over_drawdown_count"]
        file_path = f"filter.csv"
        with open(file_path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)

if __name__ == "__main__":
    filter()