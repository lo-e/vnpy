import json

def calculate_increace_info(trader:str, symbol:str):
    # json文件中查找交易数据
    trades_list = []
    with open("trade_data.json", "r", encoding='utf-8') as f:
        json_data = json.load(f)
        symbol_trade_dict = json_data.get(trader, {})
        trades_list = symbol_trade_dict.get(symbol, [])

    if trades_list:
        for trade_data in trades_list:
            dt = trade_data["datetime"]
            direction = trade_data["direction"]
            price_before = trade_data["price_before"]
            value_before = trade_data["value_before"]
            price_after = trade_data["price_after"]
            value_after = trade_data["value_after"]

            # 策略方向
            direction = direction

            # ============

            # 之前持仓价格
            position_price_before = price_before

            # 之前持仓价值
            position_value_before = value_before

            # 之前持仓数量
            volume_before = position_value_before / position_price_before

            # ============

            # 之后持仓价格
            position_price_after = price_after

            # 之后持仓价值
            position_value_after = value_after

            # 之后持仓数量
            volume_after = position_value_after / position_price_after

            # ============

            # 成交的合约数量
            trade_volume = volume_after - volume_before

            # 成交价格
            trade_price = (position_value_after - position_value_before) / trade_volume

            # 之前亏损比率
            loss_before = (((trade_price / position_price_before) - 1)) * direction * 100

            # 之后亏损比率
            loss_after = (((trade_price / position_price_after) - 1)) * direction * 100

            print(f"====== {dt} ======")
            print(f"原持仓价格：{position_price_before}\n原持仓数量：{volume_before}\n原持仓价值：{position_value_before}\n")
            print(f"成交价格：{trade_price}\n成交数量：{trade_volume}\n")
            print(f"后持仓价格：{position_price_after}\n后持仓数量：{volume_after}\n后持仓价值：{position_value_after}\n")
            print(f"原本亏损比率：{loss_before}%\n后亏损比率：{loss_after}%\n")

    else:
        print(f"未找到【{trader} / {symbol}】的交易数据！")
        return

if __name__ == "__main__":
    trader = "ai_trader_1"
    symbol = "1000SHIBUSDT_多"
    calculate_increace_info(trader=trader, symbol=symbol)