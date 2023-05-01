def calculate_increace_info(direction:int, price_before:float, value_before:float, price_after:float, value_after:float):
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

    print(f"原持仓价格：{position_price_before}\n原持仓数量：{volume_before}\n")
    print(f"成交价格：{trade_price}\n成交数量：{trade_volume}\n")
    print(f"现持仓价格：{position_price_after}\n现持仓数量：{volume_after}\n")
    print(f"原本亏损比率：{loss_before}%\n现亏损比率：{loss_after}%")

if __name__ == "__main__":
    symbol = "EOSUSDT.BYBIT_多"
    direction = 1
    price_before = 1.06314678
    value_before = 65902.87
    price_after = 1.0159213
    value_after = 306300.27
    calculate_increace_info(direction=direction, price_before=price_before, value_before=value_before, price_after=price_after, value_after=value_after)