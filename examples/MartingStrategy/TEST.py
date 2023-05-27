def calculate_phase_loss(phase_count:int, increase_type:int=1):
    phase_value_list = [500.0, 1500.0, 3500.0, 7500.0, 15500.0, 31500.0, 63500.0, 127500.0, 255500.0, 511500.0, 1023500.0, 2047500.0]
    if increase_type == 0:
        increase_rate_list = [0.01 * 2*i for i in range(len(phase_value_list))]

    elif increase_type == 1:
        increase_rate_list = [0.01 * 2**i for i in range(len(phase_value_list))]
    else:
        exit("检查代码！")
    init_price = 100

    value_before = phase_value_list[0]
    price_before = init_price
    phase_list = range(phase_count)[1:]
    print(f"\n第{1}阶段\n持仓价值：{value_before}\n持仓数量：{value_before/price_before}\n持仓均价：{price_before}")
    for i in phase_list:
        value_after = phase_value_list[i]
        increase_rate = increase_rate_list[i]
        increase_rate = min(increase_rate, 0.32)
        trade_price = price_before * (1 + increase_rate)

        trade_volume = (value_after - value_before) / trade_price
        price_after = value_after / ((value_before / price_before) + trade_volume)

        loss_before = ((trade_price / price_before) - 1) * 100
        loss_before = round(loss_before, 2)
        loss_before = f"{loss_before}%"

        loss_after= ((trade_price / price_after) - 1) * 100
        loss_after = round(loss_after, 2)
        loss_after = f"{loss_after}%"

        print(f"\n第{i+1}阶段\n持仓价值：{value_after}\n持仓数量：{value_after/price_after}\n持仓均价：{price_after}\n成交价格：{trade_price}\n加仓前亏损：{loss_before}\n加仓后亏损：{loss_after}")

        value_before = value_after
        price_before = price_after
    
    symbol_price_changed = ((trade_price / init_price) - 1) * 100
    symbol_price_changed = round(symbol_price_changed, 2)
    symbol_price_changed = f"{symbol_price_changed}%"

    position_price_changed = ((price_before / init_price) - 1) * 100
    position_price_changed = round(position_price_changed, 2)
    position_price_changed = f"{position_price_changed}%"
    print(f"\n\n============\n总共经历{phase_count}个阶段加仓\n初始持仓价值：{phase_value_list[0]}\t初始持仓价格：{init_price}\n最后持仓价值：{value_before}\t最后持仓价格：{price_before}\n持仓价格变化：{position_price_changed}\n合约价格变化：{symbol_price_changed}")

if __name__ == "__main__":
    calculate_phase_loss(phase_count=6, increase_type=1)