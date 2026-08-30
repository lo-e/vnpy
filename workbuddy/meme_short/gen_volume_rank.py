"""币安 U 本位永续 24h 成交量(USDT)排行榜 → CSV。

数据来源: /fapi/v1/ticker/24hr (币安原生直接给 quoteVolume=24h USDT 成交额)
过滤口径: 与涨跌幅任务同款宇宙 —— symbols_usdt_perp.json (PERPETUAL + TRADIFI_PERPETUAL + USDT + TRADING)
排序: 按 quoteVolume(24h USDT 成交额) 降序
输出: data/volume_rank_usdt_perp_{YYYYMMDD}.csv

注: 涨跌幅脚本(binance_24h_change_all.py)用的是 /fapi/v1/klines, 仅存单根5m K 的 quoteVolume,
    不含 24h 累计成交量。本脚本改用 ticker/24hr 直接取 24h 累计 quoteVolume。
"""
import os
import json
import csv
import requests
from datetime import datetime, timezone, timedelta

LOCAL_TZ = timezone(timedelta(hours=8))
SYMBOLS_FILE = "data/symbols_usdt_perp.json"
TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
OUT_DIR = "data"

with open(SYMBOLS_FILE, encoding="utf-8") as f:
    UNIVERSE = set(json.load(f))   # 涨跌幅同款合约清单


def fetch_ticker():
    """拉全量 U 本位永续 24h 行情 (contractType=PERPETUAL 覆盖 PERPETUAL + TRADIFI_PERPETUAL)。"""
    r = requests.get(TICKER_URL, params={"contractType": "PERPETUAL"}, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    raw = fetch_ticker()
    # 与涨跌幅宇宙取交集, 只保留清单内合约
    rows = []
    in_universe = 0
    for t in raw:
        sym = t["symbol"]
        if sym not in UNIVERSE:
            continue
        in_universe += 1
        rows.append({
            "symbol": sym,
            "quote_volume_24h_usdt": float(t["quoteVolume"]),
            "volume_24h_base": float(t["volume"]),
            "price_change_percent_24h": float(t["priceChangePercent"]),
            "last_price": float(t["lastPrice"]),
        })
    # 按 24h USDT 成交额降序
    rows.sort(key=lambda x: x["quote_volume_24h_usdt"], reverse=True)

    os.makedirs(OUT_DIR, exist_ok=True)
    today = datetime.now(LOCAL_TZ).strftime("%Y%m%d")
    out = os.path.join(OUT_DIR, f"volume_rank_usdt_perp_{today}.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rank", "symbol", "quote_volume_24h_usdt", "volume_24h_base",
                    "price_change_percent_24h", "last_price"])
        for i, r in enumerate(rows, 1):
            w.writerow([i, r["symbol"], f'{r["quote_volume_24h_usdt"]:.2f}',
                        f'{r["volume_24h_base"]:.6f}',
                        f'{r["price_change_percent_24h"]:.4f}',
                        f'{r["last_price"]:.8g}'])

    print(f"ticker 返回总数: {len(raw)}")
    print(f"命中涨跌幅宇宙: {in_universe} / 清单 {len(UNIVERSE)} (未命中 {len(UNIVERSE)-in_universe})")
    print(f"输出: {out}  ({len(rows)} 行)")
    print("\n--- Top 20 (按 24h USDT 成交额) ---")
    for r in rows[:20]:
        print(f'{r["symbol"]:>16}  {r["quote_volume_24h_usdt"]:>18,.2f} USDT  '
              f'涨跌幅 {r["price_change_percent_24h"]:+7.2f}%')


if __name__ == "__main__":
    main()
