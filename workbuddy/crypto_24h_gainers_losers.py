# -*- coding: utf-8 -*-
"""
获取加密货币涨跌幅榜（24h + 1h），保存到 CSV。字段：symbol, change, price, volume

数据源：Binance U 本位合约
  - 24h：fapi/v1/ticker/24hr（全量，含 priceChangePercent）
  - 1h ：逐 symbol 拉 fapi/v1/klines(interval=1h, limit=2)，取「1 小时前整点」那根 K 线的
        开盘价（升序 data[0]）作基准，算 (now - 1h_ago)/1h_ago = 过去 1 小时涨跌幅
        （Binance 无 1h 批量涨跌接口，用并发拉 K 线基准价，726 个约 10~30s）
  - Binance 是 CoinGlass 加密行情的同源上游，免费、全量、无需 API key
代理：沿用项目惯例走本地代理 127.0.0.1:10811（与 OKXDataService 一致）；不通时回退直连。
"""
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

PROXIES = {
    "http": "http://127.0.0.1:10811",
    "https": "http://127.0.0.1:10811",
}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
KLINE_URL = "https://fapi.binance.com/fapi/v1/klines"
TOP_N = 50
FIELDS = ["symbol", "change", "price", "volume"]


def fetch_ticker():
    """拉全量 24h ticker，先代理后直连。"""
    last_err = None
    for label, proxies in (("proxy", PROXIES), ("direct", None)):
        try:
            r = requests.get(TICKER_URL, headers={"User-Agent": UA},
                             proxies=proxies, timeout=30)
            r.raise_for_status()
            data = r.json()
            print(f"[ok] 通过 {label} 拉取 ticker 成功，共 {len(data)} 个交易对")
            return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            print(f"[warn] {label} ticker 失败: {e}")
    raise RuntimeError(f"ticker 两种连接方式均失败: {last_err}")


def get_1h_base(symbol):
    """拉「1 小时前整点」K 线开盘价作基准（升序返回，data[0] 即上一根完整 1h K 线）。"""
    try:
        r = requests.get(KLINE_URL, params={"symbol": symbol, "interval": "1h", "limit": 2},
                         headers={"User-Agent": UA}, proxies=PROXIES, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data and len(data) >= 2:
            return float(data[0][1])  # 上一根 1h K 线开盘价 ≈ 1 小时前价格
    except Exception:  # noqa: BLE001
        pass
    return None


def parse(rows):
    out = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        try:
            pct24 = float(r.get("priceChangePercent", "0"))
        except ValueError:
            pct24 = 0.0
        out.append({
            "symbol": sym,
            "price": r.get("lastPrice", ""),
            "volume": r.get("quoteVolume", ""),  # USDT 计价成交额
            "change_24h": pct24,
            "change_1h": None,
        })
    return out


def fill_1h(items):
    print(f"[..] 并发拉取 {len(items)} 个 symbol 的 1h 基准价 ...")
    done = 0
    with ThreadPoolExecutor(max_workers=20) as ex:
        fut = {ex.submit(get_1h_base, it["symbol"]): it for it in items}
        for f in as_completed(fut):
            it = fut[f]
            open1h = f.result()
            if open1h:
                try:
                    last = float(it["price"])
                except (TypeError, ValueError):
                    last = None
                if last:
                    it["change_1h"] = (last - open1h) / open1h * 100.0
            done += 1
            if done % 150 == 0:
                print(f"  {done}/{len(items)}")
    print(f"[ok] 1h 数据补全 {sum(1 for i in items if i['change_1h'] is not None)}/{len(items)}")


def write_csv(path, items, period):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for it in items:
            w.writerow({
                "symbol": it["symbol"],
                "change": round(it[f"change_{period}"], 4) if it[f"change_{period}"] is not None else "",
                "price": it["price"],
                "volume": it["volume"],
            })
    print(f"[ok] 已写入 {path}（{len(items)} 条）")


def main():
    rows = fetch_ticker()
    items = parse(rows)
    if not items:
        print("[err] 未解析到任何 USDT 合约数据，退出")
        sys.exit(1)

    fill_1h(items)

    here = os.path.dirname(os.path.abspath(__file__))

    # 24h 榜
    items.sort(key=lambda x: x["change_24h"], reverse=True)
    write_csv(os.path.join(here, "crypto_gainers.csv"), items[:TOP_N], "24h")
    write_csv(os.path.join(here, "crypto_losers.csv"), items[::-1][:TOP_N], "24h")

    # 1h 榜（只排有 1h 数据的）
    valid = [i for i in items if i["change_1h"] is not None]
    valid.sort(key=lambda x: x["change_1h"], reverse=True)
    write_csv(os.path.join(here, "crypto_gainers_1h.csv"), valid[:TOP_N], "1h")
    write_csv(os.path.join(here, "crypto_losers_1h.csv"), valid[::-1][:TOP_N], "1h")

    print("\n=== 24h 涨幅 Top5 ===")
    for it in items[:5]:
        print(f"  {it['symbol']:12s} {it['change_24h']:+.2f}%")
    print("=== 24h 跌幅 Top5 ===")
    for it in items[::-1][:5]:
        print(f"  {it['symbol']:12s} {it['change_24h']:+.2f}%")
    print("\n=== 1h 涨幅 Top5 ===")
    for it in valid[:5]:
        print(f"  {it['symbol']:12s} {it['change_1h']:+.2f}%")
    print("=== 1h 跌幅 Top5 ===")
    for it in valid[::-1][:5]:
        print(f"  {it['symbol']:12s} {it['change_1h']:+.2f}%")


if __name__ == "__main__":
    main()
