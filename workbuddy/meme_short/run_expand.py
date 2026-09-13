# -*- coding: utf-8 -*-
"""
run_expand.py — meme_short 扩展到 09-12 的统一调度器
======================================================
gen 任务(binance_24h_change_all.py)拉完全合约 5m 并写进 Mongo 后, 一键跑完:
  1) market_regime.py   重算 regime_btc.csv (从 Mongo 读 BTCUSDT, 口径一致, 覆盖到 09-12)
  2) top1_analysis.py   聚合 000000_top1_rise.csv (涨幅榜 Top1 信号)
  3) backtest_short_top1_persym.py 三版同源回测:
       final      = REGIME_FILTER=0              (无 regime, 与历史 baseline 等价)
       bear       = REGIME_FILTER=1 REGIME_ALLOW=BEAR
       nosideways = REGIME_FILTER=1 REGIME_ALLOW=BULL,BEAR
     三版各独立 BT_OUT + BT_PROGRESS, 防止断点续跑文件互盖。

日志全部进 data/change_20251201_20260912/(按用户要求: 运行日志不落顶层)。
日志命名约定(2026-09-13 超拍板): 所有日志文件名一律带 "_" 前缀,
    例如 _run_expand.log / _step1_regime.log / _step3_bt_final.log。
所有 env 在进程内强制设好(含代理), 不依赖破壳 cwd / 全局坏代理。

用法:
    python run_expand.py
"""
import os
import sys
import runpy

WD = os.path.dirname(os.path.abspath(__file__))
WIN = "change_20251201_20260912"
SRC = os.path.join(WD, "data", WIN)
LOGDIR = SRC  # 日志进数据文件夹
os.chdir(WD)

# 代理强制(防止破壳全局坏代理 14068 把脚本坑死)
os.environ["HTTPS_PROXY"] = "http://127.0.0.1:10809"
os.environ["HTTP_PROXY"] = "http://127.0.0.1:10809"
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

START_DT = "2025-12-01 00:00"
END_DT = "2026-09-12 00:00"
os.environ["START_DT"] = START_DT
os.environ["END_DT"] = END_DT
os.environ["SRC_DIR"] = SRC
os.environ["SIGNAL_FILE"] = os.path.join(SRC, "000000_top1_rise.csv")


def run_step(name, script, log_name, extra_env=None):
    print("\n" + "=" * 60)
    print(f"STEP {name}: {script}")
    print("=" * 60, flush=True)
    if extra_env:
        for k, v in extra_env.items():
            os.environ[k] = v
    log_path = os.path.join(LOGDIR, log_name)
    # 重定向 stdout/stderr 到日志(追加模式, 三步互不覆盖)
    old_out, old_err = sys.stdout, sys.stderr
    with open(log_path, "a", encoding="utf-8") as lf:
        sys.stdout = lf
        sys.stderr = lf
        try:
            runpy.run_path(os.path.join(WD, script), run_name="__main__")
        finally:
            sys.stdout, sys.stderr = old_out, old_err
    print(f"  -> 日志: {log_path}", flush=True)


# 1) regime 重算(从 Mongo 读 BTC, 覆盖到 09-12)
run_step("regime", "market_regime.py", "_step1_regime.log")

# 2) 聚合 top1 信号
run_step("aggregate", "top1_analysis.py", "_step2_top1.log")

# 3) 三版回测(各独立 OUT/PROGRESS)
run_step("bt_final", "backtest_short_top1_persym.py", "_step3_bt_final.log",
         {"REGIME_FILTER": "0", "REGIME_ALLOW": "BULL,BEAR",
          "BT_OUT": os.path.join(SRC, "_bt_mp9_final.csv"),
          "BT_PROGRESS": os.path.join(SRC, "_bt_prog_final.json")})

run_step("bt_bear", "backtest_short_top1_persym.py", "_step3_bt_bear.log",
         {"REGIME_FILTER": "1", "REGIME_ALLOW": "BEAR",
          "BT_OUT": os.path.join(SRC, "_bt_mp9_bear.csv"),
          "BT_PROGRESS": os.path.join(SRC, "_bt_prog_bear.json")})

run_step("bt_nosideways", "backtest_short_top1_persym.py", "_step3_bt_nosideways.log",
         {"REGIME_FILTER": "1", "REGIME_ALLOW": "BULL,BEAR",
          "BT_OUT": os.path.join(SRC, "_bt_mp9_nosideways.csv"),
          "BT_PROGRESS": os.path.join(SRC, "_bt_prog_nosideways.json")})

print("\n✅ run_expand 全部完成。产物在:", SRC, flush=True)
print("  _bt_mp9_final.csv / _bt_mp9_bear.csv / _bt_mp9_nosideways.csv", flush=True)
