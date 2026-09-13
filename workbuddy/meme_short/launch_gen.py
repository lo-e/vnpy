"""启动器: 在 python 进程内强制设置代理(绕过破壳 env 不生效的问题), 再运行信号生成。
代理 10809 对 fapi 合约 K 线可用; 全局 14068 是坏的。
用法: python launch_gen.py
"""
import os
import runpy

os.chdir(os.path.dirname(os.path.abspath(__file__)))  # 锁定 cwd=脚本目录, 不依赖壳
os.environ["HTTPS_PROXY"] = "http://127.0.0.1:10809"
os.environ["HTTP_PROXY"] = "http://127.0.0.1:10809"
os.environ["NO_PROXY"] = "127.0.0.1,localhost"
os.environ["START_DT"] = "2025-12-01 00:00"
os.environ["END_DT"] = "2026-09-12 00:00"
os.environ["BW"] = "2"  # 并发降2, 避免弱代理(10809)并发502

# 启动诊断: 确认实际代理 + 启动瞬间连通性
import requests as _rq
print("=== launch_gen diag ===", flush=True)
print("HTTPS_PROXY =", os.environ.get("HTTPS_PROXY"), flush=True)
print("HTTP_PROXY  =", os.environ.get("HTTP_PROXY"), flush=True)
for _u in ("https://fapi.binance.com/fapi/v1/exchangeInfo",
           "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=2"):
    try:
        _r = _rq.get(_u, timeout=20)
        print(f"  {_u[:55]} -> {_r.status_code}", flush=True)
    except Exception as _e:
        print(f"  {_u[:55]} -> FAIL {type(_e).__name__}: {str(_e)[:90]}", flush=True)
print("=== diag end ===", flush=True)

runpy.run_path("binance_24h_change_all.py", run_name="__main__")
