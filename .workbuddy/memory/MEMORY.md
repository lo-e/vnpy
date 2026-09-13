# 项目长期记忆 (vnpy / meme_short 做空妖币)

## 权限边界
- 当前会话**已解除 AI 只读约束**：数据任务可自主动手；交易代码仅作只读咨询，跑回测前先与超确认。历史铁律(08-18)仍作默认基线：非确认不动文件。

## 目录与数据约定
- 策略全量在 `workbuddy/meme_short/`（运行 `cd meme_short`，数据 `data/change_*`）；reversal_* 留 workbuddy 顶层。
- 涨跌幅任务：用户给确定起止时间（北京时间UTC+8），输出 `data/change_{起}_{止}/{sym}.csv`；K线落 `Workbuddy_5Min_Db/{SYMBOL}.BINANCE`；close[T]=openTime=T-5min close，change%=(close[T]-close[T-24h])/close[T-24h]*100。
- 币种 `data/symbols_usdt_perp.json`（PERPETUAL+TRADIFI_PERPETUAL+USDT+TRADING，含TradFi 170个，527→696）。
- 运行日志按窗口进 `data/change_{起}_{止}/`；顶层只留脚本+通用文件。**日志文件名一律带 `_` 前缀**(超 2026-09-13 拍板)：`_run_expand.log`/`_step1_regime.log`/`_step3_bt_final.log` 等。网络走代理 127.0.0.1:10809；klines>1000分页。
- 壳环境坑：破壳全局代理 14068 是坏的 + 壳 cwd=工作区根(非 meme_short)。跑脚本用**绝对路径** + `launch_gen.py`/`run_expand.py` 这类启动器在 python 内 `os.environ` 强设 10809 + `os.chdir`。用 anaconda python(有 requests/pymongo)，managed python 没有。

## 定稿策略（做空 24h 涨幅 Top1 妖币均值回归）
- 信号每整10分钟取Top1，读Mongo 5m；开仓价=openTime=T-5min close。
- 入场过滤：MAX_TOP1_PCT=80 / RATIO6_MIN=-1 / HIGH_AGE_MAX=12 / 创30天新高 / SKIP_SIM_FILTER=1。
- 仓位：PULSE_W=2 PULSE_TH=0.85（r6≥0.85→2x）MAX_PERMIT_OPENS=9。
- 出场先到先得：①expire≥24h ②pump_stop(8h缓冲后high破24h高,PUMP_DELAY_H=8) ③8h内兜底止损=开仓价×STOP_FACTOR。
- 固定止盈/止损 TAKE_PROFIT_PCT=80 + STOP_FACTOR=1.8（±80%封顶）；p24锚已删。净收益另扣~0.35pp/份。

## 定稿参数（backtest_short_top1.py 默认值，裸跑=定稿）
RATIO6_MIN=-1/PULSE_W=2/PULSE_TH=0.85/PUMP_ENABLE=1/PUMP_DELAY_H=8/SKIP_SIM_FILTER=1/T0=2025-12-01/T1=2026-08-16/CVD_DIVERGE_W=12/OFF_HIGH_MAX=0.15/ADD_ON_BREAK=1/MAX_PERMIT_OPENS=9/HIGH_AGE_MAX=12/TAKE_PROFIT_PCT=80/STOP_FACTOR=1.8。SIGNAL_FILE 可env覆盖。（backtest_short_top1.py 裸跑默认值；最新定稿窗口 2025-12-01~2026-09-12 由 run_expand.py 覆盖 T1=2026-09-12 重跑，见上「当前定稿成绩」）

## 当前定稿成绩（2026-09-13，窗口 2025-12-01~2026-09-12）
- **无regime版(定稿)** `_bt_mp9_final.csv`：1322笔/胜率55.52%/加权+943.09%/maxDD56.56/ddRatio0.0589，三版曲线`_curve_3versions.html`（data/change_20251201_20260912/）。
- 对照：bear(只BEAR)`_bt_mp9_bear.csv`(+865.29/OOS 0笔) / nosideways`_bt_mp9_nosideways.csv`(+865.79/OOS -15.47)。
- **edge 全在 BEAR**：BULL 桶全窗口 72笔仅+0.50pp；OOS(08-29→09-12, 14天) 66笔 -5.29pp 走平在分布内(14d百分位9.5%)，判定 EDGE HOLD。
- **OOS 预警**：BTC 08-24 起多次判 BULL(+7.25%)，pump_stop 率 38%→64%；BULL 环境做空妖币结构性偏弱，refresh 重点盯 pump_stop 率。
- **nosideways 选择效应教训**：final 里 SIDEWAYS 交易占 permit 名额反而挤掉部分 BULL 亏损单，剔 SIDEWAYS 在 2026 OOS 是负贡献（与 2024 相反）——小样本结论会翻脸，禁凭短窗拍板。

## 定稿工作流规则（08-16拍板）
1. 定稿后用定稿代码重跑生成`final`结尾CSV。2. 自审：边界sanity+逐笔对照实际数据。3. 清理舍弃文件只留核心。4. 更新曲线(plot_final_curve.py，顶部显maxDD+ddRatio)。5. **双版本回测(08-24起默认)**：无regime(_base)+剔SIDEWAYS(_regime_final)，CSV+HTML都要。

## regime 系统（08-23/24定稿）
- `market_regime.py`：BTC日收盘复合年化斜率分类，BULL_R200=0.25/BEAR_R200=-0.15；REGIME_FILTER(默认0)/REGIME_ALLOW(BULL,BEAR=剔SIDEWAYS/BEAR=只BEAR)；T日信号用T-1日regime；end_ms clamp最新完整K。
- 2024：剔SIDEWAYS+306.3 vs 基线+103（BEAR甜区+153，SIDEWAYS毒药-147）。⚠️regime_btc.csv须覆盖≥回测窗口，缺失=白放行=失真。

## 关键教训（铁律）
- 差异归因先查代码再谈数据（曾误归因数据残缺，实为p24锚逻辑）。
- 笔数骤减先查数据完整性（曾Mongo不稳误判负优化）。
- 加仓alpha跨MP比较用非加权pnl%，加权均盈被均分权重污染。
- EF/E方案(funding拥挤)可用；F(OI背离)币安只留30天OI→历史不可行。
- A/B/C/E/F入场点优化全否决归档，禁动令沿失败路线不重动。

## meme_long 镜像(做多24h跌幅Top1) 负样本归档 (2026-08-30)
- 镜像 meme_short 反向：信号=跌幅榜Top1，做多均值回归。引擎`workbuddy/meme_long/backtest_long_top1.py`方向镜像正确(PnL=(exit-open)/open, 止损×0.2/止盈×1.8/confirm_new_low/dump_stop)。修复原`ensure_close_map`代理不可达时`while not check_proxy():sleep(60)`无限死等→改本地覆盖即返回+代理等待上限。
- 窗口2025-12-01~2026-08-28 离线125币：219笔/胜率35.6%/加权−1.312%(累计−42.57%)。结构不对称真实：暴跌币继续跌/阴跌，做多均值回归不成立；暴涨币倾向回落，做空才有效。
- 83/132币 confirm_new_low 100%不通过(跌幅第一但未创30d新低)→引擎正确拒买，非bug。RISE侧313币100%过confirm_new_high。诊断`diag_filters.py`可复现。
- 结论：无edge，归档负样本，勿沿此线重动。代理127.0.0.1:4715挂→7币缺数据跳过；全量需代理恢复跑311币complete文件。
