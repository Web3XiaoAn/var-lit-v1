# adaptive-median-v1 架构与公式

## 分层

`adaptive_strategy/` 只接受不可变值对象并返回 `Decision`，不访问网络、磁盘或交易客户端。`main.py` 负责同步 Var/Lighter 行情，执行风险授权，并把冻结候选交给现有 Firm Quote、Commit 和 Lighter 对冲链路。

核心对象：

- `MarketFrame`：双源时间、age/skew、Var BBO、500U 参考 VWAP、200U 执行 VWAP及双向 rate。
- `WindowStats`：5m/30m/1h 的 median、Q80、MAD、密度、最大断流和数据源；三个窗口都进入正式交易门槛。
- `ParameterEpoch`：模型/配置哈希、基准、各门槛组成、reserve 与有效期。
- `OpenCandidate` / `CloseCandidate`：冻结的决策上下文，不执行下单。
- `Decision`：`NO_ACTION`、`OPEN`、`CLOSE` 或 `PAUSE`，并给出稳定原因码。

## 参数编译

方向 `d` 的基准、机会线与最终开仓线：

```text
B_d = 0.15×M5m_d + 0.55×M30m_d + 0.30×M1h_d
Q_d = 0.15×Q80_5m_d + 0.55×Q80_30m_d + 0.30×Q80_1h_d

T_econ_d = -wear - B_opposite + reserve_open_d + reserve_close_opposite
T_open_d = max(Q_d, T_econ_d, T_balance_d)
```

30m 权重最高，用来匹配主要持仓周期；5m以较小权重修正短时结构，并与实时500U筛选、200U深度及 Firm Guard 共同捕捉瞬时套利；1h保留30%权重，避免只追逐短周期噪声。

平仓不是复用开仓 Q80，而是同时满足独立的“价差回归”和“整轮收益”公式：

```text
T_close_d = B_opposite（开仓时冻结）
close only if current_close_rate >= T_close_d
          and round_lower_bound >= floor(held_time)

floor(t) = 0U,                         t < 30m
         = -1.0bps × open_notional,    t >= 30m
```

rate 和 bps 都以小数表示；金额换算在实际订单名义金额上完成。v1 正常整轮 wear 为 1.0bps，开仓和平仓各包含两条腿、每腿临时 reserve 0.50bps。

每五分钟编译候选。5m/30m/1h在约60分钟全部就绪后，第一份完整候选立即激活；后续参数变化必须连续两轮同方向越过 `0.25×MAD1h` 死区，单次变化不超过 `0.50×MAD1h`，正式epoch至少冻结十分钟。在途候选始终使用自己冻结的epoch，参数更新不会重算它。

看板中的实时窗口只有在完整覆盖相应时长且密度、断流和最新样本均合格后才显示为就绪。正式参数必须等待5m、30m、1h三个实时窗口全部合格，不使用4h历史值顶替实时窗口。每秒原始行情帧继续追加到 `log/strategy_market_samples.jsonl`，因此离线研究仍可分析更长周期，但4h不再参与实盘公式或看板。数据用于复盘和后续人工修订，不会在实盘中自动改写模型。

看板每200ms用同一轮最新行情重算BBO、500U/200U价差和预估开仓盈亏；中位数在每个有效1秒样本后重新发布。正式门槛仍每5分钟编译、两次确认并按epoch冻结，展示刷新不会绕过参数稳定机制。

## 机会平衡

连续通过且间隔不超过 15 秒的样本合并为一个机会事件。最近一小时某方向事件数超过另一方向两倍时，只提高机会过多一侧的门槛，使保留事件最多为 `2×max(1, 较少侧事件数)`；不会降低弱方向门槛来凑数量。

## 决策与 Firm Guard

初筛使用 500U 参考 rate。双向同时通过时：

```text
score_d = (rate_d - T_d) / max(MAD30m_d, epsilon)
```

选择 score 更高方向；相同则选择实际 200U 整轮 lower bound 更高方向。Firm Guard 复用唯一一次 Firm Quote，并以冻结门槛、Firm 实际金额和该 Firm 数量对应的最新 Lighter 深度复核。新开仓 Firm 金额超过 200U 或冻结订单金额会拒绝并进入 `HALTED`；已有仓位按冻结的实际 BTC 数量平仓，价格变化导致平仓名义金额超过 200U 时不会被开仓上限阻断。

## 数据失败策略

统计异常、窗口未就绪、超过 60 秒断流、最新样本过期或 epoch 过期时，禁止新开仓。采样、受控平仓、异常恢复与账户对账仍继续工作。缺少自适应仓位冻结上下文时自动平仓失败关闭。

## 状态与发布

运行状态 schema 为 v2。旧状态为空时重新初始化；旧状态含仓位或未决意图时拒绝启动。自适应仓位必须携带模型、配置哈希和完整冻结 `OpenCandidate`。

发布流程固定为：`observe ≥ 1h` → 第一份完整参数立即激活 → 用户显式配置新canary令牌 → 最多一轮200U完整开平 → `REVIEW_REQUIRED` → 人工审计理论、Firm与实际磨损。首次激活后的参数变化仍需两次确认；没有无限轮生产模式，也没有在线学习。

命令通道另有独立共享密钥 `VARIATIONAL_COMMAND_AUTH_TOKEN`（至少 32 字符），必须与 Chrome 插件弹窗完全一致；它不是 canary 单次令牌。插件升级后必须清除旧模板缓存并重新确认多/空开平四套订单与报价模板全部就绪。当前发布阶段只验收 macOS，Windows 整合包在 macOS 完整实盘跑通前保持冻结。
