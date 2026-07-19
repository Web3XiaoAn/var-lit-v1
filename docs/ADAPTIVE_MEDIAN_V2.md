# adaptive-median-v2 首次实盘验证公式

v2 保留 v1 的 Firm Quote、最新 Lighter 深度复核、200U 开仓硬上限、精确数量平仓、账户对账、异常恢复和单轮 canary。Chrome 命令协议没有变化。

## 窗口与开仓

5m、30m、1h 权重为 25%、45%、30%。方向 `d`：

```text
B_d     = 0.25×M5m_d   + 0.45×M30m_d   + 0.30×M1h_d
Entry_d = 0.25×Q80_5m  + 0.45×Q80_30m  + 0.30×Q80_1h
Exit_d  = 0.25×Q95_5m  + 0.45×Q95_30m  + 0.30×Q95_1h

Economic_d = -wear - Exit_opposite + reserve_open + reserve_close
Open_d     = max(Entry_d, Economic_d, Balance_d)
```

Q95只表示最近窗口中曾经可等到的有利退出机会，用来筛选是否值得开仓，不承诺未来价格。候选随后仍以同一冻结epoch、Variational Firm Quote和该数量对应的最新Lighter VWAP复核，失败就不提交。

## 平仓

中位数回归继续记录为诊断字段，但v2不把它作为第二个硬门槛。精确双边经济是唯一正常平仓条件：

```text
floor(t) = 0U,                         t < 30m
         = -1.0bps × open_notional,    t >= 30m

close only if exact_round_lower_bound >= floor(t)
```

因此增加了平仓机会，但没有提高允许亏损。120分钟只报警，不强制突破底线平仓。

## 重启续接

启动时只读取 `strategy_market_samples.jsonl` 中最新的BTC、500U/200U、内部断流小于60秒且覆盖至少一小时的连续有效rate。停机到第一帧新鲜行情的单次间隔最多允许5分钟，并明确标记为重启桥接；其他历史断流仍会拒绝。历史价格从不用于下单，首个决策必须使用重启后的新鲜同步行情和Firm Quote。

重复的开仓决策追踪按“状态变化立即记录、相同状态每秒一次心跳”合并。该调整不改变行情处理或下单链路，只减少日志队列负担。
