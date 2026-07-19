# 策略研究数据库

`strategy_research.sqlite3` 是回测、复盘和新策略反事实测试的唯一长期数据源。
旧版行情、旧执行链路、当前实盘样本、运行会话和完整轮次均按内容哈希去重。

`VARIATIONAL_RUNTIME_DIR` 只保留主程序恢复持仓、滚动一小时窗口和短期诊断
所需的有限文件，不再作为长期样本仓库。

## 持续同步

正式主程序每秒在后台同步一次新增完整行。同步任务只读取日志并批量写SQLite，
不进入策略判断、Firm Quote、Commit或Lighter下单链路；数据库故障也不会暂停交易。

当前进程尚未包含同步任务时，可临时跟随它：

```bash
venv/bin/python tools/sync_research_database.py --follow-pid <主程序PID>
```

手动补同步并查看统计：

```bash
venv/bin/python tools/sync_research_database.py --once --stats
```

## 容量

默认上限为900MiB，可在 `.env` 调整：

```dotenv
RESEARCH_DATABASE_ENABLED=true
RESEARCH_DATABASE_MAX_MIB=900
RESEARCH_DATABASE_SYNC_SECONDS=1
```

数据库和运行日志可以放在项目目录之外。例如：

```dotenv
VARIATIONAL_RUNTIME_DIR=/absolute/path/var-lit-v1-local/runtime
RESEARCH_DATABASE_FILE=/absolute/path/var-lit-v1-local/research/strategy_research.sqlite3
```

真实 `.env` 保留在本机且由 Git 忽略；GitHub 只提交不含凭证的
`.env.example`。

达到上限时，数据库会先删除最旧的非固定执行追踪和运行日志，再清理最旧行情。
历史基准样本标记为固定数据，不参与自动删除。SQLite页缓存约8MiB；900MiB指
磁盘上限，不是RAM占用。

旧模型JSON中的 `calibrationDataset` 路径作为模型哈希的一部分保持原样，避免
改动已封存模型；对应文件内容已经以固定事件写入本数据库，运行时不会读取旧路径。

## 数据表

主要表为 `research_events`：

- `stream`：`strategy_market_sample`、`order_metric`、`execution_trace`、
  `normalized_round`、`normalized_execution`、`runtime_log`等。
- `event_time_ms`：统一后的事件时间。
- `source`：原始来源。
- `pinned`：是否为不可自动清理的历史样本。
- `event_key`：内容哈希去重键。
- `payload_json`：完整原始或标准化JSON。

每次完整平仓后还会自动更新：

- `research_rounds`：从最终双腿成交自动配对的整轮数据，包含整轮PnL、
  开平仓执行损耗以及bps口径。
- `round_quality_labels`：不修改原始数据的质量标签。
- `research_round_quality`：合并人工和自动标签后的查询视图。

自动规则将“整轮执行损耗不低于2bps”标记为
`suspected_bad_execution`，只代表需要复核，不等同于策略本身失败。
用户人工确认的 `bad_execution` 优先级最高；个人轮次编号和复盘结论只保存在
外部研究数据库，不写入可公开上传的仓库文档。

尚未平仓时也可以直接标记已经成交的开仓腿，平仓后整轮会自动继承：

```bash
venv/bin/python tools/label_research_trade.py \
  --trade-key '<开仓trade key>' \
  --phase open \
  --label bad_execution \
  --note '人工确认的不良开仓'
```

人工复核后可以补充或纠正标签：

```bash
venv/bin/python tools/label_research_round.py \
  --open-trade-key '<开仓trade key>' \
  --close-trade-key '<平仓trade key>' \
  --label bad_execution \
  --note '人工复盘说明'
```

示例：

```bash
sqlite3 /path/from/RESEARCH_DATABASE_FILE \
  "SELECT stream, COUNT(*) FROM research_events GROUP BY stream;"
```

回测读取 `strategy_market_sample`；整轮表现读取 `research_round_quality`；
追查不良成交时联查 `order_metric` 和 `execution_trace`。
