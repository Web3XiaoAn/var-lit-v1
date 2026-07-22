# Var-Lit V1

Var-Lit V1 是 Variational 与 Lighter 之间的 BTC 双边执行程序。这是项目第一次公开发布，
公开版本号为 `v1`，运行时与 Chrome 扩展构建标识均为 `var-lit-v1`。

程序从专用 Chrome 中读取 Variational 行情并复用账户持有人已经捕获的网页订单模板，
通过 Lighter SDK/API 完成另一条腿的对冲。策略、账户对账、异常恢复、执行证据与浏览器桥接
都在本机或服务器本地运行。

> 这是实盘交易软件，不承诺收益。使用者必须自行验证代码、平台规则、账户权限和风险，
> 并对钱包、API 凭证、订单与资金结果负责。

## V1 已包含

- BTC 单市场、Variational ↔ Lighter 双边开平仓。
- 5 分钟、30 分钟、1 小时滚动窗口的自适应中位数门槛。
- Variational Firm Quote/Commit 与 Lighter Market IOC 对冲。
- 双边最终成交价复核、Firm 金额校验、坏价 IOC 保护和分段平仓底线。
- 仓位、活动委托与未决意图对账；异常恢复和手动 Variational 成交自动对冲。
- `observe` 只观察模式与 `live` 连续实盘模式。
- 本地 Chrome 扩展、三通道健康探针、执行延迟分析和资源采样工具。
- Ubuntu 24.04、普通 Chrome + Xvfb + 轻量 Openbox、systemd 的服务器部署模板。
- 仅监听回环地址、通过 SSH 隧道访问的 200ms 实时网页运维面板。
- 独立低优先级 SQLite 研究采集服务；Runtime 自身仍只保留约一小时滚动数据，数据库写入不进入交易热路径。

内部策略状态标识为 `adaptive-median-v6`。它是版本化公式和冻结持仓上下文的内部标识，
不代表公开产品已经发布过六版，也不能在不迁移状态的情况下改名。公开产品、运行时、
扩展和部署服务均从 Var-Lit V1 开始计版。

## 目录

- `main.py`、`execution_reserve.py`：运行入口、风控和执行预留。
- `adaptive_strategy/`：纯策略计算、模型与不可变决策对象。
- `variational/`：浏览器本地通道、Lighter 下单、遥测与研究库接口。
- `dashboard/`：服务器回环地址上的实时运维面板静态资源。
- `chrome_extension/`：Var-Lit V1 Chrome Bridge。
- `deploy/`：Ubuntu、Xvfb、Openbox、Chrome 与 systemd 部署文件。
- `tools/`：只读探针、性能分析、资源采样和研究数据工具。
- `tests/`：Python 与扩展自动测试。
- `docs/`、`research_data/README.md`：策略与研究数据说明。

真实 `.env`、Chrome profile、钱包数据、虚拟环境、日志、SQLite 数据库和运行状态都不会
进入 Git。不要强制添加这些文件。

## 文档入口

- [Ubuntu 24.04 从购机到实盘部署](deploy/README.md)：购机、防火墙、SSH、Chrome、钱包、systemd、验收、升级和故障恢复。
- [Windows 网页面板客户端](docs/WINDOWS_DASHBOARD.md)：Windows 10/11 通过 SSH 隧道安全查看服务器面板。
- [整体架构、核心逻辑与模型由来](docs/ARCHITECTURE_AND_MODELS.md)：数据流、交易状态机、风控边界和两个现行模型的校准口径。
- [当前策略公式](docs/ADAPTIVE_MEDIAN_V6.md)：`adaptive-median-v6` 与 `execution-survival-v2` 的精确定义。

## 运行要求

- macOS 或 x86_64 Linux；服务器推荐 Ubuntu 24.04 LTS。
- Python 3.11 或 3.12。
- Google Chrome 116+。
- OKX Wallet Chrome 扩展和已经完成验证的 Variational 账户。
- 自己的 Lighter API 私钥、API Key Index 与 Account Index。
- 服务器推荐 2 vCPU、8 GiB RAM、80 GiB SSD；东京节点与 SSH 密钥登录。

程序当前只接受 BTC，其他资产会在启动检查中被拒绝。

## 本机首次运行

### 1. 安装 Python 依赖

```bash
git clone https://github.com/Web3XiaoAn/var-lit-v1.git
cd var-lit-v1
python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
```

### 2. 创建私有配置

```bash
cp .env.example .env
chmod 600 .env
```

只在 `.env` 中填写自己的三项 Lighter 凭证。模板默认是安全的 `observe` 模式；运行数据
和研究数据库默认放在项目同级的 `var-lit-v1-local`，不会随 Git 上传。

主要配置：

```dotenv
STRATEGY_EXECUTION_MODE=observe
STRATEGY_ORDER_NOTIONAL_USD=200
STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT=0.05
STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT=-0.073
```

根目录 `.env` 是唯一运行配置源。重复键、缺少必要键或未知键都会拒绝启动；终端环境变量
不会覆盖交易参数。

### 3. 初始化专用 Chrome

退出使用同一专用 profile 的旧实例，然后运行：

```bash
deploy/launch_chrome.sh
```

该脚本使用独立 Chrome profile，不读取日常浏览器资料。第一次打开后：

1. 在 `chrome://extensions` 开启开发者模式。
2. 选择“加载已解压的扩展程序”，加载仓库中的 `chrome_extension`。
3. 安装并由账户持有人本人解锁 OKX Wallet、登录 Variational、完成钱包验证。
4. 在 Var-Lit V1 Bridge 中启用自动绑定，并按状态提示完成目标金额的订单模板采集。

授权头只保存在当前浏览器会话；助记词和私钥不由脚本读取或保存。Chrome 或登录状态更新
后，保持 Variational BTC 永续页登录并刷新一次，使扩展重新就绪。

### 4. 只读验证

确保 `main.py` 没有运行，再执行：

```bash
venv/bin/python tools/probe_extension.py --duration 60
venv/bin/python tools/check_host_readiness.py --phase local
```

扩展探针必须显示三个本地通道就绪，且 `orders_sent` 固定为 `0`。探针代码没有发送下单
命令的路径。

### 5. 启动 observe

```bash
venv/bin/python main.py --lang zh
```

服务器可关闭终端看板，交易与风控任务保持不变：

```bash
venv/bin/python main.py --lang zh --no-dashboard
```

首次运行必须先确认双边空仓、无活动委托、无未决意图、仓位一致，并等待实时窗口就绪。
看板中的 `Lighter 下单通道` 必须为 `WS READY`；`REST FALLBACK` 只允许平仓、恢复和
手动成交对冲，不允许策略新开仓。

### 6. 从本机查看服务器网页面板

Runtime 启动后，网页面板只监听运行机器的 `127.0.0.1:8780`，不会开放公网端口。在本机
新终端建立 SSH 本地转发：

```bash
ssh -N -L 18780:127.0.0.1:8780 var-lit
```

然后在本机浏览器打开 `http://127.0.0.1:18780`。页面通过 WebSocket 每 200ms 读取同一个
Runtime 的权威状态；关闭 SSH 命令后，本机页面就无法再访问。危险操作必须先展开权威
仓位与活动委托预览，再在 60 秒内二次确认；状态在确认前发生变化会被拒绝。

无需连接账户即可检查页面布局与按钮交互：

```bash
venv/bin/python tools/demo_operations_dashboard.py
```

演示进程只使用内存模拟数据，不加载 `.env`、Chrome、交易所客户端或订单通道。

### 7. 人工启用 live

停止程序，把 `.env` 中 `STRATEGY_EXECUTION_MODE` 改为 `live`，再重新启动。修改模式是
单独的人工批准点；程序不会自动把 `observe` 升级为 `live`。

## 核心风控口径

- 新开仓同时要求 500U 参考深度和当前目标金额 Firm 实际价差越过同一冻结门槛。
- Firm 金额必须落在“目标金额 ± 1U”闭区间；200U、500U 或其他正数都按当前配置计算。
- 开仓不把历史坏成交反向叠加到信号门槛；最终 Firm 与 Lighter IOC 仍绑定坏价保护。
- 平仓按冻结的实际 BTC 数量执行，并使用最终双边经济价格复核。
- 30 分钟前，扣除平仓 reserve 的整轮 lower bound 必须不小于 0。
- 30 分钟后，整轮 lower bound 必须不小于实际开仓金额的 `-1.0bps`。
- 盈亏统计本身不会停机；订单结果不明确、对冲失败或真实账户不一致会暂停自动化。
- 执行模式只控制新开仓；已有策略仓位仍继续平仓、恢复和对账。

## 数据边界

运行时内存只取最新一小时。行情 JSONL 至少保留 61 分钟并定期压缩，物理跨度最多约
70 分钟。服务器模板设置 `RESEARCH_DATABASE_ENABLED=false`，表示 **Runtime 进程本身**
不写长期数据库；独立的 `var-lit-v1-research.service` 以低优先级从轮转证据同步 SQLite，
避免数据库写入阻塞 Quote、Commit 或 Lighter 下单。恢复状态、成交证据和轮转日志仍会
保留。

默认数据库磁盘上限 900 MiB。也可只在本机启用同一套外部 SQLite 研究流程；后台同步
不进入 Firm Quote、Commit 或 Lighter 下单热路径。详见
[`research_data/README.md`](research_data/README.md)。

## Ubuntu 部署

东京 Ubuntu 24.04、2 vCPU、8 GiB RAM、80 GiB SSD 是当前单 BTC 实例的参考起点，
不是容量保证。完整的购机、防火墙、SSH 密钥、安装、钱包初始化、面板隧道、systemd、
升级与恢复顺序见
[`deploy/README.md`](deploy/README.md)。服务器中 Chrome 仍是普通浏览器，只是窗口显示在
Xvfb 虚拟屏幕；首次安装 OKX、登录和签名必须由账户持有人完成。

## 测试

```bash
venv/bin/python -m unittest discover -s tests -p 'test_*.py'
node --test tests/test_extension_templates.js
bash -n deploy/launch_chrome.sh deploy/run_runtime.sh
```

GitHub Actions 在 Ubuntu 24.04 上同时覆盖 Python 3.11、3.12 和扩展测试。

## 研究与资源分析

```bash
# 只读汇总数据质量、执行延迟、候选存活、盘口因子和完整轮次
venv/bin/python tools/analyze_execution_survival.py \
  /var/lib/var-lit-v1/research/strategy_research.sqlite3 --since-hours 24

# 采集 Python 与专用 Chrome 的 CPU/RSS
venv/bin/python tools/profile_process_resources.py \
  --group python=<脚本PID> --group chrome=<Chrome主进程PID> \
  --duration 300 --interval 1 --summary-only
```

统一分析器以只读方式打开研究库，不连接交易所，也不进入实盘进程。报告严格区分币种、
方向、observe/live、样本类型和策略版本，并汇总 Variational Quote/Commit、Commit 到
Lighter Ack/Fill、执行存活率、reserve、深度、book/trade flow、microprice、保护回退及
整轮收益。

当前策略公式和执行保护详见 [`docs/ADAPTIVE_MEDIAN_V6.md`](docs/ADAPTIVE_MEDIAN_V6.md)。
