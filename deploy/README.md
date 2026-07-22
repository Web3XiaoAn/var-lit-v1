# Var-Lit V1：Ubuntu 24.04 部署

目标环境：东京、Ubuntu 24.04 LTS x86_64、2 vCPU、8 GiB RAM、80 GiB SSD，使用 SSH
密钥登录。项目目录固定为 `/opt/var-lit-v1`，外部状态目录固定为
`/var/lib/var-lit-v1`，专用 Linux 用户为 `varlit`。

服务器浏览器采用普通 Google Chrome、Xvfb 虚拟显示器和轻量 Openbox 窗口管理器。
Openbox 只负责 OKX Wallet 弹窗、文件选择器与焦点，不安装完整桌面；不需要长期打开
远程桌面。

> 本文中的 `<SSH用户>`、`<服务器IP>`、`<你的公网IP>` 和密钥路径都必须替换成自己的值。
> 不要复制他人的 IP、账户或密钥。腾讯云官方入口：
> [购买轻量应用服务器](https://cloud.tencent.com/document/product/1207/44580)、
> [套餐说明](https://cloud.tencent.com/document/product/1207/44755)、
> [防火墙](https://cloud.tencent.com/document/product/1207/44577)、
> [SSH 密钥](https://cloud.tencent.com/document/product/1207/44573)。

## 0. 购买与网络安全

### 0.1 购买什么

在腾讯云“轻量应用服务器”购买一台实例，当前单 BTC Runtime 的参考起点为：

| 项目 | 选择 |
|---|---|
| 地域 | 东京（服务器创建后不能直接改地域） |
| 镜像 | Ubuntu 24.04 LTS x86_64 |
| CPU / 内存 | 2 vCPU / 8 GiB |
| 系统盘 | SSD 80 GiB |
| 公网带宽 | 200 Mbps 套餐或当前可选的相近规格 |
| 登录 | 优先 SSH 密钥，不使用弱密码 |

这是当前实测环境的配置记录，不是性能保证。新资产、更多浏览器或更大研究库需要重新测量
CPU、内存、磁盘和执行延迟。购买后记录公网 IPv4，但不要把它写入 Git。

### 0.2 创建 SSH 密钥

可在腾讯云控制台创建并绑定密钥，也可使用已有 Ed25519 密钥。在自己的 Mac/Linux：

```bash
ssh-keygen -t ed25519 -a 64 -f ~/.ssh/var-lit -C var-lit
chmod 600 ~/.ssh/var-lit
```

私钥 `~/.ssh/var-lit` 只留在自己的电脑；只把 `.pub` 公钥导入腾讯云。Windows 用户见
[`../docs/WINDOWS_DASHBOARD.md`](../docs/WINDOWS_DASHBOARD.md)。

### 0.3 防火墙只允许可信 IP

先在自己的电脑查看 **当前出口 IPv4**，VPN 开关会改变结果：

```bash
curl -4 https://ifconfig.me; echo
```

腾讯云实例“防火墙”只添加：

| 来源 | 协议 | 端口 | 策略 |
|---|---|---:|---|
| `<你的公网IP>/32` | TCP | 22 | 允许 |

删除 `0.0.0.0/0 → TCP/22` 的宽泛规则。项目不需要公网 80/443/8780/5900/9222；面板、
VNC 和 Chrome 调试都走 SSH 隧道。VPN 出口变化时，先在控制台把新 IP 加入白名单并验证
新 SSH 会话，再删除旧规则，避免把自己锁在服务器外。

### 0.4 首次 SSH 与本机别名

镜像的初始 SSH 用户以控制台提示为准，常见为 `ubuntu`：

```bash
ssh -i ~/.ssh/var-lit <SSH用户>@<服务器IP>
```

成功后在自己电脑 `~/.ssh/config` 写入：

```sshconfig
Host var-lit
    HostName <服务器IP>
    User <SSH用户>
    IdentityFile ~/.ssh/var-lit
    IdentitiesOnly yes
    ServerAliveInterval 15
    ServerAliveCountMax 6
```

以后只需 `ssh var-lit`。在保持首个会话在线时，另开终端验证别名，确认无误后再继续。

### 0.5 服务器 SSH 与 UFW

Ubuntu 官方 OpenSSH 与 UFW 说明分别见
[OpenSSH server](https://documentation.ubuntu.com/server/how-to/security/openssh-server/) 和
[防火墙](https://documentation.ubuntu.com/server/how-to/security/firewalls/)。腾讯云防火墙是第一层，
UFW 是实例内第二层：

```bash
sudo apt update
sudo apt install -y openssh-server ufw
sudo systemctl enable --now ssh
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow from <你的公网IP> to any port 22 proto tcp
sudo ufw enable
sudo ufw status numbered
```

启用 UFW 前必须保留当前 SSH 会话，并用第二个终端验证新连接。确认密钥登录成功后，可在
`/etc/ssh/sshd_config.d/60-var-lit.conf` 设置 `PasswordAuthentication no` 和
`PermitRootLogin no`，执行 `sudo sshd -t` 无输出后再 `sudo systemctl reload ssh`。不要在
未验证密钥前禁用密码登录。

## 1. 基础系统

先通过 SSH 登录服务器，更新系统并安装基础依赖：

```bash
sudo apt update
sudo apt install -y ca-certificates curl git python3 python3-venv xvfb x11vnc \
  openbox fonts-noto-cjk libu2f-udev
```

从 [Google 官方 Linux 安装页](https://support.google.com/chrome/a/answer/9025926) 下载当前
amd64 Chrome `.deb`，再让 `apt` 安装本地包与依赖：

```bash
curl -fLO https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install -y ./google-chrome-stable_current_amd64.deb
rm google-chrome-stable_current_amd64.deb
```

安装后必须确认：

```bash
google-chrome-stable --version
python3 --version
Xvfb -help >/dev/null
```

创建专用用户和目录：

```bash
sudo useradd --create-home --shell /bin/bash varlit
sudo install -d -o varlit -g varlit -m 0750 /opt/var-lit-v1
sudo install -d -o varlit -g varlit -m 0750 /var/lib/var-lit-v1
sudo install -d -o varlit -g varlit -m 0750 /var/lib/var-lit-v1/runtime
sudo install -d -o varlit -g varlit -m 0750 /var/lib/var-lit-v1/research
```

8 GiB 机器通常不依赖 swap，但建议保留 2 GiB 应对 Chrome 短时峰值。不要使用
`--no-sandbox`、`--single-process` 或强制压缩 renderer 数量；这些参数可能降低账户安全、
浏览器稳定性或交易时钟质量。

没有 swap 时可一次性创建：

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
swapon --show
```

## 2. 拉取公开仓库与 Python 环境

```bash
sudo -u varlit git clone https://github.com/Web3XiaoAn/var-lit-v1.git \
  /opt/var-lit-v1
sudo -u varlit python3 -m venv /opt/var-lit-v1/venv
sudo -u varlit /opt/var-lit-v1/venv/bin/python -m pip install \
  -r /opt/var-lit-v1/requirements.txt
```

创建项目目录之外的私有配置；这样 Runtime 只能原子更新参数文件，不能改写程序目录：

```bash
sudo -u varlit cp /opt/var-lit-v1/deploy/server.env.example \
  /var/lib/var-lit-v1/runtime.env
sudo chmod 600 /var/lib/var-lit-v1/runtime.env
sudo -u varlit nano /var/lib/var-lit-v1/runtime.env
```

只填写自己的三项 Lighter 凭证。服务器模板默认：

- `STRATEGY_EXECUTION_MODE=observe`
- `STRATEGY_ORDER_NOTIONAL_USD=500`
- `RESEARCH_DATABASE_ENABLED=false`
- 运行状态写入 `/var/lib/var-lit-v1/runtime`

不要把 `runtime.env`、Chrome profile、助记词、钱包私钥或数据库放回 Git 仓库。

## 3. 安装 systemd 模板

```bash
sudo install -m 0644 \
  /opt/var-lit-v1/deploy/systemd/var-lit-v1-display.service.example \
  /etc/systemd/system/var-lit-v1-display.service
sudo install -m 0644 \
  /opt/var-lit-v1/deploy/systemd/var-lit-v1-window-manager.service.example \
  /etc/systemd/system/var-lit-v1-window-manager.service
sudo install -m 0644 \
  /opt/var-lit-v1/deploy/systemd/var-lit-v1-chrome.service.example \
  /etc/systemd/system/var-lit-v1-chrome.service
sudo install -m 0644 \
  /opt/var-lit-v1/deploy/systemd/var-lit-v1-runtime.service.example \
  /etc/systemd/system/var-lit-v1-runtime.service
sudo install -m 0644 \
  /opt/var-lit-v1/deploy/systemd/var-lit-v1-research.service.example \
  /etc/systemd/system/var-lit-v1-research.service
sudo systemctl daemon-reload
sudo systemctl enable --now var-lit-v1-display.service
sudo systemctl enable --now var-lit-v1-window-manager.service
sudo systemctl enable --now var-lit-v1-chrome.service
sudo systemctl enable --now var-lit-v1-research.service
```

`deploy/research-retention.conf` 是研究阶段才使用的 12 GiB systemd drop-in，不应默认安装。
标准部署保留 service 模板的 900 MiB 上限。只有明确需要更长研究样本、确认磁盘空间充足并
接受增长后，才复制为 `/etc/systemd/system/var-lit-v1-research.service.d/retention.conf`。

此时不要启动 runtime。先检查浏览器服务：

```bash
systemctl status var-lit-v1-display.service var-lit-v1-window-manager.service \
  var-lit-v1-chrome.service
journalctl -u var-lit-v1-chrome.service -n 100 --no-pager
```

Chrome 的调试端口只绑定 `127.0.0.1`，Xvfb 禁止 TCP 监听。
Xvfb 与 Chrome 默认使用 `1920x1080`，避免 OKX 钱包导入窗口超出虚拟屏幕；Openbox
必须在 Chrome 之前启动，否则扩展弹窗可能被放到屏幕外。

## 4. 一次性钱包与浏览器初始化

先创建仅供初始化使用的 VNC 密码（不要复用钱包、SSH 或交易凭证）：

```bash
sudo -u varlit x11vnc -storepasswd /var/lib/var-lit-v1/x11vnc.pass
```

在服务器另开一个 SSH 会话，仅在初始化期间启动本地 VNC：

```bash
sudo -u varlit x11vnc -display :99 -localhost -forever -shared -rfbport 5900 \
  -rfbauth /var/lib/var-lit-v1/x11vnc.pass
```

保持服务器端 `x11vnc` 前台窗口运行，在自己的电脑建立 SSH 隧道：

```bash
ssh -N -L 5901:127.0.0.1:5900 <SSH用户>@<服务器IP>
```

用本机 VNC 客户端连接 `127.0.0.1:5901`。不要把 5900 或 5901 端口开放到公网。然后由账户
持有人本人完成：

1. 在 Chrome Web Store 安装 OKX Wallet 并解锁。
2. 在 `chrome://extensions` 开启开发者模式，加载
   `/opt/var-lit-v1/chrome_extension`。
3. 登录 Variational BTC 永续页并完成钱包验证。
4. 打开 Var-Lit V1 Bridge，启用自动绑定，并按状态提示完成 500U 模板采集。

完成后结束 `x11vnc`，关闭 SSH 隧道；Xvfb 与 Chrome 服务继续运行。Chrome profile 位于
`/var/lib/var-lit-v1/chrome-profile`，不得提交或复制到公开位置。

## 5. 只读验收

```bash
sudo -H -u varlit /opt/var-lit-v1/venv/bin/python \
  /opt/var-lit-v1/tools/check_host_readiness.py --phase server \
  --chrome-profile /var/lib/var-lit-v1/chrome-profile
sudo -H -u varlit /opt/var-lit-v1/venv/bin/python \
  /opt/var-lit-v1/tools/probe_extension.py --duration 60
```

必须确认：

- 主机检查无失败项。
- 三个扩展本地通道均就绪。
- 扩展构建为 `var-lit-v1`，`orders_sent` 为 `0`。
- 模板金额、钱包地址与目标账户正确。
- Variational 与 Lighter 双边空仓、无活动委托、无未决意图。

## 6. observe 运行

```bash
sudo systemctl enable --now var-lit-v1-runtime.service
journalctl -u var-lit-v1-runtime.service -f
```

至少完成一轮完整的一小时实时窗口、网页刷新恢复、账户对账和延迟验收。服务器默认使用
`--no-dashboard`，只关闭终端绘制，不会关闭行情、策略、风控、日志或双边交易任务。

网页运维面板只监听服务器 `127.0.0.1:8780`。在 Mac/Linux 新终端建立临时 SSH 通道：

```bash
ssh -N -L 18780:127.0.0.1:8780 var-lit
```

然后在 Mac 浏览器打开 `http://127.0.0.1:18780`。页面以 200ms WebSocket 推送读取同一
Runtime 内存状态，不依赖 VNC，也不开放腾讯云公网端口。关闭本地 SSH 通道后，面板立即
无法从外部访问；不得在防火墙中开放 8780。

面板命令采用“展开预览 → 60 秒内二次确认 → 一次性提交”的流程。危险操作绑定准备时的
权威账户快照，确认前仓位、活动委托或执行状态发生变化便自动拒绝。参数保存只写入
`/var/lib/var-lit-v1/runtime.env`，当前进程不热更新，重启 Runtime 后才生效。

Windows 10/11 使用方法与一键 PowerShell 客户端见
[`../docs/WINDOWS_DASHBOARD.md`](../docs/WINDOWS_DASHBOARD.md)。

Python 异常退出只重启 runtime，不连带重启 Chrome；正常 `systemctl stop` 或 Ctrl+C 会被
识别为优雅停机，不会误报关键任务故障。

## 7. live 人工批准点

确认 observe 验收通过后：

```bash
sudo systemctl stop var-lit-v1-runtime.service
sudo -u varlit nano /var/lib/var-lit-v1/runtime.env
# 把 STRATEGY_EXECUTION_MODE=observe 改为 live
sudo systemctl start var-lit-v1-runtime.service
```

再次核对启动日志中的账户、金额、构建、模式和仓位。程序不会自动从 observe 切到 live。

启用 live 前至少逐项确认：

- `runtime.env` 中金额、方向门槛、磨损下限和 Lighter 三项凭证均属于自己的账户；
- Variational/Lighter 双边权威仓位相等且方向相反，空仓时均为 0；
- 活动订单为 0，命令/REST/WebSocket 三通道就绪；
- Variational 网页在线、钱包已解锁、订单模板金额正确；
- 面板仅能经 SSH 隧道访问，8780/5900/9222 没有公网规则；
- 已理解程序不承诺收益，安全恢复也可能产生真实磨损。

## 安全升级代码

不要在持仓或存在活动订单时直接覆盖 Runtime。先在面板暂停新开仓，等待本轮结束，然后：

```bash
cd /opt/var-lit-v1
sudo -u varlit git fetch --prune origin
sudo -u varlit git status --short
sudo -u varlit git checkout main
sudo -u varlit git pull --ff-only origin main
sudo -u varlit venv/bin/python -m pip install -r requirements.txt
sudo -u varlit venv/bin/python -m unittest discover -s tests -p 'test_*.py'
sudo systemctl restart var-lit-v1-runtime.service
```

`git status --short` 必须为空；不为空时先查清本地修改，不要用 `reset --hard` 覆盖。更新后：

```bash
sudo systemctl is-active var-lit-v1-runtime.service var-lit-v1-research.service
sudo journalctl -u var-lit-v1-runtime.service -n 120 --no-pager
sudo -u varlit /opt/var-lit-v1/venv/bin/python \
  /opt/var-lit-v1/tools/probe_extension.py --duration 60
```

重启前后的停机时间应尽量短，但绝不能为了时间跳过空仓、活动订单和恢复状态核对。策略
更新是否重置面板批次与信号窗口取决于版本迁移；权威成交证据和研究数据库不应因刷新
浏览器或普通 Runtime 重启而删除。

## 崩溃与重启恢复

服务器重启后，已 `enable` 的 display、Openbox、Chrome、research 和 Runtime 会由 systemd
按依赖启动。检查顺序：

```bash
sudo systemctl status var-lit-v1-display.service \
  var-lit-v1-window-manager.service var-lit-v1-chrome.service \
  var-lit-v1-research.service var-lit-v1-runtime.service
sudo journalctl -u var-lit-v1-runtime.service -b -n 200 --no-pager
```

如果 Chrome 页面掉线，优先使用面板“优先刷新 Var”，然后重新读取权威账户、活动订单和
成交记录；刷新不等于订单未成交。只有浏览器服务确实退出才执行：

```bash
sudo systemctl restart var-lit-v1-chrome.service
```

只有 Runtime 退出才执行：

```bash
sudo systemctl restart var-lit-v1-runtime.service
```

不要把“重启全部服务”当作通用修复。遇到未知活动订单、单边仓位或未决 Commit 时，保持
新开仓暂停，先查真实账户再使用受保护的对账/恢复动作。

## 运维顺序

- 启动：`var-lit-v1-display` → `var-lit-v1-window-manager` → `var-lit-v1-chrome` →
  `var-lit-v1-runtime`。
- 停止 runtime 不需要停止 Chrome；保留浏览器会话可减少重新验证和模板恢复。
- Chrome 或 OKX 更新后，先回到一次性 VNC 流程确认登录和扩展状态，再恢复 runtime。
- `runtime.env`、运行目录和 Chrome profile 都在 Git 之外；升级代码不会覆盖它们。
- Runtime 内部的长期研究数据库默认关闭；独立的低优先级
  `var-lit-v1-research.service` 每5秒把完整行同步到同构SQLite，不进入交易进程。

只读资源采样：

```bash
cd /opt/var-lit-v1
sudo -u varlit venv/bin/python tools/profile_process_resources.py \
  --group python=<脚本PID> --group chrome=<Chrome主进程PID> \
  --duration 300 --interval 1 --summary-only
```

建议长期观察可用内存、swap、Chrome renderer 重启、扩展三通道状态和 Variational Commit
延迟，不要仅根据总内存占用判断浏览器是否健康。

常用命令：

```bash
# 状态
sudo systemctl is-active var-lit-v1-runtime.service var-lit-v1-research.service

# 最近日志
sudo journalctl -u var-lit-v1-runtime.service -n 120 --no-pager

# 只重启 Runtime（不会关闭 Chrome）
sudo systemctl restart var-lit-v1-runtime.service

# 数据库只读分析
sudo -H -u varlit /opt/var-lit-v1/venv/bin/python \
  /opt/var-lit-v1/tools/analyze_execution_survival.py \
  /var/lib/var-lit-v1/research/strategy_research.sqlite3 --since-hours 24
```
