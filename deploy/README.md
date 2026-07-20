# Var-Lit V1：Ubuntu 24.04 部署

目标环境：东京、Ubuntu 24.04 LTS x86_64、2 vCPU、8 GiB RAM、80 GiB SSD，使用 SSH
密钥登录。项目目录固定为 `/opt/var-lit-v1`，外部状态目录固定为
`/var/lib/var-lit-v1`，专用 Linux 用户为 `varlit`。

服务器浏览器采用普通 Google Chrome、Xvfb 虚拟显示器和轻量 Openbox 窗口管理器。
Openbox 只负责 OKX Wallet 弹窗、文件选择器与焦点，不安装完整桌面；不需要长期打开
远程桌面。

## 1. 基础系统

先通过 SSH 登录服务器，更新系统并安装基础依赖：

```bash
sudo apt update
sudo apt install -y ca-certificates curl git python3 python3-venv xvfb x11vnc \
  openbox fonts-noto-cjk libu2f-udev
```

从 Google 官方下载当前 amd64 Chrome `.deb`，再用 `apt install ./文件名.deb` 安装。
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
```

8 GiB 机器通常不依赖 swap，但建议保留 2 GiB 应对 Chrome 短时峰值。不要使用
`--no-sandbox`、`--single-process` 或强制压缩 renderer 数量；这些参数可能降低账户安全、
浏览器稳定性或交易时钟质量。

## 2. 拉取公开仓库与 Python 环境

```bash
sudo -u varlit git clone https://github.com/Web3XiaoAn/var-lit-v1.git \
  /opt/var-lit-v1
sudo -u varlit python3 -m venv /opt/var-lit-v1/venv
sudo -u varlit /opt/var-lit-v1/venv/bin/python -m pip install \
  -r /opt/var-lit-v1/requirements.txt
```

创建私有配置：

```bash
sudo -u varlit cp /opt/var-lit-v1/deploy/server.env.example /opt/var-lit-v1/.env
sudo chmod 600 /opt/var-lit-v1/.env
sudo -u varlit nano /opt/var-lit-v1/.env
```

只填写自己的三项 Lighter 凭证。服务器模板默认：

- `STRATEGY_EXECUTION_MODE=observe`
- `STRATEGY_ORDER_NOTIONAL_USD=500`
- `RESEARCH_DATABASE_ENABLED=false`
- 运行状态写入 `/var/lib/var-lit-v1/runtime`

不要把 `.env`、Chrome profile、助记词、钱包私钥或数据库放回 Git 仓库。

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

在自己的电脑建立 SSH 隧道：

```bash
ssh -N -L 5901:127.0.0.1:5900 <购买服务器时配置的SSH用户>@服务器IP
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

至少完成一轮完整的一小时实时窗口、断线恢复、账户对账和延迟验收。服务器默认使用
`--no-dashboard`，只关闭终端绘制，不会关闭行情、策略、风控、日志或双边交易任务。

网页运维面板只监听服务器 `127.0.0.1:8780`。在 Mac 新终端建立临时 SSH 通道：

```bash
ssh -N -L 18780:127.0.0.1:8780 var-lit
```

然后在 Mac 浏览器打开 `http://127.0.0.1:18780`。页面以 200ms WebSocket 推送读取同一
Runtime 内存状态，不依赖 VNC，也不开放腾讯云公网端口。关闭本地 SSH 通道后，面板立即
无法从外部访问；不得在防火墙中开放 8780。

面板命令采用“展开预览 → 60 秒内二次确认 → 一次性提交”的流程。危险操作绑定准备时的
权威账户快照，确认前仓位、活动委托或执行状态发生变化便自动拒绝。参数保存只写入
`.env`，当前进程不热更新，重启 Runtime 后才生效。

Python 异常退出只重启 runtime，不连带重启 Chrome；正常 `systemctl stop` 或 Ctrl+C 会被
识别为优雅停机，不会误报关键任务故障。

## 7. live 人工批准点

确认 observe 验收通过后：

```bash
sudo systemctl stop var-lit-v1-runtime.service
sudo -u varlit nano /opt/var-lit-v1/.env
# 把 STRATEGY_EXECUTION_MODE=observe 改为 live
sudo systemctl start var-lit-v1-runtime.service
```

再次核对启动日志中的账户、金额、构建、模式和仓位。程序不会自动从 observe 切到 live。

## 运维顺序

- 启动：`var-lit-v1-display` → `var-lit-v1-window-manager` → `var-lit-v1-chrome` →
  `var-lit-v1-runtime`。
- 停止 runtime 不需要停止 Chrome；保留浏览器会话可减少重新验证和模板恢复。
- Chrome 或 OKX 更新后，先回到一次性 VNC 流程确认登录和扩展状态，再恢复 runtime。
- `.env`、运行目录和 Chrome profile 都在 Git 之外；升级代码不会覆盖它们。
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
