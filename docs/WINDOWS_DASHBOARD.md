# Windows 10/11：安全打开服务器网页面板

Windows 在本项目中只充当运维面板客户端。Runtime、Chrome Bridge、钱包和交易服务仍固定
部署在 Ubuntu 24.04 服务器；不要把交易密钥复制到 Windows，也不要在公网开放 8780。

## 1. 安装 OpenSSH Client

以管理员身份打开 PowerShell：

```powershell
Get-WindowsCapability -Online | Where-Object Name -like 'OpenSSH.Client*'
Add-WindowsCapability -Online -Name OpenSSH.Client~~~~0.0.1.0
ssh -V
```

如果第一条显示 `State : Installed`，无需重复安装。微软官方说明见
[OpenSSH 安装文档](https://learn.microsoft.com/windows-server/administration/openssh/openssh_install_firstuse)。

## 2. 建立 SSH 别名

在 `%USERPROFILE%\.ssh\config` 写入：

```sshconfig
Host var-lit
    HostName <服务器公网IP或域名>
    User <购买服务器时创建的SSH用户>
    IdentityFile ~/.ssh/<你的私钥文件>
    IdentitiesOnly yes
    ServerAliveInterval 15
    ServerAliveCountMax 6
```

私钥只保存在当前 Windows 用户的 `.ssh` 目录。腾讯云防火墙只允许你当前可信公网 IP 的
`TCP/22`，不要设置 `0.0.0.0/0`，也不要开放 8780。先验证：

```powershell
ssh var-lit
```

成功后输入 `exit` 返回 Windows。

## 3. 一条命令打开面板

在仓库目录运行签名策略仅作用于当前 PowerShell 进程，然后启动随仓库提供的脚本：

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\clients\windows\Open-VarLitDashboard.ps1
```

脚本会：

1. 使用 `ssh.exe` 建立 `18780 → 服务器 127.0.0.1:8780` 隧道；
2. 等待本地端口实际可用；
3. 打开 `http://127.0.0.1:18780`；
4. 保持窗口直到按 Enter，再关闭该次 SSH 隧道。

自定义别名或本地端口：

```powershell
.\clients\windows\Open-VarLitDashboard.ps1 -SshHost my-server -LocalPort 28780
```

脚本不连接交易 API、不加载 `.env`、不发送订单，也不会自动恢复新开仓。面板内的实盘操作
仍受权威快照和 60 秒二次确认保护。

## 4. 不使用脚本时

在一个 PowerShell 窗口保持：

```powershell
ssh -N -T -o ExitOnForwardFailure=yes -L 18780:127.0.0.1:8780 var-lit
```

再用浏览器打开 `http://127.0.0.1:18780`。关闭该 PowerShell 或按 `Ctrl+C` 后隧道即断开，
服务器 Runtime 不受影响。

## 5. 常见问题

- **Connection refused（本地 18780）**：服务器 Runtime 尚未监听 8780。SSH 登录后执行
  `sudo systemctl status var-lit-v1-runtime.service`。
- **Connection closed / timed out（22 端口）**：核对当前公网 IP、腾讯云防火墙 22 规则、
  VPN 出口和 SSH 用户/密钥。VPN 出口变化时，防火墙白名单也要更新。
- **本地 18780 已被占用**：使用 `-LocalPort 28780`，浏览器打开对应端口。
- **页面关闭或 VNC 关闭后担心 Runtime 停止**：网页面板、VNC 和 Runtime 是独立服务；
  关闭客户端只断开查看通道，不会停止 systemd Runtime。
- **不要做的事**：不要把 8780、5900 或 Chrome 调试端口开放公网；不要把 `.ssh` 私钥、
  钱包助记词或 Lighter 凭证放进仓库。
