# Security

## 凭证边界

- 真实 `.env`、Lighter 私钥、API 索引、钱包助记词、钱包私钥和 Chrome profile 不得提交。
- 只复制 `.env.example` 或 `deploy/server.env.example`，并把生成的 `.env` 权限设为 `0600`。
- OKX Wallet 的安装、解锁、登录和签名必须由账户持有人本人完成。
- Chrome 调试端口、扩展三个 WebSocket 端口与临时 VNC 端口只能绑定 loopback。
- 不要把运行日志或数据库直接附在公开 Issue；它们可能包含账户和成交信息。

## 报告问题

如果发现可能泄露凭证、绕过 loopback 限制、重复下单或造成未对冲仓位的问题，请停止
`var-lit-v1-runtime`，保留现场证据，并使用 GitHub 仓库的私密安全报告功能联系维护者。
报告中不要粘贴真实密钥、助记词或完整 Chrome profile。

## 实盘升级

代码更新后先运行全部测试和 `observe` 验收。只有账户、仓位、模板、扩展构建和延迟检查
全部通过后，才能由账户持有人手动切换到 `live`。
