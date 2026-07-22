# Changelog

## Public server parity

- 公开仓库同步当前服务器 Runtime、`adaptive-median-v6`、`execution-survival-v2`、开仓对冲
  恢复、持仓时间与当日统计、Variational 优先刷新和运维面板最新版。
- 删除 v1–v5 旧策略实现、旧分析脚本和只验证旧实现的测试；现行协议 schema 名称保留。
- 新增从腾讯云购机、防火墙、SSH、Chrome、钱包到 systemd、observe/live、升级和崩溃恢复
  的 Ubuntu 24.04 全流程教程。
- 新增 Windows 10/11 网页面板 SSH 客户端、系统架构、核心状态机和模型校准说明。
- 修正进程刚启动时首次账户过期刷新被冷却时间误挡的边界，并移除测试中的冗余 pytest
  依赖。

## Close hold floor v1

- 常规平仓下限在持仓未满一小时时保持原值；满一小时额外放宽`0.2bps`，之后每满
  十分钟再放宽`0.2bps`。金额随实际开仓金额等比例缩放，下一轮从基础下限重新开始。
- 操作面板在有仓时显示持仓时间，空仓时显示距上一轮双边平仓完成的时间。
- 服务器可写配置迁移到`/var/lib/var-lit-v1/runtime.env`，面板可继续原子保存参数，
  Runtime 无需获得程序目录写权限。

## Open hedge recovery v1

- 开仓 Lighter 对冲仍先使用冻结经济限价执行三次受保护 IOC。
- 受保护重试耗尽后，只对未成交余量追加一次 IOC 恢复单；恢复单仍受最新完整盘口、
  数据新鲜度和配置滑点上限约束，但不再因已经过时的冻结经济限价直接撤销 Var 开仓。
- 恢复单仍失败或结果不明确时，才按既有安全路径平掉 Var；平仓流程保持不变。

## V1

第一次公开发布。

- 发布名称、运行时、Chrome 扩展和 systemd 服务统一为 Var-Lit V1。
- 提供 BTC Variational ↔ Lighter 双边开平仓、对账、恢复和执行保护。
- 提供 observe/live 人工门控、专用 Chrome Bridge 与只读健康探针。
- 提供 Ubuntu 24.04、Xvfb、Google Chrome 和 systemd 部署模板。
- 服务器研究数据库由独立低优先级服务采集，不进入交易热路径。
- Python 3.11/3.12 与 Chrome 扩展测试进入 GitHub Actions。
