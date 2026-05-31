---
title: "Polymarket 量化交易实战：从零开发跟单 Bot"
url: "https://zhuanlan.zhihu.com/p/2003971118399251558"
source: "知乎 (Zhihu) / 登链社区 LearnBlockchain.cn"
date: "2025-01-01"
type: "technical"
theme: "onchain"
lang: "zh"
---

# Polymarket 量化交易实战：从零开发跟单 Bot

## Polymarket 混合架构

Polymarket 并非纯链上 DEX，采用混合架构：
- **链下 CLOB（Off-chain order book）**：订单提交、撮合在链下服务器完成
- **链上结算（On-chain Settlement）**：交易数据提交到 Polygon 区块链上的 CTF Exchange 合约进行原子结算

## 跟单 Bot 核心逻辑

跟单 Bot 的核心——通过 Polymarket Data API 查询用户活动，监听目标地址的交易动作。

**执行推荐：FAK（Fill-And-Kill）市价单**
- 立即吃掉盘口流动性
- 未成交部分自动撤销
- 不挂单暴露意图

## 关键技术要点

1. **SDK 选择**：强烈建议使用官方 TypeScript SDK `@polymarket/clob-client`，不要手搓签名逻辑
2. **去重**：使用 `transactionHash` 作为唯一键，在内存中维护 `Set<string>` 记录已处理 Hash，防止同一笔交易被重复跟单
3. **Proxy Wallet 机制**：Polymarket 引入 Proxy Wallet 实现免 Gas 交易，开发者需搞清楚 `SIGNATURE_TYPE`

## Bot 进化历程

### V1 — 朴素轮询
- 定时器驱动，轮询大户地址，看到买单就跟
- 问题：轮询延迟固定存在、市价跟单滑点不可控

### V2 — 事件驱动（Pull → Push）
- 从"定时器驱动"变为"事件驱动"
- 降低延迟
- 新问题：本地订单簿与实时订单簿不同步

## 市场结构洞察（来自 Hubble 对 146 万地址的审计）

- 仅 **3.7% 的高频账号**贡献了 **37.44%** 的交易量
- "高成交量"往往反映做市商博弈而非真实共识
- 真人标准：参与市场不超过 300 个，持仓时间通常大于 1 小时
- 只有剔除 <10 分钟的算法刷量后，才能从机器噪音中剥离出真正的 Smart Money 信号

## Polymarket 数据结构陷阱

数据结构极为复杂：买入、分裂、合并、赎回等多种链上操作。
- 许多工具（甚至官网）如果计算维度选错，PNL 可能偏差数倍
- 真正的聪明钱 PNL 应基于**事件维度**，综合流入流出及当前持仓市值来穿透计算

## 量化套利机器人主要策略

1. **数学平价套利**：当 Yes+No 总成本 <$1 时，同时买入两侧，无风险套利
2. **极短期加密货币波动市场**：BTC/ETH 5分钟和15分钟预测市场，极端行情时易产生价格错位
3. **数字做市商**：高频双向挂单赚取价差

## 套利市场规模

- April 2024 – April 2025：套利者从 Polymarket 提取超过 **$4,000 万**
- 2026 年：73% 的套利利润被 <100ms 执行的机器人捕获

## 安全警告

"ClawHavoc" 供应链攻击：黑客在插件市场植入恶意插件，盗取浏览器 Cookie、API Key 和本地钱包私钥。主要针对 24 小时运行智能体的机器。

## 主流工具汇总

- **Polygun**：Telegram 跟单机器人，已收购 Polymarket Analytics
- **Kreo**：Telegram 机器人；覆盖 Polymarket + Kalshi；每日亏损上限+止损规则
- **PolyHub（Hubble）**：聪明钱地址识别 + 跟单工具
- **Oddpool**：预测市场"彭博社"；聚合 Polymarket、Kalshi、CME 等实时赔率和套利机会
