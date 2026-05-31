---
title: "去中心化预测市场Augur简介"
url: "https://www.imooc.com/article/282567"
source: imooc.com
date: "2020-01-01"
type: article
theme: depm
lang: zh
---

# 去中心化预测市场Augur简介

**来源:** 慕课手记 / 8btc.com  
**参考链接:**  
- https://www.imooc.com/article/282567  
- https://blog.csdn.net/gitblog_00029/article/details/139037356  
- https://www.8btc.com/p/augur  
**语言:** 中文

## 概述

Augur 是一个基于以太坊区块链技术的去中心化预测市场平台。用户可以用数字货币进行预测和下注，依靠群众的智慧来预判事件的发展结果，有效消除对手方风险和服务器的中心化风险，同时采用加密货币创建出一个全球性的市场。

## 核心理念

Augur 的目标是通过集体智慧来进行预测——这是预测市场理论的区块链实现。用户创建预测市场，对各种事件进行投注：体育赛事的结果、政治选举的赢家、天气变化等。

## 预言机（Oracle）机制

Augur 允许用户输入一个 Web URL 地址作为最终的结果来源，创建"预言机"（Oracle）——Augur 因此是联系区块链世界和真实世界的预言机平台。REP 代币持有者通过质押 REP 参与事件报告流程，获得诚实报告的费用激励。

## 技术架构（三层设计）

Augur UI 的架构由三个独立的层次组成：
1. **底层智能合约**：以太坊上的一系列智能合约，负责处理所有预测市场逻辑（创建/参与/结算市场）
2. **中间服务层**：连接智能合约与用户界面
3. **上层 Web UI**：用户交互界面

## Augur-Core 的核心优势

- **去中心化**：所有交易都在以太坊上进行，无第三方干预，保证公平性
- **安全性强**：智能合约经过严格设计，防止欺诈和操纵
- **易用性高**：Docker 集成简化了开发环境搭建
- **可扩展性**：支持合约迭代升级，适应不断变化的需求

## 生态扩展：Catnip Exchange

Catnip Exchange 属于 AugurDAO 中的一个项目，基于 Aragon 之上的去中心化自治社区组织，为 Augur 提供订单薄和 AMM 机制的预测市场交易平台。

## 挑战与局限

传统中心化预测市场问题：交易额受限、费用高、访问封闭、不透明。去中心化预测市场提供了非托管和无需允许的解决方案，但使用门槛高、用户体验差，一直无法被普通用户广泛采用。

## 关联项目

- **Gnosis**（以太坊，Conditional Token Framework）
- **Polymarket**（Polygon，使用 Gnosis CTF）
- **Zeitgeist**（Polkadot，融入 futarchy 治理）
