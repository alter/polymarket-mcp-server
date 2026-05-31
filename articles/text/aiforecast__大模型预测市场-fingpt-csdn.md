---
title: "FinGPT 开源股票预测大模型及金融 LLM 预测综述"
url: "https://zhuanlan.zhihu.com/p/1897811578432227248"
source: "zhihu"
date: "2025-01-01"
type: "article"
theme: "aiforecast"
lang: "zh"
---

# FinGPT 开源股票预测大模型及金融 LLM 预测综述

**来源：** 知乎 / CSDN（中文技术社区）

## 主要内容

**FinGPT（15.9K stars GitHub）：**
- 以数据为中心的金融大语言模型（LLM）开发框架
- FinGPT-Forecaster：综合分析公司并预测下周股价走势
- 涵盖：金融报表解读、数据分析、股票及量化交易
- 开源，适合研究者和从业者使用

**FinArena（人机协作投资框架）：**
- 强调复杂投资问题中的人机协作
- 通过"报告代理"（Report Agent）为投资者提供风险偏好接口
- 将投资者风险偏好与模型输出结合，生成个性化投资建议
- 多智能体协作：TimeGPT（时间序列）+ LLaMA/GPT（文本分析）

**RAG 增强的股票预测：**
- 新型 RAG 框架解决传统方法无法发现数据背后语义联系的问题
- 提高股票走势预测精度和可靠性

## 与 Polymarket 交易的关联

FinGPT 的开源框架可以直接适配到 Polymarket 数据上，用于训练面向预测市场的 LLM 预测模型。RAG 增强方法（先检索相关新闻，再进行概率估计）是中文社区验证的有效范式。
