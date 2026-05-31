---
title: "2026年Polymarket交易机器人全景：技术架构与生产实践"
url: "https://dev.to/nevosaynevo/2026-nian-polymarket-jiao-yi-ji-qi-ren-quan-jing-ji-zhu-jia-gou-yu-sheng-chan-shi-jian-ci7"
source: "dev.to"
date: "2026-05-30"
type: "overview"
theme: "tools"
lang: "zh"
---

# 2026年Polymarket交易机器人全景

## 官方Python SDK

### py-clob-client（归档，已停止维护）

迁移到新统一SDK: https://github.com/Polymarket/py-sdk

```bash
pip install py-clob-client
```

```python
from py_clob_client.client import ClobClient
HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
client = ClobClient(HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID)
client.set_api_creds(client.create_or_derive_api_creds())
```

### py-clob-client-v2（当前版本）

新统一SDK：`Polymarket/py-sdk`

```python
from py_clob_client_v2 import ClobClient, OrderArgs, OrderType, Side
client = ClobClient(host=host, chain_id=137, key=os.environ["PK"])
creds = client.create_or_derive_api_key()
resp = client.create_and_post_order(
    order_args=OrderArgs(token_id="...", price=0.4, side=Side.BUY, size=100),
    order_type=OrderType.GTC,
)
```

### polymarket-us-python

官方Polymarket US API Python SDK。使用Ed25519签名认证。API密钥在polymarket.us/developer生成。支持事件、市场、订单薄、BBO、WebSocket（私有+公共流）。

## 开源交易机器人

### Polymarket/agents（官方AI交易框架）

模块化架构：
- `Gamma.py`：`GammaMarketClient`类，获取解析市场和事件元数据
- `Polymarket.py`：`Polymarket`类，API交互、市场数据检索、交易执行
- `cli.py`：主要用户界面

### sdohuajia/polymarket-bot

专为BTC 15分钟涨跌市场设计，利用WebSocket获取实时价格，根据预设价差条件自动执行买入止损，集成Web可视化控制面板。建议Python 3.8+，推荐在海外服务器运行。

GitHub: https://github.com/sdohuajia/polymarket-bot

### WrBug/PolyHermes（中文跟单系统）

功能强大的Polymarket预测市场跟单交易系统，支持：
- 自动化跟单
- 多账户管理
- 实时订单推送
- 统计分析
- Docker一键部署

GitHub: https://github.com/WrBug/PolyHermes

## 官方GitHub组织

https://github.com/polymarket — 100个开源仓库，包括py-sdk、ts-sdk、py-clob-client、polymarket-cli等。

## 注意事项

Polymarket服务条款禁止美国及某些司法管辖区的用户通过UI和API（包括受限区域开发的Agent）进行交易，但全球可查看数据和信息。
