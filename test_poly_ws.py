import asyncio
import websockets
import json
import logging

logging.basicConfig(level=logging.INFO)

async def test():
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    print(f"Connecting to {url}")
    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            token = "28342067911960552115358272066240777232595262486860946251344574810276141891103"
            
            # Payload 1: What WebSocketManager sends
            msg_mcp = {
                "type": "subscribe",
                "channel": "market",
                "event": "agg_orderbook",
                "assets": [token]
            }
            
            # Payload 2: Official Polymarket Docs
            msg_official = {
                "assets_ids": [token],
                "type": "market"
            }
            
            print(f"Sending MCP payload: {msg_mcp}")
            await ws.send(json.dumps(msg_mcp))
            
            for _ in range(2):
                try:
                    out = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    print(f"Received JSON: {out}")
                except Exception as e:
                    print(f"Error reading: {e}")
            
    except Exception as e:
        print(f"Connect error: {e}")

if __name__ == "__main__":
    asyncio.run(test())
