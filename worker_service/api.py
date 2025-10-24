# import aiohttp

# async def fetch_market_data(exchange: str, asset: str):
#     """
#     Fetch market data from GoMarket public API for a specific exchange and asset.
    
#     :param exchange: Exchange market name (e.g., "binance")
#     :param asset: Asset pair (e.g., "BTC/USDT")
#     :return: dict with exchange, asset, and current price
#     """
#     url = f"https://gomarket-api.goquant.io/api/symbols/{exchange}/spot"

#     async with aiohttp.ClientSession() as session:
#         async with session.get(url) as resp:
#             if resp.status != 200:
#                 raise Exception(f"Failed to fetch data from {exchange}: {resp.status}")
            
#             data = await resp.json()
    
#     # Find the specific asset in the response
#     for symbol in data.get("symbols", []):
#         if symbol["name"] == asset:
#             price_str = symbol.get("price")
#             # The API may return price as string or empty; handle empty gracefully
#             price = float(price_str) if price_str else 0.0
#             return {
#                 "exchange": exchange,
#                 "asset": asset,
#                 "price": price
#             }

#     # If asset not found in exchange
#     return {"exchange": exchange, "asset": asset, "price": 0.0}


# worker_service/api.py
"""
Helper to fetch market data from GoMarket using a provided aiohttp.ClientSession.
This version expects to be called with a long-lived session (no per-call session creation).
"""

from typing import Dict
import aiohttp

async def fetch_market_data(exchange: str, asset: str, session: aiohttp.ClientSession) -> Dict:
    """
    Fetch market data for a given exchange and asset.

    :param exchange: exchange name as used by GoMarket (e.g., "binance")
    :param asset: asset in normalized format "BTC-USDT" or "BTC/USDT"
    :param session: aiohttp.ClientSession reused across calls
    :return: dict {"exchange": exchange, "asset": asset, "price": float}
             if price is missing or asset not found returns price 0.0
    """
    # Normalize asset to the API format (GoMarket appears to use '-' between base/quote)
    target_name = asset.replace("/", "-")

    url = f"https://gomarket-api.goquant.io/api/symbols/{exchange}/spot"

    async with session.get(url) as resp:
        if resp.status != 200:
            # Allow caller to catch/handle
            raise RuntimeError(f"GoMarket fetch failed for {exchange} status={resp.status}")

        data = await resp.json()

    # Find the matching symbol (GoMarket uses "name" like "BTC-USDT")
    for symbol in data.get("symbols", []):
        name = symbol.get("name")
        if name == target_name:
            price_str = symbol.get("price")
            if not price_str:
                return {"exchange": exchange, "asset": asset, "price": 0.0}
            try:
                price = float(price_str)
            except (ValueError, TypeError):
                price = 0.0
            return {"exchange": exchange, "asset": asset, "price": price}

    # Not found
    return {"exchange": exchange, "asset": asset, "price": 0.0}
