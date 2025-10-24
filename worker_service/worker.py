# import asyncio
# import json
# from datetime import datetime, timezone
# import redis.asyncio as redis
# from config import REDIS_HOST, REDIS_PORT, REDIS_DB
# from api import fetch_market_data

# r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


# # ------------------- Poll all markets ------------------- #
# async def poll_markets():
#     while True:
#         try:
#             keys = await r.keys("pair:*")
#             if keys:
#                 # Process each market concurrently
#                 tasks = [asyncio.create_task(process_market(key)) for key in keys]
#                 await asyncio.gather(*tasks)
#         except Exception as e:
#             print(f"[ERROR] Polling markets failed: {e}")
#         await asyncio.sleep(1)  # polling interval


# # ------------------- Process a single market ------------------- #
# async def process_market(key):
#     """
#     key: pair:{exchange}
#     value: hash {user_id: '{"asset":"BTC-USDT","threshold":45000}'}
#     """
#     try:
#         users = await r.hgetall(key)
#     except Exception as e:
#         print(f"[ERROR] Could not read key {key}: {e}")
#         return

#     if not users:
#         return

#     exchange = key.split(":")[1]
#     alerts_to_push = []

#     for user_id, value_json in users.items():
#         try:
#             value = json.loads(value_json)
#             asset = value["asset"]
#             threshold = float(value["threshold"])
#         except Exception:
#             print(f"[WARNING] Invalid data in Redis for key {key}: {value_json}")
#             continue

#         try:
#             # Fetch market price
#             data = await fetch_market_data(exchange, asset)
#             price = float(data["price"])
#         except Exception as e:
#             print(f"[ERROR] Failed to fetch price for {asset} on {exchange}: {e}")
#             continue

#         if price >= threshold:
#             timestamp = datetime.now(timezone.utc).isoformat()
#             alert = {
#                 "user_id": user_id,
#                 "asset": asset,
#                 "exchange": exchange,
#                 "price": price,
#                 "threshold": threshold,
#                 "timestamp": timestamp,
#                 "message": f"⚠️ {timestamp} for {asset} on {exchange} price {price} crossed your threshold {threshold}"
#             }
#             alerts_to_push.append(alert)

#     # Push all alerts for this market to Redis stream
#     if alerts_to_push:
#         await asyncio.gather(*[
#             r.xadd("alerts", {"data": json.dumps(alert)}) for alert in alerts_to_push
#         ])


# # ------------------- Main ------------------- #
# if __name__ == "__main__":
#     print("[WORKER] Polling markets started...")
#     asyncio.run(poll_markets())

# worker_service/worker.py
"""
Refactored worker service:
- Uses SMEMBERS active_pairs instead of KEYS
- Reuses a single aiohttp.ClientSession
- Concurrency limiter (Semaphore)
- Cooldown to avoid repeated alerts
- Normalizes asset names to "BTC-USDT"
- Pushes final alerts to Redis stream "alerts"
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Iterable, Tuple

import aiohttp
import redis.asyncio as redis

from config import REDIS_HOST, REDIS_PORT, REDIS_DB
from api import fetch_market_data

# --------- Tunables (override via env or edit here) ----------
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))       # seconds between poll cycles
MAX_CONCURRENT_FETCHES = int(os.getenv("MAX_CONCURRENT_FETCHES", "10"))
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "20"))        # aiohttp timeout seconds
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "300"))  # don't re-alert same user/pair sooner than this
ALERT_STREAM = "alerts"
ACTIVE_PAIRS_SET = "active_pairs"   # expected values: "BTC-USDT:binance" (asset:exchange)
PAIR_HASH_PREFIX = "pair:"           # pair:{exchange} contains user fields -> JSON {"asset":"...","threshold":...}
LAST_ALERT_PREFIX = "last_alert:"    # last_alert:{user_id}:{asset}:{exchange} -> unix_ts
# --------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("worker_service")

# Redis client (reused)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# Semaphore for limiting concurrent fetches to external API
fetch_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)


def normalize_asset(asset: str) -> str:
    """
    Normalize user-provided asset to canonical format used internally and by GoMarket:
    prefer 'BTC-USDT' (dash). Also accept 'BTC/USDT' and convert to 'BTC-USDT'.
    """
    if not asset:
        return asset
    return asset.replace("/", "-").upper()


async def safe_fetch(exchange: str, asset: str, session: aiohttp.ClientSession) -> Dict:
    """
    Fetch market data while honoring the semaphore and catching errors.
    Returns dict with keys exchange, asset, price (float)
    """
    async with fetch_semaphore:
        try:
            # fetch_market_data raises on non-200
            result = await asyncio.wait_for(fetch_market_data(exchange, asset, session), timeout=FETCH_TIMEOUT)
            return result
        except asyncio.TimeoutError:
            logger.warning("Timeout fetching %s on %s", asset, exchange)
        except Exception as e:
            logger.exception("Error fetching %s on %s: %s", asset, exchange, e)
    return {"exchange": exchange, "asset": asset, "price": 0.0}


def should_alert(user_id: str, asset: str, exchange: str) -> bool:
    """
    Check cooldown for user/asset/exchange. If no recent alert, return True.
    Otherwise False.
    """
    key = f"{LAST_ALERT_PREFIX}{user_id}:{asset}:{exchange}"
    ts = asyncio.get_event_loop().run_until_complete(r.get(key)) if False else None
    # We avoid blocking here — the caller will check cooldown via async function below
    # This function is provided for completeness; async version is used in flow.
    return True


async def is_cooled_down(user_id: str, asset: str, exchange: str) -> bool:
    """
    Async cooldown check, returns True if enough time passed or no record found.
    """
    key = f"{LAST_ALERT_PREFIX}{user_id}:{asset}:{exchange}"
    val = await r.get(key)
    if not val:
        return True
    try:
        last_ts = int(val)
    except (ValueError, TypeError):
        return True
    now_ts = int(datetime.now(timezone.utc).timestamp())
    return (now_ts - last_ts) >= COOLDOWN_SECONDS


async def record_alert_timestamp(user_id: str, asset: str, exchange: str):
    key = f"{LAST_ALERT_PREFIX}{user_id}:{asset}:{exchange}"
    now_ts = int(datetime.now(timezone.utc).timestamp())
    await r.set(key, now_ts)


async def load_active_pairs() -> Iterable[Tuple[str, str]]:
    """
    Read the active_pairs set and yield (asset, exchange) tuples.
    Expected member format: "BTC-USDT:binance" (asset:exchange)
    """
    members = await r.smembers(ACTIVE_PAIRS_SET)
    for m in members or []:
        try:
            asset, exchange = m.split(":", 1)
            yield normalize_asset(asset), exchange.lower()
        except ValueError:
            logger.warning("Ignoring malformed active_pairs entry: %s", m)


async def process_pair(asset: str, exchange: str, session: aiohttp.ClientSession):
    """
    For a specific asset+exchange, read all users in pair:{exchange} and test thresholds.
    This supports the existing Redis schema where pair:{exchange} is a hash:
       field=user_id -> JSON {"asset": "BTC-USDT", "threshold": 45000}
    """
    pair_hash_key = f"{PAIR_HASH_PREFIX}{exchange}"
    try:
        users = await r.hgetall(pair_hash_key)
    except Exception as e:
        logger.exception("Failed to read %s: %s", pair_hash_key, e)
        return

    if not users:
        return

    # Fetch market price for this specific asset & exchange
    market = await safe_fetch(exchange, asset, session)
    price = float(market.get("price", 0.0) or 0.0)

    # If price is 0.0 treat as missing/stale and skip alerting
    if price <= 0.0:
        # Log at debug; avoid spamming logs
        logger.debug("No price for %s on %s (skipping)", asset, exchange)
        return

    alerts_to_push = []

    for user_id, value_json in users.items():
        try:
            value = json.loads(value_json)
            user_asset = normalize_asset(value.get("asset", ""))
            threshold = float(value.get("threshold"))
        except Exception:
            logger.warning("Invalid user entry in %s for field %s: %s", pair_hash_key, user_id, value_json)
            continue

        # Only process if user_asset matches this asset
        if user_asset != asset:
            continue

        # Threshold crossing logic: only alert when price >= threshold
        if price >= threshold:
            # Check cooldown
            cooled = await is_cooled_down(user_id, asset, exchange)
            if not cooled:
                logger.debug("Skipping alert due to cooldown for %s:%s on %s", user_id, asset, exchange)
                continue

            timestamp = datetime.now(timezone.utc).isoformat()
            message = f"⚠️ {timestamp} — {asset} on {exchange} price {price} crossed your threshold {threshold}"

            alert = {
                "user_id": str(user_id),
                "asset": asset,
                "exchange": exchange,
                "price": price,
                "threshold": threshold,
                "timestamp": timestamp,
                "message": message
            }
            alerts_to_push.append(alert)
            # record timestamp to avoid duplicates
            await record_alert_timestamp(user_id, asset, exchange)

    # Push alerts into the Redis stream in parallel
    if alerts_to_push:
        try:
            await asyncio.gather(*[
                r.xadd(ALERT_STREAM, {"data": json.dumps(a)})
                for a in alerts_to_push
            ])
            logger.info("Pushed %d alerts for %s on %s", len(alerts_to_push), asset, exchange)
        except Exception:
            logger.exception("Failed to push alerts to stream %s", ALERT_STREAM)


async def poll_loop():
    timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        logger.info("Worker poll loop started (interval=%ss, concurrency=%s)", POLL_INTERVAL, MAX_CONCURRENT_FETCHES)
        while True:
            try:
                # Build list of (asset, exchange) to process from active_pairs
                tasks = []
                async for asset, exchange in load_active_pairs():
                    # create a task per pair
                    tasks.append(asyncio.create_task(process_pair(asset, exchange, session)))

                if tasks:
                    # Run them concurrently (limited by the semaphore inside safe_fetch)
                    await asyncio.gather(*tasks, return_exceptions=True)
                else:
                    logger.debug("No active pairs found")

            except Exception:
                logger.exception("Error during poll cycle")
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    print("Worker started")
    try:
        asyncio.run(poll_loop())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
