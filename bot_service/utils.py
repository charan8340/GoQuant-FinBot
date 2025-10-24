import json
import redis
from config import REDIS_HOST, REDIS_PORT, REDIS_DB

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def push_arbitrage_config(user_info):
    """Pushes user arbitrage configuration to Redis stream."""
    r.xadd("user_submissions", {"data": json.dumps(user_info)})
    print(f"[BOT] Arbitrage config pushed: {user_info}")

def push_market_view_config(user_info):
    """Pushes user market view subscription to Redis stream."""
    r.xadd("market_submissions", {"data": json.dumps(user_info)})
    print(f"[BOT] Market view config pushed: {user_info}")
