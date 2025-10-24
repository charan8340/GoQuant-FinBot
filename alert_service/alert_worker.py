import asyncio
import json
import redis.asyncio as aioredis
from telegram import Bot
from telegram.request import HTTPXRequest
from config import TELEGRAM_BOT_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_DB
from datetime import datetime, timezone
import time

# Redis for alerts + message tracking
r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

request = HTTPXRequest(connect_timeout=30, read_timeout=30)
bot = Bot(token=TELEGRAM_BOT_TOKEN, request=request)

MESSAGE_TRACK_KEY = "sent_messages"  # Redis hash to store message_id per user+asset+exchange

async def send_alert(alert: dict):
    """
    Sends or updates a Telegram message for a specific user+asset+exchange.
    """
    try:
        key = f"{alert['user_id']}:{alert['asset']}:{alert['exchange']}"
        message_id = await r.hget(MESSAGE_TRACK_KEY, key)

        if message_id:
            # Update existing message
            await bot.edit_message_text(
                chat_id=alert["user_id"],
                message_id=int(message_id),
                text=alert["message"]
            )
            print(f"[ALERT UPDATED] {alert['asset']} on {alert['exchange']} for user {alert['user_id']}")
        else:
            # Send first message
            msg = await bot.send_message(chat_id=alert["user_id"], text=alert["message"])
            # Store message_id in Redis
            await r.hset(MESSAGE_TRACK_KEY, key, msg.message_id)
            print(f"[ALERT SENT] {alert['asset']} on {alert['exchange']} for user {alert['user_id']}")
    except Exception as e:
        print(f"[ALERT ERROR] {e}")

async def process_alerts(alerts):
    tasks = [send_alert(alert) for alert in alerts]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def listen_alerts():
    print("[ALERT SERVICE] Listening for alerts...")
    last_id = "0-0"
    start_time = time.time()

    while True:
        try:
            messages = await r.xread({"alerts": last_id}, block=1000, count=50)
            if messages:
                alerts_to_send = []
                for stream_name, msgs in messages:
                    for msg_id, data in msgs:
                        alert = json.loads(data["data"])
                        alerts_to_send.append(alert)
                        last_id = msg_id
                
                current_time = time.time()
                if current_time - start_time < 120:
                    await process_alerts(alerts_to_send)
                
                start_time = current_time
            else:
                await asyncio.sleep(0.1)
        except aioredis.ConnectionError:
            print("[ALERT SERVICE] Redis connection error, retrying in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[ALERT SERVICE] Unexpected error: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(listen_alerts())