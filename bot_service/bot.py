import json
import redis
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)
from handlers import (
    start,
    handle_asset_selection,
    handle_exchange_selection,
    handle_threshold,
    echo,
    handle_saved_choice,
    handle_monitor_actions, handle_new_threshold,
)
from config import TELEGRAM_BOT_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_DB
from telegram.request import HTTPXRequest

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def push_to_redis(user_info):
    r.xadd("user_submissions", {"data": json.dumps(user_info)})
    print(f"[BOT] Pushed to Redis: {user_info}")

def main():
    request = HTTPXRequest(connect_timeout=20, read_timeout=20)
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).request(request).build()

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_asset_selection, pattern="^asset:|^done_assets$"))
    app.add_handler(CallbackQueryHandler(handle_exchange_selection, pattern="^exchange:|^done_asset$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_threshold))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_new_threshold))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    app.add_handler(CallbackQueryHandler(handle_saved_choice, pattern="^use_saved$|^start_new$"))
    app.add_handler(CallbackQueryHandler(handle_monitor_actions, pattern="^config_pair:|^monitor_pair:|^monitor_start$|^monitor_stop$"))

    print("[BOT] Running...")
    app.run_polling(stop_signals=None)

if __name__ == "__main__":
    main()
