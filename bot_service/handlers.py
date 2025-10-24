import json
import redis.asyncio as redis
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import BadRequest
from config import REDIS_HOST, REDIS_PORT, REDIS_DB

# Assets and Exchanges
ASSETS = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "ADA-USDT"]
EXCHANGES = [
    "okx", "deribit", "bybit", "binance", "cryptocom", "kraken", "kucoin",
    "bitstamp", "bitmex", "coinbase_intl", "coinbase", "bitfinex", "gateio",
    "mexc", "gemini", "htx", "bitget", "dydx", "bitso", "hyperliquid",
    "blofin", "ibkr"
]

# Redis connection
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# ----------------- Helpers ----------------- #
async def safe_reply(update: Update, text: str, **kwargs):
    try:
        if update.message:
            return await update.message.reply_text(text, **kwargs)
        elif update.callback_query:
            try:
                return await update.callback_query.edit_message_text(text, **kwargs)
            except BadRequest:
                return await update.callback_query.message.reply_text(text, **kwargs)
    except BadRequest:
        pass

def pair_label(pair_key: str) -> str:
    return pair_key.replace(":", " on ")

async def save_user_threshold(asset: str, exchange: str, user_id: str, threshold: float):
    key = f"pair:{asset}:{exchange}"
    await r.hset(key, str(user_id), threshold)
    await r.xadd("pair_updates", {"pair": key})
    await r.sadd("active_pairs", f"{asset}:{exchange}")  # Add to active pairs for worker

# ----------------- Start ----------------- #
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    saved_config = await r.hget("user_configs", str(user.id))

    if saved_config:
        cfg = json.loads(saved_config)
        context.user_data.update(cfg)
        await safe_reply(
            update,
            "👋 Welcome back! We found your previous configuration.\n"
            "Would you like to:\n1️⃣ Use your saved settings\n2️⃣ Start a new setup?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Use saved", callback_data="use_saved"),
                    InlineKeyboardButton("Start new", callback_data="start_new")
                ]
            ])
        )
    else:
        context.user_data.clear()
        context.user_data["assets_list"] = []
        context.user_data["asset_exchanges"] = {}
        await send_asset_buttons(update, context)

async def handle_saved_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "use_saved":
        saved_config = await r.hget("user_configs", str(update.effective_user.id))
        if not saved_config:
            await safe_reply(update, "No saved configuration found, please start new setup.")
            return
        cfg = json.loads(saved_config)
        context.user_data.update(cfg)
        await show_monitor_selection(update, context)
    else:
        # Start new setup
        context.user_data.clear()
        context.user_data["assets_list"] = []
        context.user_data["asset_exchanges"] = {}
        await send_asset_buttons(update, context)

# ----------------- Asset selection ----------------- #
async def send_asset_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard, row = [], []
    for i, asset in enumerate(ASSETS, 1):
        selected = asset in context.user_data.get("assets_list", [])
        label = f"✅ {asset}" if selected else asset
        row.append(InlineKeyboardButton(label, callback_data=f"asset:{asset}"))
        if i % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("Done selecting assets", callback_data="done_assets")])
    markup = InlineKeyboardMarkup(keyboard)
    await safe_reply(update, "Select assets (multiple allowed):", reply_markup=markup)

async def handle_asset_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "done_assets":
        if not context.user_data.get("assets_list"):
            await safe_reply(update, "You must select at least one asset.")
            return
        context.user_data["current_asset_index"] = 0
        context.user_data["current_asset"] = context.user_data["assets_list"][0]
        await send_exchange_buttons(update, context)
        return

    asset = data.split(":", 1)[1]
    if asset not in context.user_data.get("assets_list", []):
        context.user_data["assets_list"].append(asset)
        context.user_data["asset_exchanges"][asset] = []
    await send_asset_buttons(update, context)

# ----------------- Exchange selection ----------------- #
async def send_exchange_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asset = context.user_data.get("current_asset")
    keyboard, row = [], []

    for i, ex in enumerate(EXCHANGES, 1):
        selected = ex in context.user_data["asset_exchanges"].get(asset, [])
        label = f"✅ {ex}" if selected else ex
        row.append(InlineKeyboardButton(label, callback_data=f"exchange:{ex}"))
        if i % 3 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("Done for this asset", callback_data="done_asset")])
    markup = InlineKeyboardMarkup(keyboard)
    await safe_reply(update, f"Select exchanges for {asset} (multiple allowed):", reply_markup=markup)

async def handle_exchange_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    asset = context.user_data.get("current_asset")

    if data == "done_asset":
        idx = context.user_data.get("current_asset_index", 0)
        assets_list = context.user_data["assets_list"]
        if idx + 1 < len(assets_list):
            next_idx = idx + 1
            context.user_data["current_asset_index"] = next_idx
            context.user_data["current_asset"] = assets_list[next_idx]
            await send_exchange_buttons(update, context)
        else:
            # Threshold entry flow
            context.user_data["threshold_asset_index"] = 0
            asset = assets_list[0]
            exchanges = context.user_data["asset_exchanges"][asset]
            context.user_data["current_threshold_asset"] = asset
            context.user_data["current_threshold_exchanges"] = exchanges
            context.user_data["current_exchange_index"] = 0
            await safe_reply(update, f"Enter threshold for {asset} on {exchanges[0]}:")
        return

    ex = data.split(":", 1)[1]
    selected_ex = context.user_data["asset_exchanges"].get(asset, [])
    if ex in selected_ex:
        selected_ex.remove(ex)
    else:
        selected_ex.append(ex)
    context.user_data["asset_exchanges"][asset] = selected_ex
    await send_exchange_buttons(update, context)


async def save_user_market_threshold(exchange: str, user_id: str, asset: str, threshold: float):
    key = f"pair:{exchange}"

    # Store each user info as JSON in a Redis hash
    user_info = json.dumps({
        "asset": asset,
        "threshold": threshold
    })

    await r.hset(key, str(user_id), user_info)

# ----------------- Threshold input ----------------- #
async def handle_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    asset = context.user_data["current_threshold_asset"]
    exchanges = context.user_data["current_threshold_exchanges"]
    idx = context.user_data.get("current_exchange_index", 0)
    ex = exchanges[idx]

    try:
        threshold_value = float(update.message.text.strip())
    except ValueError:
        await safe_reply(update, "⚠️ Please enter a valid number for threshold.")
        return

    # --- Save user threshold to the market hash in Redis ---
    await save_user_market_threshold(ex, user_id, asset, threshold_value)

    # Move to next exchange
    idx += 1
    if idx < len(exchanges):
        context.user_data["current_exchange_index"] = idx
        await safe_reply(update, f"Enter threshold for {asset} on {exchanges[idx]}:")
        return

    # Move to next asset
    asset_idx = context.user_data.get("threshold_asset_index", 0) + 1
    assets_list = context.user_data["assets_list"]
    if asset_idx < len(assets_list):
        next_asset = assets_list[asset_idx]
        context.user_data["current_threshold_asset"] = next_asset
        context.user_data["current_threshold_exchanges"] = context.user_data["asset_exchanges"][next_asset]
        context.user_data["current_exchange_index"] = 0
        context.user_data["threshold_asset_index"] = asset_idx
        await safe_reply(update, f"Enter threshold for {next_asset} on {context.user_data['current_threshold_exchanges'][0]}:")
        return

    # All thresholds done
    await safe_reply(update, "✅ All thresholds received! Configuration saved successfully.")
    context.user_data.clear()



# ----------------- Monitor / Configure ----------------- #
async def show_monitor_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    saved = await r.hget("user_configs", str(user_id))
    if not saved:
        await safe_reply(update, "No configuration found. Use /start to configure pairs.")
        return
    cfg = json.loads(saved)
    thresholds = cfg.get("thresholds", {})
    context.user_data["monitor_pairs"] = context.user_data.get("monitor_pairs", list(thresholds.keys()))

    keyboard, row = [], []
    for i, pair_key in enumerate(thresholds.keys(), 1):
        selected = pair_key in context.user_data["monitor_pairs"]
        label = f"✅ {pair_label(pair_key)}" if selected else pair_label(pair_key)
        row.append(InlineKeyboardButton(label, callback_data=f"monitor_pair:{pair_key}"))
        if i % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([
        InlineKeyboardButton("▶️ Start Monitoring", callback_data="monitor_start"),
        InlineKeyboardButton("⏹ Stop Monitoring", callback_data="monitor_stop")
    ])
    keyboard.append([InlineKeyboardButton("⚙️ Configure Pairs", callback_data="monitor_configure")])
    markup = InlineKeyboardMarkup(keyboard)
    await safe_reply(update, "Select pairs to monitor (multiple allowed):", reply_markup=markup)

async def handle_monitor_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data

    if data.startswith("monitor_pair:"):
        pair_key = data.split(":", 1)[1]
        selected = context.user_data.get("monitor_pairs", [])
        if pair_key in selected:
            selected.remove(pair_key)
        else:
            selected.append(pair_key)
        context.user_data["monitor_pairs"] = selected
        await show_monitor_selection(update, context)
        return

    if data == "monitor_start":
        pairs = context.user_data.get("monitor_pairs", [])
        if not pairs:
            await query.edit_message_text("⚠️ Select at least one pair.")
            return
        await r.hset(f"monitor:{user_id}", mapping={"pairs": json.dumps(pairs)})
        await query.edit_message_text("▶️ Monitoring started for:\n" + "\n".join(pair_label(p) for p in pairs))
        return

    if data == "monitor_stop":
        await r.delete(f"monitor:{user_id}")
        context.user_data.pop("monitor_pairs", None)
        await query.edit_message_text("⏹ Monitoring stopped.")
        return

    if data == "monitor_configure":
        await show_configure_options(update, context)
        return

    if data.startswith("config_pair:"):
        await handle_config_pair(update, context)
        return

async def show_configure_options(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    saved = await r.hget("user_configs", str(user_id))
    if not saved:
        await safe_reply(update, "No saved configuration.")
        return
    cfg = json.loads(saved)
    thresholds = cfg.get("thresholds", {})

    keyboard = [[InlineKeyboardButton(f"⚙️ {pair_label(p)}", callback_data=f"config_pair:{p}") ] for p in thresholds.keys()]
    markup = InlineKeyboardMarkup(keyboard)
    await safe_reply(update, "Select a pair to configure (send new threshold):", reply_markup=markup)

async def handle_config_pair(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    pair_key = query.data.split(":", 1)[1]
    context.user_data["configuring_pair"] = pair_key
    await query.edit_message_text(f"You are configuring *{pair_label(pair_key)}*.\nSend the new threshold value.", parse_mode="Markdown")

# ----------------- Fallback ----------------- #
async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id == context.bot.id:
        return
    await safe_reply(update, "Please use /start to begin or follow prompts.")
