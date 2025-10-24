# import asyncio
# from telegram import Bot
# from config import TELEGRAM_BOT_TOKEN

# async def send_test():
#     bot = Bot(token=TELEGRAM_BOT_TOKEN)
#     chat_id = 5314051824  # Replace with your actual user id
#     await bot.send_message(chat_id=chat_id, text="✅ Test alert from GoQuant worker!")
#     print("Message sent successfully")

# # Run the async function
# asyncio.run(send_test())

import asyncio
from telegram import Bot
from telegram.request import HTTPXRequest

async def main():
    request = HTTPXRequest(connect_timeout=30, read_timeout=30)
    bot = Bot(token="8234860072:AAHbZAGpPc4fyVgibJ7z8XmT0XCr7DrkpXU", request=request)
    await bot.send_message(chat_id=5314051824, text="✅ Test message!")

asyncio.run(main())
