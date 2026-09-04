import aiohttp

BOT_TOKEN = "8911764684:AAHAw5OfT1r8Xj-x40w2JUMqzh1u1K5Hc7Q"
CHAT_ID = "1182351877"

async def send_alert(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                print(f"Ошибка отправки: {await resp.text()}")
