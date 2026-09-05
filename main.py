#from kafka_consumer import start_consuming
from asyncio import run
from logging import getLogger
from fastapi import FastAPI as App
from settings import settings
import aiohttp

logger = getLogger("main")
logger.setLevel("INFO")

import aiohttp
from aiohttp_socks import ProxyConnector

async def send_alert(text: str):
    url = f"https://api.telegram.org/bot{settings.BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": settings.CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    
    # Используем проверенный SOCKS5 прокси с паролем
    proxy_url = "socks5://twist_sz:Lfkb@45.90.58.55:1080"
    
    try:
        connector = ProxyConnector.from_url(proxy_url)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    logger.info("Алерт успешно отправлен через прокси")
                else:
                    logger.error(f"Ошибка Telegram API: {resp.status} - {await resp.text()}")
    except Exception as e:
        logger.error(f"Исключение при отправке: {repr(e)}")

app = App(title="notifications-service", version="1.0.0", description="Notifications service")

@app.post("/notifications")
async def notify(message: dict):
    logger.info(f"Message received: {message}")
    await send_alert(f"Alert:\n{message.get('hypothesis_id', '')}\n{message.get('name', '')}\n{message.get('contact', '')}")  # type: ignore
    return {"message": "Notification received"}

#def main():
    try:
        run(start_consuming())
    except KeyboardInterrupt:
        logger.info("Service stopped")
    except Exception as e:
        logger.error(f"There is an exception: {e}")
    finally:
        logger.info("Stopping")
        exit()

#if __name__ == "__main__":
    main()