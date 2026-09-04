import json
import asyncio
import os
import logging
from aiokafka import AIOKafkaConsumer
import aiohttp

# 1. Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("kafka_consumer")

# 2. Переменные окружения
KAFKA_URL = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BROKER_URL", "localhost:9092"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "8911764684:AAHAw5OfT1r8Xj-x40w2JUMqzh1u1K5Hc7Q")
CHAT_ID = os.getenv("CHAT_ID", "1182351877")
TOPICS = ["user_registered", "payment_success", "new_leads"]

# 3. Безопасный десериализатор (не роняет приложение при битых данных)
def safe_deserializer(m):
    raw_str = m.decode("utf-8", errors="replace").strip()
    
    # 1. Пробуем распарсить как JSON
    try:
        return json.loads(raw_str)
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        pass
    
    # 2. Если не JSON, пробуем распарсить ваш формат: hypothesis_id|name|contact
    if "|" in raw_str:
        parts = raw_str.split("|")
        if len(parts) >= 3:
            return {
                "hypothesis_id": parts[0],
                "name": parts[1],
                "contact": parts[2]
            }
            
    # 3. Если формат неизвестен
    logger.warning(f"Неизвестный формат сообщения: {raw_str}")
    return {"_error": "Unknown format", "_raw": raw_str}


# 4. Отправка в Telegram
async def send_alert(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    
    # Укажи реальный IP твоего сервера
    proxy_url = "http://45.90.58.55:8888"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, proxy=proxy_url, timeout=10) as resp:
                if resp.status == 200:
                    logger.info("Алерт успешно отправлен через личный прокси")
                else:
                    logger.error(f"Ошибка Telegram API: {resp.status}")
    except Exception as e:
        logger.error(f"Ошибка сети или прокси: {e}")


# 5. Бизнес-логика
async def process_message(topic: str, value: dict):
    # Защита от невалидных данных
    if not isinstance(value, dict) or "_error" in value:
        logger.warning(f"Пропускаем невалидное сообщение из топика {topic}")
        return
        
    logger.info(f"Обработка сообщения из топика: {topic}")
    
    try:
        if topic == "new_leads":
            alert_text = (
                f"Новая заявка!\n\n"
                f"Гипотеза: <code>{value.get('hypothesis_id', 'N/A')}</code>\n"
                f"Имя: {value.get('name', 'N/A')}\n"
                f"Контакт: {value.get('contact', 'N/A')}"
            )
            await send_alert(alert_text)
        elif topic == "user_registered":
            logger.info(f"Логика welcome email для {value.get('email')}")
        elif topic == "payment_success":
            logger.info(f"Логика payment email для {value.get('email')}")
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")

# 6. Главный цикл
async def start_consuming():
    logger.info(f"Запуск консьюмера. Подключение к: {KAFKA_URL}")
    logger.info(f"Подписка на топики: {TOPICS}")

    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_URL,
        group_id="notification-service-group-v2",  # Новая группа, чтобы игнорировать старый мусор
        auto_offset_reset="latest",                # Читаем только новые сообщения
        value_deserializer=safe_deserializer
    )

    await consumer.start()
    logger.info("Консьюмер успешно запущен и слушает события")

    try:
        async for msg in consumer:
            logger.info(f"Получено сообщение: {msg.value}")
            await process_message(msg.topic, msg.value) # type: ignore
    except asyncio.CancelledError:
        logger.info("Остановка консьюмера")
    finally:
        await consumer.stop()
        logger.info("Консьюмер корректно остановлен")
