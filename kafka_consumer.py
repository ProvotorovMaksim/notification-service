import json
import asyncio
from confluent_kafka import Consumer, KafkaException
from logging import getLogger
from settings import settings
from mail import send_email  # Функция отправки из предыдущего шага

logger = getLogger("kafka_consumer")
logger.setLevel("INFO")

# Используем KAFKA_BOOTSTRAP_SERVERS или KAFKA_BROKER_URL для обратной совместимости
KAFKA_URL = getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', getattr(settings, 'KAFKA_BROKER_URL', 'localhost:9092'))

consumer_config = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'notification-service-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True  # Коммитим оффсет только после успешной обработки
}

TOPICS = ['user_registered', 'payment_success']

async def process_message(topic: str, key: str, value: dict):
    """Асинхронная бизнес-логика обработки события"""
    try:
        if topic == 'user_registered':
            await send_email(
                to_email=value.get('email'), # type: ignore
                subject="Добро пожаловать в наш сервис!",
                template_name="welcome.html",
                body_data={"username": value.get('username', 'Пользователь')}
            )
            logger.info(f"Welcome email sent to {value.get('email')}")
            
        elif topic == 'payment_success':
            await send_email(
                to_email=value.get('email'), # type: ignore
                subject="Оплата прошла успешно",
                template_name="payment_success.html",
                body_data={
                    "username": value.get('username', 'Пользователь'),
                    "amount": value.get('amount', 0)
                }
            )
            logger.info(f"Payment success email sent to {value.get('email')}")
        else:
            logger.warning(f"Unknown topic received: {topic}")
            
    except Exception as e:
        # Ловим ошибки отправки почты, чтобы не ронять весь consumer
        logger.error(f"Error processing message from {topic}: {e}")

def start_consuming():
    logger.info(f"Starting Kafka consumer, subscribing to: {TOPICS}")
    
    with Consumer(consumer_config) as consumer:
        consumer.subscribe(TOPICS)
        try:
            while True:
                msg = consumer.poll(1.0) # Таймаут 1 секунда
                
                if msg is None:
                    continue
                
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                topic = msg.topic()
                key = msg.key().decode('utf-8') if msg.key() else None
                
                try:
                    value_dict = json.loads(msg.value().decode('utf-8'))
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON from topic {topic}. Raw: {msg.value()}")
                    continue
                
                # Запускаем асинхронную функцию из синхронного потока.
                # Это безопасно, если start_consuming() запущен в отдельном потоке (см. ниже).
                try:
                    asyncio.run(process_message(topic, key, value_dict)) # type: ignore
                except RuntimeError:
                    # Фоллбэк, если event loop уже существует в этом потоке
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(process_message(topic, key, value_dict)) # type: ignore
                    
        except KeyboardInterrupt:
            logger.info("Stopping consumer by user request...")
        except KafkaException as e:
            logger.error(f"Fatal Kafka error: {e}")
        finally:
            logger.info("Closing consumer gracefully...")
            consumer.close()
