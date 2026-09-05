from os import getenv
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
#    KAFKA_BROKER_URL: str = getenv("KAFKA_BROKER_URL", "localhost:9092:9092")
#    MAIL_USERNAME: str = getenv("MAIL_USERNAME", "username")
#    MAIL_PASSWORD: str = getenv("MAIL_PASSWORD", "password")
#    MAIL_FROM: str = getenv("MAIL_FROM", "from")
#    MAIL_PORT: int = int(getenv("MAIL_PORT", "1234"))
#    MAIL_SERVER: str = getenv("MAIL_SERVER", "http://api.mail.ru")
#    MAIL_STARTTLS: bool = bool(getenv("MAIL_STARTTLS", "false"))
#    MAIL_SSL_TLS: bool = bool(getenv("MAIL_SSL_TLS", "false"))
    CHAT_ID: int = int(getenv("CHAT_ID", "1182351877"))
    BOT_TOKEN: str = getenv("BOT_TOKEN", "8911764684:AAHAw5OfT1r8Xj-x40w2JUMqzh1u1K5Hc7Q")

    class Config:
        env_file = ".env"

settings = Settings()
