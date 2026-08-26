import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, FileSystemLoader
from settings import settings
from logging import getLogger

logger = getLogger("mail_service")

# Путь к папке с HTML-шаблонами
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")

env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

async def send_email(to_email: str, subject: str, template_name: str, body_data: dict):
    """Отправляет email, используя стандартный smtplib и Jinja2 для шаблонов"""
    try:
        # 1. Рендерим HTML из шаблона
        template = env.get_template(template_name)
        html_content = template.render(**body_data)

        # 2. Формируем MIME-сообщение
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = settings.MAIL_FROM
        msg["To"] = to_email
        msg.attach(MIMEText(html_content, "html"))

        # 3. Подключаемся к SMTP и отправляем
        # (Для локального MailHog логин/пароль и TLS не нужны, для прода - будут использованы)
        with smtplib.SMTP(settings.MAIL_SERVER, settings.MAIL_PORT) as server:
            if settings.MAIL_STARTTLS:
                server.starttls()
            
            if settings.MAIL_USER and settings.MAIL_PASSWORD: # type: ignore
                server.login(settings.MAIL_USER, settings.MAIL_PASSWORD) # type: ignore
            
            server.send_message(msg)
            
        logger.info(f"Email successfully sent to {to_email}")
        
    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        raise
