FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY telebot_requirements.txt .
RUN pip install --no-cache-dir -r telebot_requirements.txt

COPY telebot.py .

CMD ["python", "telebot.py"]
