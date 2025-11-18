FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей для Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем Playwright и браузеры (install-deps автоматически установит все нужные пакеты)
RUN playwright install --with-deps chromium

# Копируем код
COPY . .

# Порт для Flask
EXPOSE 5000

# Запуск Flask приложения
CMD ["python", "app.py"]

