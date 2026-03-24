FROM python:3.11-slim

WORKDIR /app

# Зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код
COPY botv1_fixed.py .

# Папка для базы данных
RUN mkdir -p app/data

CMD ["python", "botv1_fixed.py"]
