FROM python:3.11-alpine AS builder
COPY requirements.txt .
RUN pip install -r requirements.txt
WORKDIR /app
ENV PYTHONPATH=/app
COPY . .
CMD ["python3", "-u", "bot/main.py"]