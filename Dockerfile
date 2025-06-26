FROM python:3.12-slim

# Avoid files .pyc
ENV PYTHONDONTWRITEBYTECODE=1
# Leave logs visible at the terminal
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y build-essential gcc curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "pipeline/main.py"]
