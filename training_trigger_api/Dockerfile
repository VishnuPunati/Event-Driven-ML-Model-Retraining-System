FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything
COPY . .

# 👇 THIS IS THE KEY LINE
ENV PYTHONPATH=/app

CMD ["python", "-m", "app.main"]
