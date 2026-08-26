FROM python:3.13-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 常驻 worker；一次性跑批改为: docker run <img> --once
ENTRYPOINT ["python", "worker.py"]
