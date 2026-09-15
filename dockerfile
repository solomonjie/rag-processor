# worker 镜像：流模式直连 RocketMQ 5.x（官方 gRPC 客户端，纯 Python，无 C++ 依赖）
# 构建：docker build -t rag-worker .
FROM python:3.13-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 流模式常驻消费（配置 Rocketmq_Endpoint）；文件模式/一次性跑批：docker run <img> --once
ENTRYPOINT ["python", "worker.py"]
