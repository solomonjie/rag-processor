# worker 镜像：流模式直连 RocketMQ（4.x 客户端为 C++ binding，仅 Linux，故必须容器运行）
# 构建：docker build -t rag-worker .
#   GitHub 直连不通时加代理前缀：--build-arg GITHUB_PROXY=https://ghproxy.net/
FROM python:3.13-slim
WORKDIR /app

ARG GITHUB_PROXY=""

# librocketmq C++ 动态库（rocketmq-client-python 的依赖，pip 不带；官方二进制发行版）
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fL ${GITHUB_PROXY}https://github.com/apache/rocketmq-client-cpp/releases/download/2.2.0/rocketmq-client-cpp-2.2.0.amd64.deb -o /tmp/librmq.deb \
    && dpkg -i /tmp/librmq.deb || apt-get -f install -y \
    && rm -rf /var/lib/apt/lists/* /tmp/librmq.deb

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 流模式常驻消费（配置 Rocketmq_NameSrv）；文件模式/一次性跑批：docker run <img> --once
ENTRYPOINT ["python", "worker.py"]
