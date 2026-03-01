FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

COPY . .
# ENTRYPOINT 设定主程序，不可被轻易覆盖
ENTRYPOINT ["python", "Orchestration.py"]
# CMD 提供默认参数，启动时可被覆盖
CMD ["--type", "clean", "--id", "1"]