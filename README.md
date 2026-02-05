操作	命令 (在 PowerShell 或 CMD 中运行)	说明
1. 创建虚拟环境	python -m venv rag_env	在当前目录下创建一个名为 my_project_env 的虚拟环境文件夹。
2. 激活虚拟环境	rag_env\Scripts\activate	(可选：用于 CMD 或 Git Bash) 激活环境。
3. 退出虚拟环境	deactivate	退出当前的虚拟环境，回到系统环境。
4. 删除虚拟环境	rmdir /s /q rag_env	(可选：用于 CMD) 递归删除虚拟环境文件夹。

need install:
pip install dotenv

pip install llama-index-vector-stores-chroma
pip install llama-index chromadb
pip install llama-index-embeddings-text-embeddings-inference
pip install llama-index-readers-json
pip install llama-index-vector-stores-milvus

pip install lxml_html_clean 
pip install newspaper3k

Run Modl Local:
docker run -d --gpus all --shm-size 4g -p 6000:8000 -v /c/Work/Model/Qwen3-0.6B:/app/model vllm/vllm-openai:latest --model /app/model --served-model-name Qwen3-0.6B --gpu-memory-utilization 0.8 --max-model-len 1024


docker run -d --gpus all --shm-size 4g -p 6000:8000 -v /c/Work/Model/Qwen3-0.6B:/app/model vllm/vllm-openai:latest --model /app/model --served-model-name qwen-0.6b --gpu-memory-utilization 0.8 --max-model-len 1024