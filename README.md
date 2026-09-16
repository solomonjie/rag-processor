# rag-processor

舆情知识库**构建端**：把上游舆情系统推送的新闻加工成 Milvus 向量库（collection `product_knowledge_base`），供消费端（FastAPI+Vue，另一仓库）检索。

架构依据见 `data/架构决策记录-2026-08-26.md`，字段/格式硬约束见 `data/知识库构建规范-消费端数据契约.md`，推送协议见 `data/舆情监测系统数据推送接口文档.md`（均在 data/ 目录，不受版本控制，改动前先读）。

## 架构（worker 直连 RocketMQ，三阶段处理核心，无中间存储依赖）

```
舆情系统 ──推──> RocketMQ 5.x ──gRPC──> worker（N 实例，同一 consumer group）
  (yqms_thirdparty_push)   (Proxy:8081)  │ 消费循环（串行，一批即一波）：
                              │   receive 一批 → 逐条 JSON 解析 + clean（毫秒级）
                              │   波处理：URL 去重 → enrich 并发池（LLM 判定/打标/region）
                              │     → index（embedding + upsert Milvus）
                              │   处理成功才 ack；不 ack 的等不可见到期重投
                              ▼
                     Milvus product_knowledge_base（最终库）
```

**可靠性全部交给 RocketMQ + Milvus，没有 MinIO/中间暂存层**：

- **ack 后置**：消息处理完（index upsert 成功）才 ack；基础设施故障（Milvus/TEI/LLM 网关不可达）不 ack，由不可见时间到期重投（at-least-once）。node_id=md5(url) 去重 + upsert 幂等保证重投无害。数据级终态失败（非法 JSON/clean 失败/LLM 判定失败）写死信后仍 ack——SimpleConsumer 的重投没有 maxReconsumeTimes/%DLQ% 兜底，不 ack 就永远重投同一具尸体
- **积压在 broker 上**：处理速度受 LLM 限制（每条新闻一次调用），串行 receive（上一波处理完才拉下一波）即天然背压，MQ 本身就是持久缓冲。**水平扩展 = 同 group 多起几个 worker**（pop 消费服务端自动分摊队列），无需协调
- **worker 无需固定盘**：全程内存处理，崩溃由 MQ 重投恢复；唯一要挂卷的是死信目录（见下）

**两条到达路径共用同一套处理核心**（extract_record / dedup_enrich / build_node+upsert），配置决定走哪条：

| 路径 | 触发条件 | 说明 |
|---|---|---|
| 流模式（生产） | `Rocketmq_Endpoint` 非空 | 直连消费 RocketMQ 5.x（Proxy gRPC 地址）；官方纯 Python 客户端，任意 OS 可直跑 |
| 文件模式（开发/手工导入） | `Rocketmq_Endpoint` 留空 | 本地 `data/inbox/` 批次文件夹三阶段（目录即状态，阶段可独立重跑，`--once` 排空） |

**死信**：单条级终态失败（非法 JSON、无发布时间、LLM 判定失败）写 `data/dead_letter/`（带阶段与 reason，可重放）；基础设施故障不写死信、走 MQ 重投。流模式生产部署给死信目录挂卷即可持久——这是容器对磁盘的唯一要求。

**丢弃（设计行为，只计数）**：广告/非新闻/不相关/抽不出正文。enrich 产物含被弃记录的 verdict 供审计。

## 处理核心（两条路径共用）

- **clean**：`canon_row` 别名映射（协议字段 infoRegion/publishTime/webName 等在这一步对齐）→ `extract_record`：trafilatura 抽正文（html 字段，**或 content 检出 HTML 标签时**——上游 content 实测多为网页 HTML，弱片段剥标签兜底）、published_date 全代码归一化为东八区定长 ISO（支持协议的 Long 秒时间戳）、`node_id = md5(url)`；全链无时间 → 死信
- **enrich**：每条新闻一次 LLM 调用（判定 is_ad/is_news/relevant + tags ≤3 + region 省级封闭词表），`LLM_Concurrency` 限并发；URL 去重在 LLM 之前（Milvus 查已入库 node_id 直接跳过，省调用费）；response_format=json_object（vllm 下 guided_json，不支持自动降级）
- **region 决策顺序**：上游 infoRegion 能归一化成省级标准名（"陕西省 西安市"→陕西省、"北京"→北京市；"中国"忽略、"未知"交 LLM）时优先于 LLM 抽取
- **index**：只入库 verdict.keep 的记录；text 严格按契约 §4.1；llama-index upsert 路径（幂等，同 node_id 覆盖）；BM25 + HNSW，TTL 180 天

## 输入格式（按扩展名解析，文件模式；流模式为单条 JSON）

| 扩展名 | 解析方式 | 说明 |
|---|---|---|
| `.json` | 单对象或 JSON 数组 | 爬取侧标准投递格式 |
| `.jsonl` | 每行一个对象 | 大文件流式友好 |
| `.xlsx` | 每行一条记录 | 列名自动映射（见下），需要 `html` 或 `content` 列之一 |
| `.txt` | 单文件单篇 | 首个非空行=标题，其余为正文；url 用 `txt://文件名` |
| `.html` | 单文件单页 | 整文件作为 HTML 走 trafilatura；url 用 `file://文件名` |

record 规范字段（Excel 列名/JSON 键名均按别名映射，中英文/驼峰均可）：

| 规范字段 | 别名示例 | 必填 | 说明 |
|---|---|---|---|
| `url` | URL/链接/链接地址 | 建议 | 缺失时用内容指纹合成 `nohost://`（重复导入可去重，但无法按渠道过滤） |
| `html` | 网页内容/raw_html | 与 content 二选一 | 有则走 trafilatura 抽正文 |
| `content` | fullContent/全量内容/正文 | 与 html 二选一 | 已抽好的纯文本，直通 |
| `title` | 标题 | 否 | 缺省取正文首行 |
| `publish_time` | publishTime/发布时间/日期 | 否 | 优先级：publish_time > 页面内时间 > collect_time，全无→死信 |
| `source` | webName/来源/站点 | 否 | 缺省取 url 域名 |
| `collect_time` | collectTime/采集时间 | 否 | 时间兜底；txt/html 文件自动取文件修改时间 |
| `region` | 地区/发生地/infoRegion/authorRegion | 否 | 爬取侧自带合法省级名时优先于 LLM 抽取 |

published_date 归一化为定长 `YYYY-MM-DDTHH:MM:SS`（契约 §6.1，东八区口径），支持 `2026-01-19 16:14:05`、`2026/1/19`、`2026年1月19日 9:05`、ISO 带时区、Unix 时间戳（秒/毫秒）、Excel 日期单元格等。

## 依赖服务

| 服务 | 用途 | 说明 |
|---|---|---|
| RocketMQ 5.x（上游集群） | 数据到达 | 协议见 data/ 接口文档；连接 Proxy 的 gRPC endpoint（默认端口 8081），非 NameServer |
| Milvus ≥2.5 | 内容库 + tag_collection | 服务端 BM25(jieba+cnalphanumonly)，HNSW/IP，TTL 180 天 |
| TEI | Qwen3-Embedding-0.6B (1024维) | **必须与消费端同一模型**，换模型=跨端协调 |
| LLM（OpenAI 兼容） | 判定+打标 | vllm 私有部署（guided_json 约束解码）或商用 API（自动降级白名单校验） |

## 使用

虚拟环境（在 PowerShell 或 CMD 中运行）：

| 操作 | 命令 | 说明 |
|---|---|---|
| 1. 创建虚拟环境 | `python -m venv rag_env` | 在当前目录下创建一个名为 rag_env 的虚拟环境文件夹 |
| 2. 激活虚拟环境 | `rag_env\Scripts\activate` | Git Bash 用 `source rag_env/Scripts/activate` |
| 3. 退出虚拟环境 | `deactivate` | 退出当前虚拟环境，回到系统环境 |
| 4. 删除虚拟环境 | `rmdir /s /q rag_env` | （可选，用于 CMD）递归删除虚拟环境 |

```bash
pip install -r requirements.txt

python create_collection.py        # 建表/校验（--drop 删除重建）

# worker：.env 里 Rocketmq_Endpoint 非空 → 流模式；留空 → 文件模式
python worker.py                   # 常驻（流模式消费 / 文件模式轮询本地批次）
python worker.py --once            # 文件模式：排空当前批次后退出（cron 用）

# 独立运行某个阶段（文件模式回放/补跑）
python -m clean.run --once         # 只跑抽取（会先收编 inbox/ 新批次）
python -m enrich.run --once        # 只跑判定打标
python -m index.run --once         # 只跑入库 + 完成批次清理
```

## 端到端测试链路（build 镜像 → 起容器 → 跑测试数据）

测试用例 = `data/pipeline.xlsx` 62 行真实样例（url 替换为每轮唯一的 `e2e-` 前缀，保证可重跑、可清理）+ 1 条坏 JSON。**worker 跑容器（生产形态），测试脚本在宿主机发压、验收线上 Milvus**。按序执行，命令可直接复制。

### 第 0 步 · 本地测试用 RocketMQ 5.1.3 集群（仅首次搭建）

生产连上游集群；本地测试自建一套（三容器）。**proxy 必须等 broker 向 namesrv 注册完再启动**，否则启动即退出：

```bash
docker run -d --name rmq5namesrv apache/rocketmq:5.1.3 sh mqnamesrv
docker run -d --name rmq5broker --link rmq5namesrv:namesrv \
  -e NAMESRV_ADDR=namesrv:9876 apache/rocketmq:5.1.3 sh mqbroker
sleep 60   # 等 broker 注册（首次约 1 分钟）
docker run -d --name rmq5proxy --link rmq5namesrv:namesrv \
  -p 8081:8081 apache/rocketmq:5.1.3 sh mqproxy -n namesrv:9876

# 建测试 topic（8 队列）
docker exec rmq5broker sh mqadmin updateTopic \
  -n namesrv:9876 -c DefaultCluster -t yqms_thirdparty_push
```

日常启停：`docker start rmq5namesrv rmq5broker` → 等 1 分钟 → `docker start rmq5proxy`（顺序同上）。
检查：`docker ps` 三容器 Up、宿主机 8081 端口监听；proxy 反复退出 = 起早了，重启它即可。

### 第 1 步 · build 镜像

```bash
cd c:\enlist\rag-processor
docker build -t rag-worker .
```

纯 pip 依赖（gRPC 客户端无 C++ 库，不需要代理 build-arg）。`.dockerignore` 挡住 .env/.dockerenv/data/rag_env/.git，密钥不进镜像。

### 第 2 步 · 起 worker 容器

```bash
docker rm -f rag-worker 2>/dev/null
MSYS_NO_PATHCONV=1 docker run -d --name rag-worker --env-file .dockerenv \
  -v "c:\enlist\rag-processor\data\dead_letter:/app/data/dead_letter" \
  rag-worker
docker logs rag-worker
```

- **检查点**：日志出现 `流模式启动: topic=... endpoint=host.docker.internal:8081 → collection=...` 后静默 = 正常（空转收批不刷日志）
- `.dockerenv`：`Rocketmq_Endpoint=host.docker.internal:8081`（容器 → 宿主机 8081 → 本地 proxy），Milvus/TEI/LLM 为线上地址
- dead_letter **必须挂卷**——否则死信写在容器文件系统里，第 3 步场景 E 验不到
- `MSYS_NO_PATHCONV=1` 仅 Git Bash 需要（防 Windows 路径被转义成 `/c/...`），PowerShell/CMD 不加
- 扩容 = 同参数再起一个容器（同 consumer group 自动分摊）

### 第 3 步 · 跑测试数据（宿主机）

```powershell
rag_env\Scripts\Activate.ps1       # 激活虚拟环境（PowerShell；不激活则下行直接写全解释器路径）
python .\e2e_test.py               # 全量 62 行，约 3~5 分钟；快速冒烟加 --limit 8
# 免激活等价写法：rag_env\Scripts\python.exe .\e2e_test.py
# CMD 激活：rag_env\Scripts\activate.bat；Git Bash：source rag_env/Scripts/activate
```

脚本四步：[1/4] 发 62 行 + 坏 JSON → [2/4] 轮询线上库等行数稳定 → [3/4] 逐行验契约 → [4/4] 重发第 1 行验幂等 + 死信核验。

**4/4 PASS 判读标准**：

| 项 | 期望 |
|---|---|
| 有入库行 | ~18/62（约 44 行被判广告/非新闻/无正文丢弃，属正常判定非 bug） |
| 契约违规 | 0（id=md5(url)、published_date 定长 ISO、tags 非空、region 省级标准名、text 无 HTML 标签） |
| 重复 url 幂等 | 1 行 |
| 坏 JSON 死信 | `data/dead_letter/` 出现 invalid_json |

容器侧同步看波统计：`docker logs rag-worker 2>&1 | grep records`，应有若干波 `[stream] records=... inserted=... ad=...`；重发那条对应 `records=1 dedup_skipped=1`（去重跳过，不花 LLM 钱）。

### 第 4 步 · 清理

```bash
python e2e_test.py --cleanup       # 删线上 e2e- 前缀测试行（自动复验残留）
docker rm -f rag-worker
```

### 常见问题

| 现象 | 处理 |
|---|---|
| 容器日志刷 `receive 失败: 50001` | proxy 挂了/起早了：按第 0 步顺序重启 proxy；worker 自动恢复，无需重启容器 |
| e2e 卡 0/62 不动 | `docker logs rag-worker` 看 Milvus/TEI/LLM 连接报错（.dockerenv 线上地址是否可达） |
| 场景 E FAIL（死信没验到） | 第 2 步 dead_letter 挂载漏了 |
| 容器起来即退 | `docker logs rag-worker` 看报错；多半漏了 `--env-file .dockerenv` |

不想要容器时整条链路可退化到宿主机直跑：终端 1 `python worker.py`（.env 已配 `Rocketmq_Endpoint=localhost:8081`）、终端 2 `python e2e_test.py`。

文件模式重放：把批次文件夹放回 `data/running/`（或把文件放回对应阶段目录）再跑该阶段即可，upsert 幂等。死信重放：取出 `item`/`record` 字段写成输入文件放 `data/inbox/`。

## 代码结构（按阶段分 folder）

```
clean/              阶段1：抽取与归一化
  loader.py         多格式输入读取（json/jsonl/xlsx/txt/html → 规范 record；canon_row 供流路径复用）
  extractor.py      record → NewsItem（trafilatura / 纯文本直通 / 时间归一化 / node_id）
  run.py            文件模式入口：收编批次 + sweep + CLI
enrich/             阶段2：LLM 判定 + 打标 + 结构化字段抽取
  run.py            prompt/约束解码/白名单 + 跨记录并发池 + dedup_enrich/apply_verdict 公共核心 + sweep + CLI
index/              阶段3：组装与入库
  run.py            TextNode 组装（契约 text 格式）+ build_node + upsert + 完成批次清理 + CLI
common/             跨阶段共享
  stream.py         流模式：RocketMQ 5.x gRPC 直连消费（SimpleConsumer 收一批即一波，ack 后置）
  batch.py          文件模式批次管理：收编 / 枚举 / 完成检查 / 整批清理
  config.py         环境变量集中读取
  schema.py         Milvus 显式 schema（15 列）+ BM25 + TTL
  store.py          llama-index upsert 路径 + 查重
  tags.py           tag_collection 只读缓存（TTL 自动刷新）
  embed.py          TEI 封装
  utils.py          目录扫描/_done 归档/jsonl/死信/批次名清洗
worker.py           总入口：按配置分流模式 / 文件模式
e2e_test.py         流模式端到端测试：pipeline.xlsx 真实行当用例 → 推 MQ → 验收线上 Milvus
create_collection.py 建表/校验 CLI（--drop 重建）
```

改某个阶段只动对应 folder；新增过滤字段走 D9 模式：enrich/run.py 的 prompt+约束解码加封闭词表项 → common/schema.py 占列（已建表需 --drop 重建）→ index/run.py metadata 加一行。

## 环境变量（.env / .dockerenv）

见文件内注释。关键项：

| 变量 | 默认 | 说明 |
|---|---|---|
| `Milvus_Collection_Name` | product_knowledge_base | 与消费端硬绑定，勿改 |
| `Embed_Dim` | 1024 | 随 embedding 模型定，改=重建 collection |
| `Monitored_Entities` | 空 | 监测主体（逗号分隔）；空=不做相关性过滤 |
| `LLM_Concurrency` | 16 | 并发判定数，按 GPU 吞吐调 |
| `LLM_Max_Tokens` | 4096 | 单次判定调用上限（含推理模型思维链；太小思维链烧光预算 content 为空） |
| `Rocketmq_Endpoint` / `Rocketmq_Topic` / `Rocketmq_Group` | 空 / yqms_thirdparty_push / rag-worker | 流模式开关与连接；endpoint 是 Proxy gRPC 地址（:8081），留空=文件模式。开启 ACL 时补 `Rocketmq_AccessKey`/`Rocketmq_SecretKey` |
| `Milvus_TTL_Days` | 180 | 数据保留期 |
| `Dead_Letter_Dir` | data/dead_letter | 死信目录；流模式容器给这里挂卷 |
