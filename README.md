# rag-processor

舆情知识库**构建端**：把上游舆情系统推送的新闻加工成 Milvus 向量库（collection `product_knowledge_base`），供消费端（FastAPI+Vue，另一仓库）检索。

架构依据见 `data/架构决策记录-2026-08-26.md`，字段/格式硬约束见 `data/知识库构建规范-消费端数据契约.md`，推送协议见 `data/舆情监测系统数据推送接口文档.md`（均在 data/ 目录，不受版本控制，改动前先读）。

## 架构（worker 直连 RocketMQ，三阶段处理核心，无中间存储依赖）

```
舆情系统 ──推──> RocketMQ ──> worker（N 实例，同一 consumer group，Linux 容器）
  (yqms_thirdparty_push)       │ 消费回调：JSON 解析 + clean（毫秒级）
                              │     └─ 记录入波缓冲，回调阻塞等待
                              │ 波线程：URL 去重 → enrich 并发池（LLM 判定/打标/region）
                              │     → index（embedding + upsert Milvus）
                              │     → 逐条唤醒回调 → index 成功才 ack
                              ▼
                     Milvus product_knowledge_base（最终库）
```

**可靠性全部交给 RocketMQ + Milvus，没有 MinIO/中间暂存层**：

- **ack 后置**：消息处理完（index upsert 成功）才返回 CONSUME_SUCCESS；基础设施故障（Milvus/TEI/LLM 网关不可达）返回 RECONSUME_LATER 由 MQ 重投。node_id=md5(url) 去重 + upsert 幂等保证重投无害（at-least-once）
- **积压在 broker 上**：处理速度受 LLM 限制（每条新闻一次调用），消费线程阻塞等波处理即天然背压——线程池满后客户端停止拉取，MQ 本身就是持久缓冲。**水平扩展 = 同 group 多起几个 worker 容器**（competing consumer，队列自动分摊），无需协调。注意**单实例在飞上限由消费线程数与 topic 队列数共同决定**（C++ 客户端按队列派发；实测 8 队列时并发被压在 8，扩到 16 队列后 62 条突发一波入齐）——高吞吐场景确认上游 topic 的队列数足够
- **worker 无需固定盘**：全程内存处理，崩溃由 MQ 重投恢复；唯一要挂卷的是死信目录（见下）

**两条到达路径共用同一套处理核心**（extract_record / dedup_enrich / build_node+upsert），配置决定走哪条：

| 路径 | 触发条件 | 说明 |
|---|---|---|
| 流模式（生产） | `Rocketmq_NameSrv` 非空 | 直连消费 RocketMQ；binding 为 C++ 库（librocketmq）仅 Linux，**必须容器内运行** |
| 文件模式（开发/手工导入） | `Rocketmq_NameSrv` 留空 | 本地 `data/inbox/` 批次文件夹三阶段（目录即状态，阶段可独立重跑，`--once` 排空） |

**死信**：单条级终态失败（非法 JSON、无发布时间、LLM 判定失败）写 `data/dead_letter/`（带阶段与 reason，可重放）；基础设施故障不写死信、走 MQ 重投。流模式生产部署给死信目录挂卷即可持久——这是容器对磁盘的唯一要求。

**丢弃（设计行为，只计数）**：广告/非新闻/不相关/抽不出正文。enrich 产物含被弃记录的 verdict 供审计。

## 处理核心（两条路径共用）

- **clean**：`canon_row` 别名映射（协议字段 infoRegion/publishTime/webName 等在这一步对齐）→ `extract_record`：trafilatura 抽正文（有 html 时）、published_date 全代码归一化为东八区定长 ISO（支持协议的 Long 秒时间戳）、`node_id = md5(url)`；全链无时间 → 死信
- **enrich**：每条新闻一次 LLM 调用（判定 is_ad/is_news/relevant + tags ≤3 + region 省级封闭词表），`LLM_Concurrency` 限并发；URL 去重在 LLM 之前（Milvus 查已入库 node_id 直接跳过，省调用费）；response_format=json_object（vllm 下 guided_json，不支持自动降级）；超长正文（存储 text 超 6 万字节预算）判定通过后追加一次摘要调用替换 content（契约 §4.1 摘录优先），失败退回 index 字节截断兜底
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
| RocketMQ（上游集群） | 数据到达 | 协议见 data/ 接口文档；4.x remoting，多 NameServer 分号分隔 |
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

# 文件模式（宿主机开发；Rocketmq_NameSrv 留空）
python worker.py                   # 常驻轮询本地批次
python worker.py --once            # 排空当前批次后退出（cron 用）

# 独立运行某个阶段（文件模式回放/补跑）
python -m clean.run --once         # 只跑抽取（会先收编 inbox/ 新批次）
python -m enrich.run --once        # 只跑判定打标
python -m index.run --once         # 只跑入库 + 完成批次清理
```

容器部署（流模式生产形态；librocketmq 仅 Linux）：

```bash
docker build -t rag-worker .       # GitHub 直连不通时加 --build-arg GITHUB_PROXY=https://ghproxy.net/
docker run -d --name rag-worker --env-file .dockerenv \
  -v /data/rag-deadletter:/app/data/dead_letter \
  rag-worker                       # 流模式常驻；扩容 = 同参数再起一个（同 group 自动分摊）
```

端到端测试（用 `data/pipeline.xlsx` 真实样例当用例；须在 Linux 侧跑，借用镜像即可）：

```bash
MSYS_NO_PATHCONV=1 docker run --rm --env-file .dockerenv \
  -e Dead_Letter_Dir=/app/src/data/dead_letter \
  -v "c:\enlist\rag-processor:/app/src" \
  --entrypoint python rag-worker /app/src/e2e_test.py --namesrv host.docker.internal:19876
python e2e_test.py --cleanup      # 清掉线上库里的测试行（url 前缀匹配，独立执行）
```

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
  stream.py         流模式：RocketMQ 直连消费 + 凑波缓冲（ack 后置）+ 波处理
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
| `Rocketmq_NameSrv` / `Rocketmq_Topic` / `Rocketmq_Group` | 空 / yqms_thirdparty_push / rag-worker | 流模式开关与连接；NameSrv 多地址分号分隔，留空=文件模式 |
| `Milvus_TTL_Days` | 180 | 数据保留期 |
| `Dead_Letter_Dir` | data/dead_letter | 死信目录；流模式容器给这里挂卷 |
