# rag-processor

舆情知识库**构建端**：把爬取的网页 HTML 加工成 Milvus 向量库（collection `product_knowledge_base`），供消费端（FastAPI+Vue，另一仓库）检索。

架构依据见 `data/架构决策记录-2026-08-26.md`，字段/格式硬约束见 `data/知识库构建规范-消费端数据契约.md`（均在 data/ 目录，不受版本控制，改动前先读）。

## 架构（单 worker 三阶段，批次文件夹制，LLM 只出现一次）

沿用原流水线的阶段词汇 clean/enrich/index（chunk 已按决策 D2 砍掉）：

```
MinIO raw/<batch>/（可选）──┐   一个批次 = 一个文件夹，三阶段依次消费
data/inbox/<batch>/（本地）─┤
                           ▼
data/running/<batch>/inbox ──[clean]→ 1_cleaned ──[enrich]→ 2_enriched ──[index]→ Milvus
                           │                一次 LLM 调用               组装+embedding
                           │                判定+打标+region             upsert
                           ▼
        批次三目录全部消费完 → 删除 MinIO 批次前缀 → 删除 running/<batch>/ 整个目录
```

**状态机：文件位置即状态，无额外成功标记**——文件在阶段目录=待处理；移入 `_done/`=该阶段成功；enrich/index 失败文件原地保留（下轮重试，upsert 幂等所以重跑无害）；三目录全空=批次完成，整批清理。**清理顺序先远端（MinIO）后本地**：远端删失败则本地保留，下轮重试，绝不会出现"本地已删、远端残留→整批重跑白花 LLM 钱"。

- clean 抽正文（trafilatura）+ 归一化；enrich 一次 LLM 调用（判定+打标+region 抽取，约束解码）；index 组装节点入库；
- URL 去重在 enrich 之前（Milvus 查 md5(url)，已入库直接跳过，省 LLM）；
- 广告/非新闻/不相关/抽不出正文 → 丢弃计数（设计行为）；LLM 失败/无发布时间/文件损坏 → `data/dead_letter/`（带批次与阶段标记，**不随批次清理删除**，修复后可重放）。

## 批次投递

两种等价方式，可同时用：

| 方式 | 约定 | 说明 |
|---|---|---|
| MinIO（生产推荐） | `{Minio_Bucket}/{Minio_Prefix}/<batch_id>/**` | 爬取侧上传，如 `rag/raw/20260826_0900/*.json`；worker 自动拉取新批次 |
| 本地目录 | `data/inbox/<batch_id>/**`（子目录）或 `data/inbox/<文件>`（散文件，文件名=batch_id） | 测试/手工导入 |

batch_id 建议用时间戳类命名（`20260826_0900`）；非法字符自动替换为 `_`。

## 输入格式（按扩展名解析）

| 扩展名 | 解析方式 | 说明 |
|---|---|---|
| `.json` | 单对象或 JSON 数组 | 爬取侧标准投递格式 |
| `.jsonl` | 每行一个对象 | 大文件流式友好 |
| `.xlsx` | 每行一条记录 | 列名自动映射（见下），需要 `html` 或 `content` 列之一 |
| `.txt` | 单文件单篇 | 首个非空行=标题，其余=正文；url 用 `txt://文件名` |
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
| `region` | 地区/发生地 | 否 | 爬取侧自带合法省级名时优先于 LLM 抽取 |

published_date 归一化为定长 `YYYY-MM-DDTHH:MM:SS`（契约 §6.1，东八区口径），支持 `2026-01-19 16:14:05`、`2026/1/19`、`2026年1月19日 9:05`、ISO 带时区、Unix 时间戳（秒/毫秒）、Excel 日期单元格等。

## 阶段间数据格式

- `running/<batch>/1_cleaned/*.jsonl`：`{node_id, url, title, content, published_date, source, region?}`（node_id = md5(url)）
- `running/<batch>/2_enriched/*.jsonl`：上者 + `verdict: {keep, drop_reason, tags[], region, why}`（含被判弃记录，供审计）
- `data/dead_letter/{时间}_{阶段}_{批次}_{文件}.jsonl`：每行 `{stage, reason, ...原始数据}`，批次清理不影响死信

## 依赖服务

| 服务 | 用途 | 说明 |
|---|---|---|
| Milvus ≥2.5 | 内容库 + tag_collection | 服务端 BM25(jieba+cnalphanumonly)，HNSW/IP，TTL 180 天 |
| TEI | Qwen3-Embedding-0.6B (1024维) | **必须与消费端同一模型**，换模型=跨端协调 |
| vllm | Qwen3.6-35B-A3B 判定+打标 | guided_json 约束解码（不支持时自动降级白名单校验） |
| MinIO（可选） | 批次源 | 不配置则纯本地目录模式 |

clean 阶段完全本地无外部依赖；enrich 需 vllm+Milvus(去重查询)+tag_collection；index 需 Milvus+TEI。

## 使用

```bash
pip install -r requirements.txt

python create_collection.py        # 建表/校验（--drop 删除重建）
python worker.py                   # 常驻：收编批次 → 三阶段 → 清理
python worker.py --once            # 排空当前批次后退出（cron 用）

# 独立运行某个阶段（回放/补跑）
python -m clean.run --once         # 只跑抽取（会先收编 inbox/MinIO 新批次）
python -m enrich.run --once        # 只跑判定打标
python -m index.run --once         # 只跑入库 + 完成批次清理
```

回放：把批次文件夹放回 `running/`（或把文件放回对应阶段目录）再跑该阶段即可，upsert 幂等。

## 代码结构（按阶段分 folder）

```
clean/              阶段1：抽取与归一化
  loader.py         多格式输入读取（json/jsonl/xlsx/txt/html → 规范 record）
  extractor.py      record → NewsItem（trafilatura / 纯文本直通 / 时间归一化）
  run.py            入口：收编批次 + sweep + CLI
enrich/             阶段2：LLM 判定 + 打标 + 结构化字段抽取
  run.py            prompt/guided_json/白名单 + URL 去重 + sweep + CLI
index/              阶段3：组装与入库
  run.py            TextNode 组装（契约 text 格式）+ upsert + 完成批次清理 + CLI
common/             跨阶段共享
  batch.py          批次管理：收编 / 枚举 / 完成检查 / 整批清理
  minio_source.py   MinIO 批次源（拉取 + 完成后删前缀，可关闭）
  config.py         环境变量集中读取
  schema.py         Milvus 显式 schema（15 列）+ BM25 + TTL
  store.py          llama-index upsert 路径 + 查重
  tags.py           tag_collection 只读缓存
  embed.py          TEI 封装
  utils.py          目录扫描/_done 归档/jsonl/死信
worker.py           总入口：顺序驱动三阶段
create_collection.py 建表/校验 CLI（--drop 重建）
```

改某个阶段只动对应 folder；新增过滤字段走 D9 模式：enrich/run.py 的 prompt+guided_json 加封闭词表项 → common/schema.py 占列（已建表需 --drop 重建）→ index/run.py metadata 加一行。

## 环境变量（.env）

见 `.env` 文件内注释。关键项：

| 变量 | 默认 | 说明 |
|---|---|---|
| `Milvus_Collection_Name` | product_knowledge_base | 与消费端硬绑定，勿改 |
| `Embed_Dim` | 1024 | 随 embedding 模型定，改=重建 collection |
| `Monitored_Entities` | 空 | 监测主体（逗号分隔）；空=不做相关性过滤 |
| `LLM_Concurrency` | 16 | 并发判定数，按 GPU 吞吐调 |
| `Milvus_TTL_Days` | 180 | 数据保留期 |
| `Minio_Endpoint` | 空 | 留空=纯本地模式；填 host:port 启用 MinIO 批次源 |
| `Minio_Delete_On_Done` | true | 批次完成是否删远端前缀；**建议 false + MinIO 生命周期策略保留原始数据**，将来换 embedding/重建 collection 可离线重跑 |

容器运行：`docker run --env-file .dockerenv -v $(pwd)/data:/app/data <image>`。
