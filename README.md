# Webhook 事件接收与重放 API

基于 FastAPI + SQLite 的 Webhook 事件接收服务:按 `event_id` 幂等接收、记录首次接收顺序,支持时间范围分页查询与批量重放。

## 启动

```bash
docker compose up --build
```

服务监听 **8000** 端口(SQLite 数据持久化在 named volume `webhook-data`)。

不用 compose 也可以:

```bash
docker build -t webhook-replay . && docker run -p 8000:8000 webhook-replay
```

本地开发:`pip install -r requirements.txt && uvicorn app.main:app --port 8000`

## 示例 curl

### 1. 接收事件(幂等)

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","occurred_at":"2026-09-18T09:00:00Z","payload":{"a":1}}'
# 首次: 201 {"status":"created","event_id":"evt-1"}
# 重复: 200 {"status":"duplicate","event_id":"evt-1"}  —— 不产生重复记录
```

### 2. 按时间范围分页查询

```bash
curl 'http://localhost:8000/events?from=2026-09-18T09:30:00Z&to=2026-09-18T11:30:00Z&page=1&page_size=50'
```

返回 `total / page / page_size / items`,每条含 `seq`(首次接收顺序号)、`event_id`、`occurred_at`、`payload`、`received_at`。`from`/`to` 均可省略。

### 3. 批量重放

```bash
curl -X POST http://localhost:8000/replay \
  -H 'Content-Type: application/json' \
  -d '{"event_ids":["evt-3","evt-1","evt-404"]}'
```

```json
{
  "replayed": 2,
  "skipped": 1,
  "results": [
    {"event_id": "evt-1", "status": "replayed", "event": {"seq": 1, "...": "..."}},
    {"event_id": "evt-3", "status": "replayed", "event": {"seq": 2, "...": "..."}},
    {"event_id": "evt-404", "status": "skipped", "reason": "not_found"}
  ]
}
```

命中的事件**按首次接收顺序(`seq`)输出**,与请求顺序无关;未命中的标记 `skipped`;请求中重复的 `event_id` 只输出一次。

### 4. 完整性校验

```bash
curl http://localhost:8000/integrity
```

```json
{"verified": true, "checked": 3, "first_break": null}
```

从最早记录开始逐条校验哈希链:**序号连续性**(相邻 `seq` 逐一递增)、**前后哈希衔接**(每条 `prev_hash` 等于上一条的 `hash`,首条 `prev_hash` 为空串)、**事件字段**(按固定 JSON 规范重算哈希比对)。空库返回 `verified=true`、`checked=0`。

发现篡改时返回首个断点的 `seq`、`event_id` 和原因(`seq_gap` / `prev_hash_mismatch` / `hash_mismatch`),`checked` 为断点前已通过校验的记录数:

```json
{"verified": false, "checked": 1, "first_break": {"seq": 2, "event_id": "evt-2", "reason": "hash_mismatch"}}
```

### 5. 消费游标:拉取并租用事件

每个 consumer 独立维护确认游标,按首次接收顺序(`seq` 从 1 开始)拉取尚未确认的连续事件。`limit`(默认 100,1~1000)与 `lease_seconds`(默认 30,1~86400)均可省略,甚至可以不带 body:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/pull \
  -H 'Content-Type: application/json' \
  -d '{"limit":100,"lease_seconds":30}'
```

```json
{
  "consumer_id": "worker-1",
  "events": [
    {"seq": 1, "event_id": "evt-1", "occurred_at": "...", "payload": {"a": 1}, "received_at": "..."},
    {"seq": 2, "event_id": "evt-2", "occurred_at": "...", "payload": {"a": 2}, "received_at": "..."}
  ],
  "lease_token": "NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk",
  "lease_expires_at": "2026-09-18T19:38:12.919089+00:00"
}
```

- **租约期内重复拉取**:原样返回**同一批次、同一 token、同一过期时间**(不续租),参数里的 `limit`/`lease_seconds` 不影响结果。
- **租约过期后**:同一批次可被重新投递(沿用原区间,换发**新 token**);旧 token 立即作废。
- **空队列**:返回 `{"events":[],"lease_token":null,"lease_expires_at":null}`,**不创建租约**;之后新事件到达可继续拉取。
- 首次出现的 `consumer_id` 自动建档,游标为 0,即首批从 `seq=1` 开始。

### 6. 消费游标:确认批次

只能确认**当前未过期租约**的整批事件,确认后游标推进到本批次末尾,下次 `pull` 从下一个 `seq` 开始:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/ack \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk"}'
```

```json
{"consumer_id": "worker-1", "acknowledged": true, "cursor_seq": 2}
```

以下情况一律返回 **409**(游标保持不变,等待租约过期后重新投递):

- token 与当前租约不一致(**伪造**或**旧 token**——旧批次过期后已被新租约取代);
- 租约**已过期**(过期批次可重投,旧 token 不得再确认);
- 当前没有租约(从未拉取、已确认完毕、或上次拉取是空批次),包括未知 consumer。

游标只能随整批确认前移,因此**禁止越序确认**:批次内的任何一条都无法被单独跳过或提前确认。

一个典型的拉取-确认循环:

```bash
# 1) 租用
BATCH=$(curl -s -X POST http://localhost:8000/consumers/worker-1/pull \
  -H 'Content-Type: application/json' -d '{"limit":100,"lease_seconds":30}')
TOKEN=$(printf '%s' "$BATCH" | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])')
# 2) 处理 $BATCH ...
# 3) 确认
curl -X POST http://localhost:8000/consumers/worker-1/ack \
  -H 'Content-Type: application/json' -d "{\"lease_token\":\"$TOKEN\"}"
```

## 哈希链规范

每条记录写入时在同一事务内计算并存储两个字段:

- `prev_hash`:上一条记录的 `hash`(首条为空串 `""`)
- `hash`:对以下文档做**固定 JSON 规范化**(键排序、紧凑分隔符 `,`/`:`、非 ASCII 不转义)后的 SHA-256 十六进制:

```json
{"seq":1,"event_id":"evt-1","occurred_at":"...","payload":{...},"received_at":"...","prev_hash":""}
```

重复 `event_id` 的投递走幂等返回,不会写入记录,链保持不变。任何对历史记录的字段篡改、删除或重排都会被 `/integrity` 在首个断点处检出。

**旧库迁移**:启动时自动为已有 SQLite 数据补加 `prev_hash`/`hash` 列并按首次接收顺序回填整条链,无需手工操作;迁移可重复执行(幂等)。

## 设计要点

- **幂等**:`event_id` 上有 `UNIQUE` 约束,重复插入触发 `IntegrityError` 后返回 200 `duplicate`,并发下也不会产生重复记录。
- **接收顺序**:自增主键 `id` 即首次接收顺序,重放按它排序。
- **完整性哈希链**:每条记录引用上一条哈希,写入与链字段更新在同一事务完成(`BEGIN IMMEDIATE` 保证并发下链头一致);`GET /integrity` 全链校验。
- **时间比较**:`occurred_at` 归一化为 UTC ISO8601 字符串存储,范围查询走字典序比较,并建有索引。
- **消费游标与租约**:每个 consumer 一行(`consumers` 表),保存已确认游标与当前租约(token、批次起止 seq、过期时间)。`pull` 在 `BEGIN IMMEDIATE` 事务内完成"判租约 → 取批次 → 写租约",与事件写入互斥,并发重复拉取只会得到同一个租约;`ack` 仅推进整批游标并清空租约,token 不符、过期或无租约一律 409。
- **配置**:数据库路径由环境变量 `DB_PATH` 控制(容器内默认 `/data/events.db`)。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/events` | 接收单个事件(幂等) |
| GET | `/events` | 时间范围分页查询(`from`/`to`/`page`/`page_size`) |
| POST | `/replay` | 批量重放,按首次接收顺序输出 |
| GET | `/integrity` | 校验完整性哈希链,返回 `verified`/`checked`/`first_break` |
| POST | `/consumers/{consumer_id}/pull` | 按首次接收顺序拉取并原子租用未确认事件(`limit`/`lease_seconds`) |
| POST | `/consumers/{consumer_id}/ack` | 确认当前租约批次并推进游标;过期/伪造/旧 token 返回 409 |
| GET | `/health` | 健康检查 |

交互式文档:启动后访问 `http://localhost:8000/docs`。
