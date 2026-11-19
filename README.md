# lab.test 主从权威 DNS（Python / FastAPI / SQLite）

网络管理员批量更新 `lab.test` 仪器地址的最小可用主从权威 DNS：

- **主节点**（`dnslab/master_app.py`）：通过 HTTP API（Swagger UI 在 `/docs`）维护 A 记录全集；
  每次发布（单条增删改或整体批量替换）在**一个 SQLite 事务**内完成记录变更、
  SOA 序列号递增（从 1 开始每次 +1）、并落一份带 SHA-256 校验和的**不可变快照**。
- **从节点**（`dnslab/slave_app.py`）：经 HTTP 从主节点拉取整份快照，校验通过且序列号
  严格更新后才生效；以 UDP 回答 `A` / `SOA` 查询，对域内不存在的名字回答 `NXDOMAIN`
  （authority 带本版 SOA），域外查询 `REFUSED`。

## 一致性保证

| 场景 | 行为 |
|---|---|
| 发布与拉取交错 | 快照是主节点单事务读出的不可变版本；从节点要么整体应用某版本，要么不应用 |
| 并发同步 | 从节点同步串行化；应用前在锁内复查序列号，最终版本一定是见过的最大序列号 |
| 中途断流 / 数据被改 | 传输中断 → 拉取报错；SHA-256 校验失败 → 拒绝；现役版本不受影响 |
| 迟到旧版 | 序列号 ≤ 现役版本的一律拒绝（`applied: false`），不会覆盖新版 |
| 记录与 SOA 同版 | 每条应答都由同一个不可变 `ZoneData` 对象生成，SOA 序列号与记录同属一版 |
| 主节点失联 | 从节点继续用最后一次成功同步的版本应答；`/sync` 报 502 但不影响应答 |
| 从节点重启 | 启动时从 SQLite 恢复最后成功版本，无需先同步即可应答 |
| 初次同步失败 | 没有任何版本时回答 `SERVFAIL`（不可用），绝不凭空应答 |

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build
```

- 主节点 API / 文档： http://localhost:8000/docs
- 从节点 API / 文档： http://localhost:8001/docs
- 试跑服务 API / 文档： http://localhost:8002/docs
- 从节点 DNS： `dig @127.0.0.1 -p 5353 host1.lab.test A` ／ `dig @127.0.0.1 -p 5353 lab.test SOA`

从节点数据在命名卷 `slave-data` 中，`docker compose restart slave` 后仍用最后版本应答。

### 本地（无 Docker）

```bash
pip install -r requirements.txt
MASTER_DB=/tmp/master.db uvicorn dnslab.master_app:app --port 8000 &
SLAVE_DB=/tmp/slave.db MASTER_URL=http://127.0.0.1:8000 DNS_PORT=5353 \
  uvicorn dnslab.slave_app:app --port 8001 &
```

## 使用（均可在 `/docs` 中操作）

```bash
# 批量发布仪器地址（一次事务、一个新序列号）
curl -X POST http://localhost:8000/records/bulk -H 'Content-Type: application/json' -d '{
  "records": [
    {"name": "host1.lab.test", "ip": "10.0.0.1"},
    {"name": "host2.lab.test", "ip": "10.0.0.2", "ttl": 60}
  ]}'

# 单条增改 / 删除
curl -X PUT  http://localhost:8000/records/host3.lab.test -H 'Content-Type: application/json' -d '{"ip": "10.0.0.3"}'
curl -X DELETE http://localhost:8000/records/host2.lab.test

# 查看主节点现役序列号 / 完整快照
curl http://localhost:8000/zone/serial
curl http://localhost:8000/zone/snapshot

# 从节点：发起同步、查看现役序列号
curl -X POST http://localhost:8001/sync -H 'Content-Type: application/json' -d '{}'
curl http://localhost:8001/status
```

`POST /sync` 可选参数：`{"serial": N}` 拉取指定历史版本（主节点保留最近
`KEEP_VERSIONS=100` 版，主要用于测试迟到旧版）；`{"url": ...}` 覆盖快照来源。
设 `SYNC_INTERVAL=秒数` 可让从节点周期自动同步（默认 0，仅手动）。

## 从节点试跑（load test）

`loadtest` 服务（`dnslab/loadtest_app.py`，`/docs` 在 8002 端口）只向**同一套部署里的从节点**
发 UDP `A` 查询，用来观察从节点在给定查询速率下能否跟上。目标是固定的
`SLAVE_DNS_HOST`/`SLAVE_DNS_PORT`（Compose 里就是 `slave:5353`），请求参数无法改指向。

在 http://localhost:8002/docs 打开 `POST /run`（“Try it out”），填写：

| 字段 | 含义 | 限制 |
|---|---|---|
| `name` | 要查询的域名（只发 A 查询） | 必须是合法域名 |
| `qps` | 每秒查询数（固定节拍，间隔 1/qps 秒） | ≤ 200 |
| `duration_seconds` | 发包秒数 | ≤ 30 |
| `max_in_flight` | 在途上限：已发出、还没按时应答/超时的查询数 | 1 – 6000 |

一轮的规则：

- **节拍固定**：第 i 条排在开始后 `i / qps` 秒。错过一个发包间隔（落后超过 1/qps 秒）
  或在途已达上限，该节拍记为**未发**，跳过且不补发。
- **500 ms 窗口**：每条查询只等到**计划时刻**（不是实际发出时刻）后 500 ms。迟到的应答
  没有对应在途查询可记，既不算应答、也不会算给后面的查询（每轮每条查询的 DNS 报文 ID
  各不相同）。
- **一次一轮**：另一轮在跑时 `POST /run` 返回 409。

`POST /run` 阻塞到本轮结束（最长约 30.5 秒）并返回报告，也可以 `GET /report` 取最近一轮。
报告字段：`planned_queries`、按时应答总数 `on_time_answered` 及按 RCODE 的
`rcode_counts`、`timeouts`、`not_sent`（附 `interval_missed` / `in_flight_full` / `send_error`
细分），三者之和恒等于 `planned_queries`（`accounting_ok`）；`diagnostics` 另列出迟到重复、
未知、畸形应答的数量，供排查用。`GET /status` 显示是否在跑。

说明：试跑容器与应用容器共用同一个镜像，从节点没起好时仍可填参数并看到全部超时；
UDP 无连接，真正的“目标不可达”只体现为超时。

## 自测

```bash
python tests/selftest.py    # 需要已安装 requirements.txt
```

脚本拉起真实主从进程，覆盖：初次同步失败报不可用、发布/同步/应答、批量更新原子切换、
迟到旧版拒绝、断流与校验和损坏、并发发布+同步交错（序列号单调、最终内容逐条一致）、
主节点失联、从节点重启、新从节点+主失联、`/docs` 可用性与主恢复后的同步。

试跑服务单独自测（用可控延迟/丢包的假从节点，检查迟到应答、在途满载、RCODE 分类与
“应答+超时+未发=计划数”）：

```bash
python tests/loadtest_selftest.py
```

## 目录

```
dnslab/dnsproto.py      最小 DNS 线格式编解码与应答逻辑（仅标准库）
dnslab/master_app.py    主节点 FastAPI 应用
dnslab/slave_app.py     从节点 FastAPI 应用 + UDP DNS 服务
dnslab/loadtest_app.py  从节点 UDP 试跑服务（固定节拍 + 500 ms 窗口 + 报告）
tests/selftest.py       主从端到端自测
tests/loadtest_selftest.py  试跑服务自测（假从节点可注入延迟/丢包/RCODE）
Dockerfile / docker-compose.yml
```

## 已知限制

- 仅 UDP、仅 `A`/`SOA`/`ANY` 查询；不支持 TCP、EDNS、IXFR/AXFR、DNSSEC、NOTIFY。
- 序列号比较用简单“严格大于”（本系统序列号从 1 单调 +1，未实现 RFC 1982 回绕比较）。
- 快照为全量传输，适合 lab 规模；超大区域需增量机制。
- 从节点应用快照时的 SQLite 写入在事件循环内完成，记录数极大时会短暂阻塞循环（lab 规模无感）。
- 主节点 `/debug/snapshot/truncated`、`/debug/snapshot/corrupt` 仅用于故障注入测试，生产应移除。
- 试跑服务按固定节拍由事件循环 `sleep` 驱动，宿主机或容器严重卡顿时部分节拍会记为未发
  （这是设计上的如实记录，不是重试）；测量结果是端到端的，含本机 UDP 栈与容器网络延迟。
