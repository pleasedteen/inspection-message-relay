# 机房运维单机巡检消息服务

多个调度进程可独立发布设备巡检请求，各巡检组（订阅）独立订阅、领取、确认。
TypeScript + Node.js 实现，持久化使用 **Node 22 内置 SQLite（`node:sqlite`，WAL + `synchronous=FULL`）**，
HTTP 与测试均只用 Node 内置能力，**运行时零第三方依赖**（仅开发期需要 `typescript` / `@types/node`）。

---

## 1. 30 秒跑起来

```bash
cd inspection-service
npm install        # 仅安装 TypeScript 开发依赖；若 node_modules 已存在可跳过
npm test           # 构建 + 一条命令跑完所有功能验证与 10 万条基准（本机约 10 秒，上限 80 秒）
```

或者直接执行交付的单命令脚本（缺依赖会自动安装）：

```bash
./check.sh
```

成功时末尾输出：

```
✅ 全部通过：29 个检查组 · 总耗时 8.x s（要求 < 80s）
✅ 基准断言通过：领取计量条目在 0→100,000 积压间完全不变，热路径全部为索引 SEARCH。
```

> 环境要求：Node.js ≥ 22.5（内置 `node:sqlite`），在 Node v22.23.4 / Linux 验证。
> npm 脚本已带 `--experimental-sqlite --no-warnings`。

### 手动启动服务

```bash
npm run start -- --port 8787 --db ./data/inspection.db
# 或：node --experimental-sqlite --no-warnings dist/main.js --port 8787 --db ./data/inspection.db
```

---

## 2. 接口说明

所有接口返回 JSON；错误体为 `{"error":{"code","message","details?"}}`。
订阅标识 `:sub` 既可以是注册返回的 `id`，也可以是唯一的 `name`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/subscriptions` | 注册巡检组，body `{"name":"inspectors-a"}`；重名幂等，返回**原始** `effective_at` |
| GET | `/subscriptions/:sub` | 查询订阅与注册生效时间 |
| POST | `/messages` | 发布巡检请求（**必须**指定首次领取截止） |
| POST | `/subscriptions/:sub/claim` | 领取下一条（或指定设备的队头），可带租期与可选长等 `wait_ms` |
| POST | `/subscriptions/:sub/ack` | 用当前有效令牌确认 |
| POST | `/admin/cleanup` | **手动**清理：回收无保护消息正文，返回实际回收条数 |
| GET | `/admin/stats` | 消息/回执/队头计数 |
| GET | `/admin/waiters` | 长等等待者列表与领取尝试计数（`initial_attempts`/`wake_attempts` 等） |
| GET | `/admin/messages/:id` | 查看消息正文状态及各订阅回执（排障） |
| GET | `/admin/query-plans` | 领取/清理热语句的真实 `EXPLAIN QUERY PLAN` |

### 2.1 发布：指定首次领取截止

`POST /messages`

```jsonc
{
  "device_id": "pump-01",            // 必填，同一设备的并发发布有统一顺序
  "payload": { "tasks": ["temp"] },  // 任意 JSON
  // 二选一：
  "first_deadline_at": 1795682683100,// 绝对 epoch 毫秒（或 ISO 字符串）
  "first_claim_ttl_ms": 5000,        // 或相对有效期
  "idempotency_key": "sched-1-17"     // 可选，发布端幂等
}
```

返回 `seq`：**同一 `device_id` 内连续、唯一、按提交顺序确定的统一顺序号**。

### 2.2 领取与租期

`POST /subscriptions/inspectors-a/claim?meter=1`

```json
{ "lease_ms": 4000, "device_id": "pump-01" }
```

- `lease_ms` 默认 30s，下限 50ms，上限 10min；`device_id` 可省略（claim-any）。
- 有消息：`200 {"claim":{ ...message, "lease_token","lease_expires_at","delivery_count","redelivery" }}`。
- 无消息：`{"claim":null,"blocked":{...LEASE_ACTIVE...}|null}`。
- `?meter=1` 时额外返回本次领取的存储访问计量（见第 5 节）。

规则：

1. **首次领取资格** = 回执 `pending` 且服务器时间 `< first_deadline_at`。
2. 首次截止到期后，仍 `pending` 的订阅在领取时会被索引清扫批量置为 `skipped`（等价于“跳过它”），不影响其他订阅。
3. 已领取（`claimed`）但未确认的回执，**无论首次截止是否已过**都保留重投权；`lease_expires_at <= now` 后可被同组任意工作者重领，下发新令牌、`delivery_count+1`。
4. 确认 `POST .../ack {"message_id","token"}` 是条件更新：必须 `state='claimed'`、令牌为当前令牌、且 `lease_expires_at > 服务器时间`。否则分别得到 `INVALID_TOKEN` / `LEASE_EXPIRED` / `ALREADY_ACKED` / `NOT_CLAIMED`（409）。

### 2.3 长等领取（可选长轮询，`wait_ms`）

`POST /subscriptions/:sub/claim` 接受可选的 `wait_ms`（毫秒，封顶 120s）；
**缺省或 `0` 即原来的立即领取语义，完全不变**。

```json
{ "lease_ms": 4000, "device_id": "pump-01", "wait_ms": 15000 }
```

- **有匹配队头立即领**：响应带 `waited_ms≈0`，与立即领取结果完全一致。
- **无活则挂起**，直到以下任一事件发生：① 新单发布为该组建立了新队头；
  ② 前项确认（ack）推进了队头；③ 阻塞队头的租约到期（按库中**绝对到期时刻**挂一次性定时器唤醒）；
  ④ 超时返回空：`{"claim":null,"blocked":...,"retry_at":...,"waited_ms":...}`。
- **断连即撤**：服务端发现客户端断连（进程被杀/网络断开）立即撤销该等待，
  撤销后不再为其做任何领取尝试；`GET /admin/waiters` 可见等待者与计数器。
- **定向与不限并存**：同组的定向（带 `device_id`）与不限设备等待可同时登记；
  事件只携带设备号精确唤醒匹配等待者，**无关设备变动不触发定向复查**；
  等待期间**不轮询数据库**（无任何后台查询，唤醒全部来自事件与到期定时器）。
- **尝试数恒定**：单个可领队头每次只唤醒一个等待者做一次领取尝试，
  领取尝试数不随等待人数增长；底层条件更新（`state`/`lease_expires_at` 守卫）
  保证**只发一份有效租约**。
- **重启语义**：等待表全在内存，进程异常退出即清空；租约/截止均为库中绝对时间，
  客户端重新长等后仍按**原租约到期时刻**重领，旧令牌不能确认新租约。

`GET /admin/waiters` 返回：

```json
{
  "now": 1795...,
  "waiters": [{ "sub_id": "...", "device_id": "pump-01|null", "waited_ms": 320, "remaining_ms": 14680 }],
  "stats": { "initial_attempts": 7, "wake_attempts": 2, "dispatches": 2, "wakes": 2, "timeouts": 1, "cancels": 0 }
}
```

其中 `wake_attempts` 是事件/定时器唤醒后的**真实领取尝试数**（空等时为 0，证明不轮询），
`initial_attempts` 是长等登记前的同步试领次数。

### 2.4 队头阻塞（同设备保序，其他设备放行）

每个 `(订阅, 设备)` 维护一个队头指针 `heads`，指向该订阅在该设备上最早的未决回执。

- 只要队头仍是 **有首次领取资格的 `pending`**，或 **已领取未确认的 `claimed`（含租约过期待重领）**，同设备后项不可领取。
- 首次截止过期 → 清扫为 `skipped`；或领取后 `ack` → 队头用局部索引跳到下一个未决项，释放后项。
- claim-any 按设备逐个查看队头：某设备队头租约有效时只阻塞**该设备**，会继续领取其他设备的队头。

### 2.5 手动清理

`POST /admin/cleanup`，以进入清理事务时取的**同一个判定时刻 `now`** 为准：

消息正文可回收，当且仅当 **不存在** 任何订阅满足：

- 仍可首次领取：`receipts.state='pending' AND messages.first_deadline_at > now`；或
- 已领取但未确认：`receipts.state='claimed'`（不看租约，过期租约仍可重投）。

满足后删除 `bodies` 行并把 `messages.body_reclaimed=1`，返回：

```json
{ "decided_at": 1795..., "reclaimed_bodies": 42, "candidate_messages": 42 }
```

`reclaimed_bodies` 是**实际删除的正文条数**；紧接着再调一次返回 `0`。
元数据与各订阅回执保留（保序/排障需要），正文删除后存储页可被 SQLite 复用。

---

## 3. 本地发布与订阅夹具

夹具源码在 `src/fixtures/`，编译产物在 `dist/fixtures/`。

### 3.1 发布夹具（模拟一个调度进程）

```bash
npm run fixture:publish -- \
  --url http://127.0.0.1:8787 \
  --scheduler-id sched-A --device pump-01 \
  --count 20 --concurrency 6 --ttl-ms 60000
```

多个该命令同时跑、打同一 `--device`，即“多个调度进程并发发布同一设备”。
末行 `@@SUMMARY@@{...,"seqs":[...]}` 给出该进程看到的顺序号，便于核对全局 `1..N` 连续唯一。
加 `--devices 4` 可把消息轮询打到 `pump-01-0..3` 多台设备。

### 3.2 订阅夹具（巡检组 / 组内工作者）

正常处理并确认（处理耗时 200ms）：

```bash
npm run fixture:subscribe -- \
  --url http://127.0.0.1:8787 \
  --name inspectors-a --max 5 --work-ms 200 --lease-ms 8000
```

领到 1 条后**不确认直接退出**，模拟断线/崩溃，用于观察租期重投。开三个终端：

```bash
# 终端 1：服务
npm run start -- --port 8787 --db ./data/demo.db

# 终端 2：先发布一条（首次领取截止给 10 分钟）
curl -s -X POST http://127.0.0.1:8787/messages \
  -H 'content-type: application/json' \
  -d '{"device_id":"pump-01","first_claim_ttl_ms":600000,"payload":{"x":1}}'

# 终端 3：“短命工作者”，领取后不 ack 立即退出（租期 1.5s）
npm run fixture:subscribe -- --name inspectors-a --mode drop --lease-ms 1500
# 约 1.5s 后再起一个订阅者：会看到 redelivery:true, delivery_count:2
npm run fixture:subscribe -- --name inspectors-a --max 1
```

关键事件以 `@@READY@@ / @@CLAIM@@ / @@ACK@@ / @@DONE@@` 行输出（含令牌、租期边界、投递次数）。
`--device pump-01` 可只处理一台设备；`--max Infinity` 持续轮询。

加 `--wait-ms 15000` 即切换为**长等领取**：没活时挂起等待（不再空返轮询），
超时空返输出 `@@WAIT@@{"empty":true,"waited_ms":...}` 行，随后进入下一轮长等；
`@@CLAIM@@` 中附带本次 `waited_ms`。

---

## 4. 80 秒测试命令覆盖了什么

```bash
npm test        # = tsc 构建 + node dist/test/run-all.js
```

编排器会在临时目录启动真实 HTTP 服务子进程，全部通过约 **8–12 秒**（含 10 万基准），覆盖：

1. **注册生效边界**：先发布的消息，后注册的组收不到；注册后的消息正常收。
2. **并发发布**：并发拉起 **3 个真实发布夹具进程**，每进程 20 条、进程内并发 6，打同一设备；
   断言三方顺序号并集恰为 `1..60`，订阅侧按 `1..60` 严格有序领取/确认。
3. **队头阻塞**：未确认的 h1 阻塞同设备 h2；claim-any 立即放行另一设备 o1；ack h1 后领到 h2。
4. **首次截止**：截止后未领取组回执变 `skipped`；已领取组保持 `claimed` 且可确认。
5. **断线重投**：drop 夹具领到后不 ack 退出；错误令牌 → `INVALID_TOKEN`；
   租期后重领 `redelivery=true, delivery_count=2`，旧令牌失效；
   新领取若拖过租期，即使令牌字符串没变，ack 也得 `LEASE_EXPIRED`；再次重领并确认成功。
6. **异常退出后重启**：`kill -9` 真实服务进程，以同一 DB 文件重启，断言：
   订阅与原始 `effective_at`、已确认不重投、未领取仍 `delivery_count=1`、
   已领取未确认回执的 `state/claim_count/lease_expires_at` **毫秒值完全一致**，
   越过崩溃前的绝对租期边界后可重投；首次截止的绝对时间同样原样恢复并在越界后跳过。
7. **手动清理**：claimed 与 pending-alive 正文保留；过期未领、全员已过截止/已确认的正文被实际回收；
   返回数 ≥2 且对应 `body_reclaimed=true`；第二次清理精确返回 0。
8. **长等领取**（可选 `wait_ms`，真实并发等待者 + 真实夹具进程）：
   立即领取语义不变、有匹配队头立即领；空等不轮询数据库（`wake_attempts=0`）、超时返回空；
   新单发布 / 前项确认 / 租约到期三类事件精确唤醒；定向与不限等待并存且无关设备变动不触发定向复查；
   1 人与 6 人等待同一可领队头的唤醒领取尝试数相等（=1）、只发一份有效租约；
   断连（SIGKILL 夹具进程）即撤销等待、退出后不再参与领取；
   `kill -9` 后原库重启，客户端重新等待仍按**原租约绝对到期时刻**重领（偏差毫秒级），旧令牌确认新租约被拒。
   每组检查均打印**等待结果**（`claim/empty@waited_ms`）与**实际领取尝试数**。
9. **基准与计量硬断言**：见第 5 节。

---

## 5. 领取开销夹具与计量方式

```bash
npm run bench     # 单独跑 0 / 1,000 / 10,000 / 100,000 四档，约 4 秒
```

### 5.1 夹具构造

- 两个订阅：`target`（固定目标订阅）与 `needy`（“其他订阅”）。
- 一台设备 `bench-pump`，`N` 条旧消息：
  - `target` 对这 `N` 条全部 `acked`，其队头已按真实 ack 推进逻辑消失；
  - `needy` 对这 `N` 条全部 `claimed`、未确认、租约已过期但仍可重投 —— 这正是
    “**因其他订阅仍需重投而留存的过期积压**”，清理对它们回收 0 条。
- 然后经**真实发布路径**发布 1 条新消息，测量 `target` 对它的**第一次 claim**。
- 档位只改变 `N`，目标订阅、设备、新消息、领取参数完全固定。

### 5.2 计量口径

一次领取事务内，Repository 对存储的逻辑访问逐条计数（`Meter`，HTTP 可加 `?meter=1` 取到）：

| 分量 | 含义 |
| --- | --- |
| `probes` | 精确键（PK / UNIQUE）探测，每次计 1 个被定位的索引叶子项 |
| `index_steps` | 索引前缀/范围扫描中**实际取出的索引项数**，按语句真实行数累计，包含清扫范围扫描 |
| `rows_read` | 读取的表记录数 |
| `rows_written` | 插入/更新/删除的记录数（写索引维护量与写行数成正比，为常数） |
| `sweep_steps` | 其中“**本次领取触发的过期清扫 + 队头推进**”处理的条目数（上面计数的子集，单列核对） |
| `total` | `probes + index_steps + rows_read + rows_written` |

说明：精确 B-tree 探测途经的内部页为 O(log N)，不随积压**条数**线性变化，故不计入“条目”；
`EXPLAIN QUERY PLAN`（基准会打印，接口 `GET /admin/query-plans` 可查）证明热语句全部是
`SEARCH ... USING [PRIMARY KEY/INDEX]`，不存在对 `receipts/messages/bodies` 的全表 `SCAN`。
墙钟延迟独立佐证。另跑一个“若领取退化为扫描本订阅全部回执”的查询作为**线性反例校准**
（仅对照，不是服务领取路径）。

### 5.3 实测结果（Node v22.23.4，本机）

```
     积压N |  DB(MB) | claim均值µs |  p95µs | 计量合计 | seek | step | 读 | 写 | 清扫 | 反例全扫行 | 全扫ms
         0 |     0.3 |       ~400 |   ~760 |       12 |    6 |    1 |  4 |  1 |    0 |          1 |   0.04
     1,000 |     1.2 |       ~410 |   ~780 |       12 |    6 |    1 |  4 |  1 |    0 |      1,001 |   ~1-2
    10,000 |    10.1 |       ~370 |   ~680 |       12 |    6 |    1 |  4 |  1 |    0 |     10,001 |     ~18
   100,000 |   100.7 |       ~370 |   ~790 |       12 |    6 |    1 |  4 |  1 |    0 |    100,001 |    ~290
```

结论：

- 目标订阅一次领取访问的记录/索引项恒为 **12**（含 1 次清扫索引探测；本场景清扫命中 0 行），
  与 `N` 完全无关；基准对四档计量各分量做**逐字段相等**硬断言，并要求 `total ≤ 24`。
- claim 墙钟稳定在 ~0.3–0.45ms（底部主要是 WAL `synchronous=FULL` 的一次提交 fsync），不随 N 增长。
- 反例全扫的行数与耗时随 N 线性增长（10 万行 / ~290ms），说明夹具确实构造了足量积压、计量有效。

### 5.4 为什么结构上不可能扫到“其他订阅的积压”

```
heads(sub_id, device_id, seq, PRIMARY KEY(sub_id, device_id)) WITHOUT ROWID
UNIQUE INDEX idx_receipts_sub_dev_seq ON receipts(sub_id, device_id, seq)
INDEX idx_receipts_open  ON receipts(sub_id, device_id, seq) WHERE state IN ('pending','claimed')
INDEX idx_receipts_pending_deadline ON receipts(sub_id, first_deadline_at) WHERE state='pending'
```

所有领取/清扫/推进语句都以 `sub_id = ?` 为前导限定：`needy` 的十万行位于另一个索引前缀，
`target` 的 claim 在 B-tree 探测后只会碰到自己的 1 个队头、1 条回执和 1 条消息/正文。

复杂度（单次 claim-any）：`O(D + S + A)`，其中 `D` = 本订阅当前有未决项的设备数（每设备 1 个队头），
`S` = 本次清扫命中的**本订阅**过期 pending 数，`A ≤ D` 为陈旧队头推进次数；
与消息总量、其他订阅的重投积压、已 ack/skipped 历史均无关。
每条回执最多被清扫/推进一次，全生命周期摊还 O(1)。

---

## 6. 崩溃恢复与时间边界

- 每次注册/发布/领取/确认/清理都是 `BEGIN IMMEDIATE` 事务，WAL + `synchronous=FULL`，
  `kill -9`（测试使用 SIGKILL）不丢已返回成功的事务；重启自动重放 WAL。
- 所有截止/租期都存**绝对 epoch 毫秒整数**：`messages.first_deadline_at`、`receipts.lease_expires_at`。
  服务没有任何内存定时器，“是否到期”一律用当前服务器时间与绝对边界比较，
  因此重启后租期与首次截止的状态、剩余/过期判定与崩溃前完全连续，不存在需要补偿的计时器。
- 统一顺序号由 `devices.last_seq` 持久化，重启后后续发布继续取号，不回退、不重号。
- 长等注册表（`waiter.ts`）是**纯内存**结构：崩溃即清空，无需恢复；客户端连接随进程死亡断开，
  重新长等时会从库中绝对的 `lease_expires_at` 重新挂定时器，因此重领时刻与崩溃前完全连续。

---

## 7. 项目结构

```
inspection-service/
├── check.sh                 # 交付的单命令（装依赖→npm test），80s 内
├── package.json
├── tsconfig.json
├── src/
│   ├── main.ts              # 启动入口 / 参数解析 / 优雅关闭
│   ├── server.ts            # 内置 http JSON API + port-file + 长等路由
│   ├── waiter.ts            # 长等注册表：事件/租约到期唤醒、断连撤销、尝试数计量（纯内存）
│   ├── client.ts            # HTTP SDK（夹具与测试使用）
│   ├── db.ts                # node:sqlite、PRAGMA、schema、事务包装
│   ├── errors.ts
│   ├── meter.ts             # 存储访问计量器
│   ├── service.ts           # 注册/发布取号/领取重投/ack/队头/清理/查询计划
│   ├── bench.ts             # 0→100k 领取开销夹具与硬断言（也可 npm run bench）
│   ├── fixtures/
│   │   ├── publish.ts       # 本地发布夹具（可多进程）
│   │   └── subscribe.ts     # 本地订阅夹具（ack / drop 断线 / --wait-ms 长等模式）
│   └── test/
│       └── run-all.ts       # 80 秒全量验证编排
└── README.md
```

### 设计取舍备注

- 发布时向所有“当前已注册”订阅扇出回执，因此“注册后生效”由事务提交顺序天然保证，
  新订阅不会看到任何历史消息，也不需要后台扫描补投。
- 正文与元数据分表（`bodies`），清理即删除正文行；保留的消息元数据/回执不影响领取热路径
  （热路径由 `heads` 与局部索引驱动）。
- 单机单进程写者：Node 同步 SQLite 调用 + `IMMEDIATE` 事务把所有写串行化，
  多调度进程通过 HTTP 并发进入，顺序由数据库事务确定。
