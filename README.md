# 分布式任务队列系统 (Distributed Task Queue)

一个基于 Redis 的分布式任务队列完整实现，包含 Producer、Broker、Worker 三大角色。

## 功能特性

- ✅ **任务可靠不丢** — 任务持久化到 Redis
- ✅ **延迟任务** — 基于 ZSet 的高效调度，非轮询全表
- ✅ **定时任务 (Cron)** — 支持标准 5 段式 Cron 表达式
- ✅ **可见性超时** — Worker 崩溃后任务自动重新派发
- ✅ **至少一次执行 + 幂等兜底** — 提供 idempotency_key 机制
- ✅ **任务优先级** — CRITICAL > HIGH > NORMAL > LOW
- ✅ **失败重试 + 指数退避** — 可配置最大重试次数
- ✅ **死信队列 (DLQ)** — 超重试上限进入死信，可人工重入
- ✅ **任务结果回传** — Pub/Sub + 持久化存储，支持同步等待

---

## 项目结构

```
task_queue/
├── __init__.py          # 对外导出
├── models.py            # Task / TaskResult 数据模型
├── broker.py            # Broker 核心（存储、调度、重发、死信）
├── producer.py          # Producer 生产者
├── worker.py            # Worker 消费者（心跳续期）
├── cron.py              # Cron 表达式解析器
└── examples/
    ├── basic_examples.py    # 基础示例：投递/延迟/优先级/幂等
    └── advanced_examples.py # 高级示例：死信/可见性超时/Cron
```

---

## 快速开始

### 安装依赖

```bash
pip install redis
```

确保本地运行 Redis（默认 `redis://localhost:6379/0`）。

### 基础使用

```python
from task_queue import Broker, Producer, Worker

broker = Broker("redis://localhost:6379/0")

# ---- Producer 侧 ----
producer = Producer(broker)
task = producer.send("math.add", {"a": 1, "b": 2})

# 同步等待结果（可选）
result = producer.wait_for_result(task.task_id, timeout=10)
print(result.success, result.result)  # True, 3

# ---- Worker 侧 ----
worker = Worker(broker)

@worker.register("math.add")
def add(payload):
    return payload["a"] + payload["b"]

worker.start()  # 阻塞运行
```

---

## 核心机制详解

### 1. 任务持久化 — 保证 Broker 重启不丢

**数据存储方案：**

```
tq:task:{task_id}        → STRING (JSON)     任务完整数据
tq:queue:{priority}      → LIST              就绪队列（按优先级分 4 条）
tq:delayed               → ZSET              延迟任务，score = scheduled_at 时间戳
tq:processing            → ZSET              处理中任务，score = 超时时间戳
tq:dead_letter           → LIST              死信队列
tq:result:{task_id}      → STRING (JSON)     任务执行结果
tq:idempotency:{key}     → STRING            幂等 key → task_id 映射（带 TTL）
```

**原理：**
- 所有任务元数据写入 Redis `STRING`，Redis 开启 **AOF 持久化**（`appendonly yes`）或 **RDB 快照**即可保证重启不丢。
- 每次状态变更（READY→PROCESSING→SUCCESS/FAILED）都会先更新 `tq:task:{id}`，再操作队列，最终一致性由 Redis 单线程保证。

参考实现：[Broker.save_task](task_queue/broker.py#L50-L52)

---

### 2. 延迟任务 — 高效到点触发，非轮询全表

**传统方案的问题：**
> 把所有延迟任务存在表中，每秒 `SELECT * FROM tasks WHERE scheduled_at < NOW() AND status = 'DELAYED'` — 全表扫描，数据量大时性能极差。

**本系统方案：Redis ZSet（跳表实现）**

```
tq:delayed  ZSET
  score = scheduled_at (Unix 时间戳)
  member = task_id
```

调度器只做一件事（每 500ms 执行一次）：

```python
now = time.time()
# O(log N + M)，M = 到期任务数量，而非总量
ready_ids = redis.zrangebyscore("tq:delayed", 0, now)
redis.zremrangebyscore("tq:delayed", 0, now)  # 原子移除
```

**时间复杂度**：`ZRANGEBYSCORE` 是 O(log N + M)，M 是到期任务数，不是全表。百万级延迟任务也能毫秒级找到到期的那几个。

参考实现：[Broker._process_delayed_tasks](task_queue/broker.py#L97-L112)

---

### 3. 可见性超时 (Visibility Timeout) — Worker 崩溃后任务重新派发

**问题场景：**
> Worker 领取任务后，进程 OOM / 机器断电 / 网络中断，任务永远不会完成，也不会被其他 Worker 接手。

**解决方案：**

```
┌─────────────┐  1. RPOP 领取任务   ┌─────────────┐
│  Worker-A   │ ──────────────────▶ │   Broker    │
└─────────────┘                     └─────────────┘
                                        │
                                        ▼
                              ZADD tq:processing
                              score = now + 300s
                              member = task_id
                                        │
     2. Worker-A 崩溃，不再续期          │
                                        ▼
                              后台 Reaper 线程每 1s 扫描：
                              ZRANGEBYSCORE tq:processing 0 now
                              → 把超时任务重新入队（或重试/死信）
```

**关键细节：**
- Worker 领取任务后，Broker 同时把任务放入 `tq:processing` ZSet，score = `当前时间 + visibility_timeout`（默认 300s）。
- Worker 后台**心跳线程**每 `heartbeat_interval`（默认 60s）调用 `renew_lease()`，把对应任务的 score 向后推。
- Reaper 线程每秒扫描 `tq:processing`，把 score < now 的任务移除并重新入队。
- 长任务靠心跳续期永远不会超时；Worker 挂了心跳停止，300s 后任务自动"现身"被其他 Worker 领取。

参考实现：
- 领取时设置超时：[Broker.fetch_task](task_queue/broker.py#L136-L153)
- 心跳续期：[Broker.renew_lease](task_queue/broker.py#L155-L160)、[Worker._heartbeat_loop](task_queue/worker.py#L61-L70)
- 超时回收：[Broker._process_timed_out_tasks](task_queue/broker.py#L123-L133)

---

### 4. 至少一次执行 + 幂等兜底

**为什么只能"至少一次"而不能"恰好一次"？**
> 分布式系统中，Worker 执行成功但回写 ACK 前崩溃，Broker 无法区分"任务没执行"和"执行了但 ACK 丢了"，只能安全地重新派发。因此**重复执行不可避免**，必须靠幂等兜底。

**本系统幂等机制：**

```python
# Producer 侧：传入业务唯一键
producer.send(
    "payment.charge",
    {"order_id": "A123", "amount": 99},
    idempotency_key="charge:A123"   # 业务唯一键
)
```

Broker 在 enqueue 前做检查：

```
SETNX tq:idempotency:charge:A123  →  task_id_xxx  (TTL=7天)
```

- 同一个 `idempotency_key` 第二次投递时，Broker 直接返回已有 task_id，不会重复入队。
- **这只解决"重复投递"问题**，解决不了"Worker 执行两次"（可见性超时触发的重派）。

**业务层幂等（必须实现）：**

```python
@worker.register("payment.charge")
def charge(payload):
    order_id = payload["order_id"]
    # 方案 A：数据库唯一索引
    #   INSERT INTO payment_log (order_id) VALUES (...)
    #   重复执行会抛 DuplicateKey，捕获后直接返回成功
    #
    # 方案 B：状态机判断
    #   order = db.get(order_id)
    #   if order.status == "PAID":
    #       return {"already_paid": True}
    #
    # 方案 C：Redis 去重
    #   if redis.setnx(f"done:{order_id}", 1):
    #       do_real_charge()
    ...
```

**原则**：框架保证"至少一次"，业务方必须保证"幂等执行"。

参考实现：[Broker.check_idempotency](task_queue/broker.py#L58-L62)、[Broker.enqueue_task](task_queue/broker.py#L68-L91)

---

### 5. 失败重试 + 指数退避 + 死信队列

**重试流程：**

```
任务执行失败
    │
    ▼
retry_count++
    │
    ├── retry_count < max_retries ?
    │       │
    │       ├── 是 → 计算 backoff = min(3600, 2^retry_count) 秒
    │       │        放入 tq:delayed ZSet，scheduled_at = now + backoff
    │       │        （指数退避：2s → 4s → 8s → 16s → ... 最多 1h）
    │       │
    │       └── 否 → 进入死信队列
    │
    ▼
LPUSH tq:dead_letter  task_id
(status = DEAD_LETTER)
```

**死信队列（DLQ）的意义：**
> 防止"毒消息"无限重试占用资源。任务进入死信后，开发人员可以通过日志排查，修复代码后调用 `broker.requeue_dead_letter_task(task_id)` 手动回滚。

```python
# 查看死信
dead = broker.list_dead_letter_tasks()

# 重入死信（代码修复后）
broker.requeue_dead_letter_task(dead[0].task_id)
```

参考实现：[Broker._handle_retry_or_dead_letter](task_queue/broker.py#L188-L216)

---

### 6. 任务优先级

**实现方式：按优先级拆分 4 条 List**

```
tq:queue:3  (CRITICAL)    最先消费
tq:queue:2  (HIGH)
tq:queue:1  (NORMAL)      默认
tq:queue:0  (LOW)         最后消费
```

Worker 每次领取时按 `CRITICAL → HIGH → NORMAL → LOW` 顺序 RPOP，保证高优先级先被处理。

**不足**：低优先级可能被饿死（当高优先级持续产生时）。如需要可加"老化"机制。

参考实现：[Broker.fetch_task](task_queue/broker.py#L136-L153)

---

### 7. 定时任务 (Cron)

支持标准 5 段式 Cron：

```
*  *  *  *  *
│  │  │  │  └── 星期 (0-6, 0=周日)
│  │  │  └───── 月份 (1-12)
│  │  └──────── 日期 (1-31)
│  └─────────── 小时 (0-23)
└────────────── 分钟 (0-59)
```

示例：
- `*/5 * * * *` — 每 5 分钟
- `0 9 * * 1-5` — 工作日 9:00
- `0 0 1 * *` — 每月 1 号 0 点

**实现原理：**
1. Producer 投递 Cron 任务时，先用 `CronParser.next_run_after()` 算出下一次执行时间，以延迟任务形式入队。
2. Worker 成功执行后，Broker 检测到 `cron_expression` 字段，自动算出下一次执行时间并再次入队。
3. 这样 Cron 任务像"链式延迟任务"一样自驱循环，无需额外调度器扫描。

参考实现：[CronParser](task_queue/cron.py)、[Broker.complete_task](task_queue/broker.py#L162-L186)

---

### 8. 任务结果回传

**双通道回传：**

1. **Pub/Sub 实时通道** — `tq:result_channel`，适用于 Producer 同步等待。
2. **持久化存储** — `tq:result:{task_id}`，适用于后续查询或断连后补查。

```python
# 同步等待（超时自动降级为轮询存储）
result = producer.wait_for_result(task_id, timeout=60)

# 异步查询
result = producer.get_result(task_id)
```

参考实现：[Broker.get_result](task_queue/broker.py#L218-L241)

---

## 运行示例

```bash
# 基础示例
python task_queue/examples/basic_examples.py

# 高级示例（死信 / 可见性超时 / Cron）
python task_queue/examples/advanced_examples.py
```

---

## 生产环境建议

| 项目 | 建议 |
|------|------|
| Redis 部署 | 至少主从 + 哨兵，或 Redis Cluster；开启 AOF `appendfsync everysec` |
| 可见性超时 | 设为平均任务耗时的 3~5 倍，心跳间隔取超时的 1/3 |
| 幂等 TTL | 根据业务最长可能重复窗口设置，默认 7 天 |
| 死信监控 | 监听 `tq:dead_letter` 长度，超过阈值告警 |
| Worker 数量 | 绑定 CPU 核数或稍多（IO 密集型可多开） |
| 队列隔离 | 不同业务可用不同 `namespace` 前缀隔离 |

---

## 核心文件速览

| 文件 | 作用 |
|------|------|
| [models.py](task_queue/models.py) | Task / TaskResult 数据结构 |
| [broker.py](task_queue/broker.py) | 存储、调度、可见性超时、死信、结果 |
| [producer.py](task_queue/producer.py) | 投递接口（即时 / 延迟 / Cron） |
| [worker.py](task_queue/worker.py) | 消费者 + 心跳续期 |
| [cron.py](task_queue/cron.py) | Cron 解析 |
