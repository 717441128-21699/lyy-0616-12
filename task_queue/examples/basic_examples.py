import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

from task_queue import Broker, Producer, Worker, TaskPriority


def basic_usage():
    print("=" * 60)
    print("示例 1: 基础任务投递与执行")
    print("=" * 60)

    broker = Broker()
    producer = Producer(broker)
    worker = Worker(broker, visibility_timeout=60, heartbeat_interval=10)

    results = []

    def add_handler(payload):
        a = payload.get("a", 0)
        b = payload.get("b", 0)
        result = a + b
        results.append(result)
        print(f"  [Worker] 计算 {a} + {b} = {result}")
        return result

    worker.register("math.add", add_handler)

    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    task = producer.send("math.add", {"a": 10, "b": 20})
    print(f"  [Producer] 发送任务: {task.task_id}")

    result = producer.wait_for_result(task.task_id, timeout=10.0)
    if result:
        print(f"  [Producer] 收到结果: success={result.success}, result={result.result}")

    worker.stop()
    worker_thread.join(timeout=5)
    broker.stop()
    print()


def delayed_task():
    print("=" * 60)
    print("示例 2: 延迟任务 (3秒后执行)")
    print("=" * 60)

    broker = Broker()
    producer = Producer(broker)
    worker = Worker(broker, visibility_timeout=60)

    def delayed_handler(payload):
        msg = payload.get("message", "")
        print(f"  [Worker] 延迟任务执行: {msg} (执行时间: {time.strftime('%H:%M:%S')})")
        return f"processed: {msg}"

    worker.register("delayed.echo", delayed_handler)

    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    start = time.time()
    print(f"  [Producer] 投递时间: {time.strftime('%H:%M:%S')}")
    task = producer.send("delayed.echo", {"message": "hello delayed"}, delay_seconds=3)
    print(f"  [Producer] 任务 {task.task_id} 将在 3 秒后执行")

    result = producer.wait_for_result(task.task_id, timeout=15.0)
    elapsed = time.time() - start
    if result:
        print(f"  [Producer] 收到结果: {result.result} (总耗时 {elapsed:.1f}s)")

    worker.stop()
    worker_thread.join(timeout=5)
    broker.stop()
    print()


def priority_demo():
    print("=" * 60)
    print("示例 3: 任务优先级 (CRITICAL > HIGH > NORMAL > LOW)")
    print("=" * 60)

    broker = Broker()
    producer = Producer(broker)
    worker = Worker(broker, visibility_timeout=60)

    execution_order = []

    def make_handler(name):
        def handler(payload):
            execution_order.append(name)
            print(f"  [Worker] 执行: {name}")
            time.sleep(0.2)
            return name
        return handler

    worker.register("task.low", make_handler("LOW-priority"))
    worker.register("task.normal", make_handler("NORMAL-priority"))
    worker.register("task.high", make_handler("HIGH-priority"))
    worker.register("task.critical", make_handler("CRITICAL-priority"))

    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()
    time.sleep(0.5)

    producer.send("task.low", {}, priority=TaskPriority.LOW)
    producer.send("task.normal", {}, priority=TaskPriority.NORMAL)
    producer.send("task.high", {}, priority=TaskPriority.HIGH)
    producer.send("task.critical", {}, priority=TaskPriority.CRITICAL)

    time.sleep(3)
    print(f"  执行顺序: {execution_order}")

    worker.stop()
    worker_thread.join(timeout=5)
    broker.stop()
    print()


def idempotency_demo():
    print("=" * 60)
    print("示例 4: 幂等性 (相同 idempotency_key 只投递一次)")
    print("=" * 60)

    broker = Broker()
    producer = Producer(broker)
    worker = Worker(broker, visibility_timeout=60)

    counter = [0]

    def dedup_handler(payload):
        counter[0] += 1
        print(f"  [Worker] 执行次数: {counter[0]}, payload: {payload}")
        return counter[0]

    worker.register("dedup.task", dedup_handler)

    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    key = "order-payment-12345"
    t1 = producer.send("dedup.task", {"order_id": "12345"}, idempotency_key=key)
    t2 = producer.send("dedup.task", {"order_id": "12345"}, idempotency_key=key)
    t3 = producer.send("dedup.task", {"order_id": "12345"}, idempotency_key=key)

    print(f"  [Producer] t1.task_id = {t1.task_id}")
    print(f"  [Producer] t2.task_id = {t2.task_id}")
    print(f"  [Producer] t3.task_id = {t3.task_id}")
    print(f"  三个任务因幂等 key 相同，实际只执行 1 次")

    time.sleep(3)
    print(f"  实际执行次数: {counter[0]}")

    worker.stop()
    worker_thread.join(timeout=5)
    broker.stop()
    print()


if __name__ == "__main__":
    basic_usage()
    delayed_task()
    priority_demo()
    idempotency_demo()
