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

from task_queue import Broker, Producer, Worker


def retry_and_dead_letter_demo():
    print("=" * 60)
    print("示例 5: 失败重试与死信队列 (指数退避)")
    print("=" * 60)

    broker = Broker()
    producer = Producer(broker)
    worker = Worker(broker, visibility_timeout=30)

    call_count = [0]

    def flaky_handler(payload):
        call_count[0] += 1
        print(f"  [Worker] 第 {call_count[0]} 次尝试执行 (预期失败)")
        raise RuntimeError(f"模拟失败 - 第 {call_count[0]} 次")

    worker.register("unreliable.task", flaky_handler)

    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    task = producer.send("unreliable.task", {"data": "test"}, max_retries=3)
    print(f"  [Producer] 发送任务 {task.task_id}，最多重试 3 次")

    time.sleep(20)

    dead_letters = broker.list_dead_letter_tasks()
    print(f"  死信队列中的任务数: {len(dead_letters)}")
    for t in dead_letters:
        print(f"    - task_id: {t.task_id}")
        print(f"      name: {t.task_name}")
        print(f"      retry_count: {t.retry_count}")
        print(f"      error: {t.error_message.split(chr(10))[0]}")

    if dead_letters:
        print(f"\n  重新入队死信任务 {dead_letters[0].task_id} ...")
        success = broker.requeue_dead_letter_task(dead_letters[0].task_id)
        print(f"  重新入队结果: {success}")

    time.sleep(15)

    dead_letters2 = broker.list_dead_letter_tasks()
    print(f"  再次检查死信队列: {len(dead_letters2)} 个任务")

    worker.stop()
    worker_thread.join(timeout=5)
    broker.stop()
    print()


def visibility_timeout_demo():
    print("=" * 60)
    print("示例 6: 可见性超时 (Worker 崩溃后任务被重新派发)")
    print("=" * 60)

    broker = Broker()
    producer = Producer(broker)

    worker1 = Worker(broker, worker_id="worker-crasher", visibility_timeout=5)
    worker2 = Worker(broker, worker_id="worker-reliable", visibility_timeout=30)

    execution_log = []

    def slow_handler(payload):
        worker_name = payload.get("_worker", "unknown")
        execution_log.append((worker_name, time.time()))
        print(f"  [Worker-{worker_name}] 开始执行，耗时 20 秒...")
        time.sleep(20)
        print(f"  [Worker-{worker_name}] 执行完成")
        return f"done by {worker_name}"

    worker1.register("slow.task", slow_handler)
    worker2.register("slow.task", slow_handler)

    original_fetch = worker1.broker.fetch_task
    def crashy_fetch(worker_id, timeout_seconds=300):
        task = original_fetch(worker_id, timeout_seconds)
        if task:
            task.payload["_worker"] = "crasher"
            print(f"  [worker-crasher] 领取任务 {task.task_id} 后立即崩溃 (模拟)")
            print(f"  [worker-crasher] 不再发送心跳，{timeout_seconds}秒后任务将超时")
            return task
        return None
    worker1.broker.fetch_task = crashy_fetch

    worker2_thread = threading.Thread(target=worker2.start, daemon=True)
    worker2_thread.start()

    worker1_thread = threading.Thread(target=worker1.start, daemon=True)
    worker1_thread.start()

    task = producer.send("slow.task", {"data": "important"}, max_retries=1)
    print(f"  [Producer] 发送任务 {task.task_id}")

    time.sleep(30)

    print(f"\n  执行日志:")
    for wname, ts in execution_log:
        print(f"    - Worker: {wname}, 时间: {time.strftime('%H:%M:%S', time.localtime(ts))}")

    if len(execution_log) >= 2:
        gap = execution_log[1][1] - execution_log[0][1]
        print(f"  worker-crasher 崩溃后约 {gap:.0f} 秒，worker-reliable 重新领取任务 (可见性超时=5秒)")

    worker1.stop()
    worker2.stop()
    worker1_thread.join(timeout=5)
    worker2_thread.join(timeout=5)
    broker.stop()
    print()


def cron_task_demo():
    print("=" * 60)
    print("示例 7: 定时任务 (每分钟执行一次)")
    print("=" * 60)
    print("  (为了演示速度，这里只展示 Cron 解析器计算下次执行时间)")

    from task_queue import CronParser
    from datetime import datetime

    test_cases = [
        ("* * * * *", "每分钟"),
        ("0 * * * *", "每小时整点"),
        ("30 9 * * 1-5", "工作日 9:30"),
        ("0 0 1 * *", "每月 1 号 0 点"),
    ]

    now = datetime.now()
    print(f"\n  当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}\n")

    for expr, desc in test_cases:
        parser = CronParser(expr)
        nxt = parser.next_run_after(now)
        print(f"  {desc:20s} | expr = {expr:15s} | next = {nxt.strftime('%Y-%m-%d %H:%M')}")

    print()


if __name__ == "__main__":
    cron_task_demo()
    retry_and_dead_letter_demo()
    visibility_timeout_demo()
