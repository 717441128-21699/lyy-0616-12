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

import fakeredis

from task_queue import Broker, Producer, Worker, Task, TaskStatus, TaskPriority


def _make_broker(shared_redis, namespace):
    return Broker(namespace=namespace, redis_client=shared_redis)


def test_crash_during_delayed_to_ready():
    print("=" * 70)
    print("崩溃场景 1: 延迟任务迁移到就绪队列的临界点崩溃")
    print("=" * 70)
    print()
    print("  模拟场景:")
    print("  Broker 正在把到期任务从延迟 ZSet 挪到就绪 List")
    print("  -> 刚从 ZSet 移除，还没 LPUSH 进就绪队列")
    print("  -> 进程被杀掉 (崩溃)")
    print("  -> 重启后任务应该被一致性扫描捞回来")
    print()

    shared_redis = fakeredis.FakeRedis(decode_responses=True)
    namespace = "crash_delayed"

    # ---- 第 1 阶段: 先正常投递，再手动构造"崩溃脏数据" ----
    print("  [阶段1] 构造崩溃后的脏数据状态...")
    broker1 = _make_broker(shared_redis, namespace)

    task = Task.create(
        task_name="test.recovery",
        payload={"msg": "hello from crash"},
        priority=TaskPriority.NORMAL,
        delay_seconds=100,
    )
    task.scheduled_at = time.time() - 5  # 早就到期了
    task.status = TaskStatus.DELAYED
    broker1.save_task(task)
    broker1.redis.zadd(f"{namespace}:tq:delayed", {task.task_id: task.scheduled_at})

    # 现在模拟"迁移中崩溃"：状态改成 READY，但队列里没有
    task.status = TaskStatus.READY
    broker1.save_task(task)
    # 从延迟 ZSet 移除（模拟已经 ZREM 了）
    broker1.redis.zrem(f"{namespace}:tq:delayed", task.task_id)
    # 注意：没有 LPUSH 到就绪队列 — 模拟中间崩溃

    in_queue = broker1.redis.lrange(f"{namespace}:tq:queue:1", 0, -1)
    in_delayed = broker1.redis.zrange(f"{namespace}:tq:delayed", 0, -1)
    task_check = broker1.get_task(task.task_id)

    print(f"  -> 任务ID: {task.task_id}")
    print(f"  -> 任务状态: {task_check.status.name}")
    print(f"  -> 是否在就绪队列: {'是' if task.task_id in in_queue else '否 (丢失!)'}")
    print(f"  -> 是否在延迟 ZSet: {'是' if task.task_id in in_delayed else '否 (已移除)'}")
    print(f"  -> 当前情况: 任务存在但不在任何队列 = 永久丢失状态")
    print()

    # ---- 第 2 阶段: 新建 Broker 实例（模拟重启） ----
    print("  [阶段2] 重启 Broker (一致性扫描应该自动修复)...")

    broker2 = _make_broker(shared_redis, namespace)
    # 注意：Worker 会触发 start_background_threads()，里面会先跑一致性扫描

    worker = Worker(broker2, visibility_timeout=30)
    executed_flag = [False]

    def handler(payload):
        executed_flag[0] = True
        print(f"  [Worker] 任务执行成功! payload={payload}")
        return payload.get("msg")

    worker.register("test.recovery", handler)
    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    time.sleep(3)

    result = broker2.get_result(task.task_id)
    final_task = broker2.get_task(task.task_id)

    worker.stop()
    worker_thread.join(timeout=5)
    broker2.stop()

    print()
    print("  [结果]")
    print(f"    任务是否被执行: {'[OK] 是' if executed_flag[0] else '[FAIL] 否'}")
    print(f"    任务最终状态: {final_task.status.name}")
    print(f"    是否能拿到结果: {'[OK] 是' if result else '[FAIL] 否'}")
    if result:
        print(f"    结果: success={result.success}, result={result.result}")

    assert executed_flag[0], "任务应该在重启后被一致性扫描恢复并执行"
    assert result and result.success, "应该能拿到成功结果"
    print()
    print("  [OK] 场景1通过: 延迟迁移点崩溃，重启后一致性扫描恢复，任务正常执行")
    print()


def test_crash_during_timeout_recovery():
    print("=" * 70)
    print("崩溃场景 2: Worker 崩溃 + Broker 超时回收时也崩溃")
    print("=" * 70)
    print()
    print("  模拟场景:")
    print("  1. Worker 领取任务后崩溃")
    print("  2. Broker 检测到超时，从 processing ZSet 移除任务")
    print("  3. 还没放进重试/死信队列，Broker 自己也崩了")
    print("  4. 重启后一致性扫描捞回，继续按重试次数推进")
    print("  5. 最终超重试上限进死信，Producer 等结果不卡超时")
    print()

    shared_redis = fakeredis.FakeRedis(decode_responses=True)
    namespace = "crash_timeout"
    max_retries = 2

    # ---- 第 1 阶段: 构造回收中崩溃的脏数据 ----
    print("  [阶段1] 构造回收过程中崩溃的脏数据...")
    broker1 = _make_broker(shared_redis, namespace)

    task = Task.create(
        task_name="test.flaky",
        payload={"data": "important"},
        priority=TaskPriority.NORMAL,
        max_retries=max_retries,
    )
    task.status = TaskStatus.PROCESSING
    task.worker_id = "crashed-worker"
    task.started_at = time.time() - 60
    task.visibility_timeout = 30
    task.retry_count = 1  # 已经重试过 1 次
    broker1.save_task(task)
    # 注意: 不加入 processing ZSet — 模拟刚 ZREM 但还没进重试队列就崩了

    in_processing = broker1.redis.zrange(f"{namespace}:tq:processing", 0, -1)
    in_delayed = broker1.redis.zrange(f"{namespace}:tq:delayed", 0, -1)
    in_queue = broker1.redis.lrange(f"{namespace}:tq:queue:1", 0, -1)

    print(f"  -> 任务ID: {task.task_id}")
    print(f"  -> 任务状态: {task.status.name}")
    print(f"  -> retry_count: {task.retry_count}")
    print(f"  -> 在 processing ZSet: {'是' if task.task_id in in_processing else '否 (丢失!)'}")
    print(f"  -> 在延迟 ZSet: {'是' if task.task_id in in_delayed else '否'}")
    print(f"  -> 在就绪队列: {'是' if task.task_id in in_queue else '否'}")
    print(f"  -> 当前情况: PROCESSING 状态但不在任何队列 = 永久丢失状态")
    print()

    # ---- 第 2 阶段: 重启，看一致性扫描恢复 + 重试推进 ----
    print("  [阶段2] 重启 Broker + Worker，观察恢复和重试推进...")

    broker2 = _make_broker(shared_redis, namespace)
    producer = Producer(broker2)
    worker = Worker(broker2, visibility_timeout=3)

    fail_count = [0]

    def always_fail_handler(payload):
        fail_count[0] += 1
        print(f"  [Worker] 第 {fail_count[0]} 次执行，仍然失败")
        raise RuntimeError("simulated failure")

    worker.register("test.flaky", always_fail_handler)
    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    print(f"  [Producer] wait_for_result(timeout=30)...")
    wait_start = time.time()
    result = producer.wait_for_result(task.task_id, timeout=30)
    wait_elapsed = time.time() - wait_start

    final_task = broker2.get_task(task.task_id)
    dead_letters = broker2.list_dead_letter_tasks()

    worker.stop()
    worker_thread.join(timeout=5)
    broker2.stop()

    print()
    print("  [结果]")
    print(f"    wait_for_result 耗时: {wait_elapsed:.1f}s (远小于 30s = 及时拿到)")
    print(f"    是否拿到结果: {'[OK] 是' if result else '[FAIL] 否'}")
    if result:
        print(f"    success: {result.success}")
        print(f"    retry_count: {result.retry_count}")
        print(f"    error: {result.error.split(chr(10))[0]}")
    print(f"    任务最终状态: {final_task.status.name}")
    print(f"    任务 retry_count: {final_task.retry_count}")
    print(f"    是否在死信队列: {'[OK] 是' if len(dead_letters) > 0 else '[FAIL] 否'}")
    print(f"    Worker 实际执行次数: {fail_count[0]}")

    assert result is not None, "应该能拿到结果，不能等到超时"
    assert result.success is False, "结果应该是失败"
    assert final_task.status == TaskStatus.DEAD_LETTER, "最终应该在死信队列"
    assert final_task.retry_count == max_retries, f"retry_count 应该是 {max_retries}"
    assert wait_elapsed < 25, f"应该很快拿到结果，耗时 {wait_elapsed:.1f}s 太长"
    print()
    print("  [OK] 场景2通过: 回收点崩溃后，一致性扫描恢复，任务推进到死信，结果及时回传")
    print()


def test_multiple_crash_recovery():
    print("=" * 70)
    print("崩溃场景 3: 多次不同位置崩溃的综合测试 (反复重启)")
    print("=" * 70)
    print()
    print("  模拟场景:")
    print("  一个任务在处理过程中经历 3 次不同阶段的崩溃")
    print("  每次重启都靠一致性扫描捞回来")
    print("  最终任务正常成功执行，不会丢")
    print()

    shared_redis = fakeredis.FakeRedis(decode_responses=True)
    namespace = "crash_multi"

    # 初始状态: READY 但不在队列（模拟就绪队列迁移崩溃）
    print("  [初始] 构造 READY 但不在队列的脏数据...")
    broker0 = _make_broker(shared_redis, namespace)
    task = Task.create(
        task_name="multi.stage.task",
        payload={"stage": "initial"},
        max_retries=5,
    )
    task.status = TaskStatus.READY
    broker0.save_task(task)
    # 不加到就绪队列
    print(f"  任务 {task.task_id} 初始状态: READY 但不在队列")
    print()

    # 第1次重启
    print("  [第1次重启] 一致性扫描发现 READY 任务不在队列，放回队列")
    broker1 = _make_broker(shared_redis, namespace)

    # Worker 领取后，模拟"领取后立刻崩溃"
    # 手动让任务变成 PROCESSING 但不在 processing ZSet
    task1 = broker1.get_task(task.task_id)
    task1.status = TaskStatus.PROCESSING
    task1.worker_id = "crashed-w1"
    task1.started_at = time.time() - 10
    broker1.save_task(task1)
    # 不在 processing ZSet
    # 也不在就绪队列（已经 RPOP 了）

    print(f"  第1次崩溃点: PROCESSING 状态但不在 processing ZSet")
    broker1.stop()
    print()

    # 第2次重启
    print("  [第2次重启] 一致性扫描发现 PROCESSING 丢失，触发重试 (retry_count++)")
    broker2 = _make_broker(shared_redis, namespace)

    time.sleep(1)
    task2 = broker2.get_task(task.task_id)
    print(f"  重启后状态: {task2.status.name}, retry_count={task2.retry_count}")

    # 再模拟一次"延迟→就绪迁移中崩溃"
    if task2.status == TaskStatus.DELAYED:
        # 手动改成 READY 但不在队列
        task2.status = TaskStatus.READY
        broker2.save_task(task2)
        broker2.redis.zrem(f"{namespace}:tq:delayed", task.task_id)
        print(f"  第2次崩溃点: 延迟迁移中崩溃 (READY 但不在队列)")
    else:
        print(f"  警告: 预期 DELAYED 状态，实际是 {task2.status.name}")

    broker2.stop()
    print()

    # 第3次重启 + 正常执行
    print("  [第3次重启] 最后一次重启，任务应该被正常执行成功")
    broker3 = _make_broker(shared_redis, namespace)

    success_flag = [False]

    def final_handler(payload):
        success_flag[0] = True
        print("  [Worker-final] 任务终于成功执行了!")
        return {"survived": True, "crashes": 3}

    worker_final = Worker(broker3, visibility_timeout=30, worker_id="final")
    worker_final.register("multi.stage.task", final_handler)
    worker_thread = threading.Thread(target=worker_final.start, daemon=True)
    worker_thread.start()

    time.sleep(3)

    result = broker3.get_result(task.task_id)
    final_task = broker3.get_task(task.task_id)

    worker_final.stop()
    worker_thread.join(timeout=5)
    broker3.stop()

    print()
    print("  [结果]")
    print(f"    任务最终状态: {final_task.status.name}")
    print(f"    最终 retry_count: {final_task.retry_count}")
    print(f"    是否执行成功: {'[OK] 是' if success_flag[0] else '[FAIL] 否'}")
    print(f"    是否拿到结果: {'[OK] 是' if result else '[FAIL] 否'}")
    if result:
        print(f"    结果: {result.result}")

    assert success_flag[0], "经过多次崩溃重启后，任务应该仍然能成功执行"
    assert result and result.success, "应该能拿到成功结果"
    print()
    print("  [OK] 场景3通过: 经历3次不同阶段崩溃后，任务最终仍能正常完成")
    print()


def test_dead_letter_result_crash_recovery():
    print("=" * 70)
    print("崩溃场景 4: 死信结果回传时崩溃 + 反复重启")
    print("=" * 70)
    print()
    print("  模拟场景:")
    print("  1. 任务超重试上限被判定进死信")
    print("  2. Broker 已更新状态 + 推入 DLQ，但还没写 result + publish")
    print("  3. Broker 崩溃 (在这个真实的崩溃窗口)")
    print("  4. 反复重启 Broker 多次 (模拟在临界点反复启停)")
    print("  5. 最终 Producer 等结果能很快拿到失败信息，不会卡到超时")
    print("  6. 重启前后结果一致，不会出现多条结果")
    print()

    shared_redis = fakeredis.FakeRedis(decode_responses=True)
    namespace = "crash_dlq_result"
    max_retries = 3
    dlq_error = "failed 3 times, business logic error: db connection timeout"

    # ---- 第 1 阶段: 构造"进死信但没写 result"的崩溃脏数据 ----
    print("  [阶段1] 构造死信回传时崩溃的脏数据...")
    broker0 = _make_broker(shared_redis, namespace)

    task = Task.create(
        task_name="test.dlq.recovery",
        payload={"action": "process_payment"},
        priority=TaskPriority.HIGH,
        max_retries=max_retries,
    )
    task.status = TaskStatus.DEAD_LETTER
    task.retry_count = max_retries
    task.error_message = dlq_error
    task.finished_at = time.time() - 10
    broker0.save_task(task)
    broker0.redis.lpush(f"{namespace}:tq:dead_letter", task.task_id)
    # 注意: 不写 result，不 publish — 模拟刚进 DLQ 就崩了

    in_dlq = broker0.redis.lrange(f"{namespace}:tq:dead_letter", 0, -1)
    result_exists = broker0.redis.exists(f"{namespace}:tq:result:{task.task_id}")
    task_check = broker0.get_task(task.task_id)

    print(f"  -> 任务ID: {task.task_id}")
    print(f"  -> 任务状态: {task_check.status.name}")
    print(f"  -> retry_count: {task_check.retry_count}")
    print(f"  -> error_message: {task_check.error_message}")
    print(f"  -> 是否在 DLQ: {'是' if task.task_id in in_dlq else '否'}")
    print(f"  -> 结果是否存在: {'是' if result_exists else '否 (丢失!)'}")
    print(f"  -> 当前情况: DEAD_LETTER + 在 DLQ，但无 result = Producer 会等到超时")
    print()

    # ---- 第 2 阶段: 反复重启 Broker 多次 (模拟在临界点反复启停) ----
    print("  [阶段2] 反复重启 Broker 3 次，每次都应该能正确处理...")
    for i in range(3):
        print(f"  [重启 {i+1}/3] 启动 Broker...")
        broker_temp = _make_broker(shared_redis, namespace)
        # 启动后台线程触发一致性扫描
        worker_temp = Worker(broker_temp, visibility_timeout=5)
        worker_temp.register("test.dlq.recovery", lambda p: None)
        worker_thread_temp = threading.Thread(target=worker_temp.start, daemon=True)
        worker_thread_temp.start()
        time.sleep(0.5)
        worker_temp.stop()
        worker_thread_temp.join(timeout=2)
        broker_temp.stop()
        print(f"  [重启 {i+1}/3] 已停止")
    print()

    # ---- 第 3 阶段: 最终验证 ----
    print("  [阶段3] 启动 Producer 等待结果，验证能快速拿到...")
    broker_final = _make_broker(shared_redis, namespace)
    producer = Producer(broker_final)

    # 先手动验证一致性扫描已经补了 result
    result_before_wait = broker_final.get_result(task.task_id)
    print(f"  -> 等待前 result 已存在: {'[OK] 是' if result_before_wait else '[FAIL] 否'}")
    if result_before_wait:
        print(f"  -> 预存结果: success={result_before_wait.success}, retry_count={result_before_wait.retry_count}")
        print(f"  -> 预存错误: {result_before_wait.error.split(chr(10))[0]}")

    print()
    print(f"  [Producer] wait_for_result(timeout=30)...")
    wait_start = time.time()
    result = producer.wait_for_result(task.task_id, timeout=30)
    wait_elapsed = time.time() - wait_start

    final_task = broker_final.get_task(task.task_id)
    dead_letters = broker_final.list_dead_letter_tasks()

    print()
    print("  [最终结果]")
    print(f"    wait_for_result 耗时: {wait_elapsed:.1f}s (远小于 30s = 及时拿到)")
    print(f"    是否拿到结果: {'[OK] 是' if result else '[FAIL] 否'}")
    if result:
        print(f"    success: {result.success}")
        print(f"    retry_count: {result.retry_count}")
        print(f"    错误原因: {result.error.split(chr(10))[0]}")
    print(f"    任务最终状态: {final_task.status.name}")
    print(f"    是否在死信队列: {'[OK] 是' if len(dead_letters) > 0 else '[FAIL] 否'}")
    print(f"    死信队列任务数: {len(dead_letters)}")

    # 验证重启前后拿到的是同一条结果
    if result_before_wait and result:
        same_result = (result_before_wait.success == result.success
                       and result_before_wait.retry_count == result.retry_count
                       and result_before_wait.error == result.error)
        print(f"    重启前后结果一致: {'[OK] 是' if same_result else '[FAIL] 否'}")

    assert result is not None, "应该能拿到结果，不能等到超时"
    assert result.success is False, "结果应该是失败"
    assert final_task.status == TaskStatus.DEAD_LETTER, "最终应该在死信队列"
    assert final_task.retry_count == max_retries, f"retry_count 应该是 {max_retries}"
    assert wait_elapsed < 5, f"应该很快拿到结果，耗时 {wait_elapsed:.1f}s 太长"
    assert dlq_error in result.error, "错误原因应该包含原始错误信息"
    if result_before_wait and result:
        assert same_result, "重启前后应该拿到同一条结果"
    print()
    print("  [OK] 场景4通过: 死信结果回传时崩溃，反复重启后结果补传成功，Producer 快速拿到失败结果")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("  崩溃一致性恢复验收测试 (共 4 个场景)")
    print("  使用 fakeredis，无需真实 Redis")
    print("=" * 70 + "\n")

    try:
        test_crash_during_delayed_to_ready()
        test_crash_during_timeout_recovery()
        test_multiple_crash_recovery()
        test_dead_letter_result_crash_recovery()

        print("=" * 70)
        print("[OK] 全部 4 个崩溃恢复场景验收通过!")
        print("=" * 70)
    except Exception as e:
        print(f"\n[FAIL] 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
