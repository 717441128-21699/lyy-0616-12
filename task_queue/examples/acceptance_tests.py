import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
import threading
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

import fakeredis

from task_queue import Broker, Producer, Worker, CronParser, TaskStatus


def test_delayed_task_restart_recovery():
    print("=" * 70)
    print("验收测试 1: 延迟任务 Broker 重启后不丢失 (10秒延迟 + 中途停止)")
    print("=" * 70)

    shared_redis = fakeredis.FakeRedis(decode_responses=True)
    namespace = f"test_recovery_{int(time.time())}"
    broker1 = Broker(namespace=namespace, redis_client=shared_redis)

    print(f"\n  步骤1: 投递 10 秒延迟任务 (当前时间 {time.strftime('%H:%M:%S')})")
    producer = Producer(broker1)
    task = producer.send("test.delayed", {"foo": "bar"}, delay_seconds=10)
    print(f"  任务 {task.task_id} 已投递，scheduled_at = {time.strftime('%H:%M:%S', time.localtime(task.scheduled_at))}")

    print(f"\n  步骤2: 等待 3 秒后停止 Broker (模拟进程崩溃)")
    time.sleep(3)
    broker1.stop()
    print(f"  Broker 已停止，此时任务仍在 ZSet 中，状态 = DELAYED")

    task_check = broker1.get_task(task.task_id)
    print(f"  任务状态检查: status={task_check.status.name}, scheduled_at={time.strftime('%H:%M:%S', time.localtime(task_check.scheduled_at))}")

    print(f"\n  步骤3: 等待 10 秒 (超过预定执行时间)，让任务到期")
    print(f"  现在时间: {time.strftime('%H:%M:%S')}，正在等待...")
    time.sleep(10)
    print(f"  现在时间: {time.strftime('%H:%M:%S')}，已超过预定执行时间")

    print(f"\n  步骤4: 启动新 Broker 实例 (模拟进程重启)")
    broker2 = Broker(namespace=namespace, redis_client=shared_redis)
    producer2 = Producer(broker2)

    worker = Worker(broker2, visibility_timeout=30)
    executed_flag = [False]

    def handler(payload):
        executed_flag[0] = True
        print(f"  [Worker] 任务已执行! payload={payload}, 执行时间={time.strftime('%H:%M:%S')}")
        return "OK"

    worker.register("test.delayed", handler)
    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    print(f"\n  步骤5: 等待最多 15 秒，看任务是否能被执行 (启动时的恢复扫描应该立即发现到期任务)")
    start = time.time()
    while time.time() - start < 15:
        if executed_flag[0]:
            break
        time.sleep(0.5)

    result = producer2.wait_for_result(task.task_id, timeout=5)

    worker.stop()
    worker_thread.join(timeout=5)
    broker2.stop()

    print(f"\n  结果:")
    print(f"    任务是否执行: {'[OK] 是' if executed_flag[0] else '[FAIL] 否'}")
    print(f"    wait_for_result 是否拿到: {'[OK] 是' if result else '[FAIL] 否'}")
    if result:
        print(f"    结果: success={result.success}, result={result.result}")

    if not executed_flag[0]:
        final_task = broker2.get_task(task.task_id)
        print(f"    任务最终状态: {final_task.status.name}")

    assert executed_flag[0], "任务应该在重启后被执行"
    assert result and result.success, "应该能拿到成功结果"
    print("  [OK] 测试 1 通过: 延迟任务在 Broker 重启后正常执行\n")


def test_worker_crash_retry_count():
    print("=" * 70)
    print("验收测试 2: Worker 连续崩溃，超时重派计入重试，超重试上限进死信")
    print("=" * 70)

    shared_redis = fakeredis.FakeRedis(decode_responses=True)
    namespace = f"test_crash_{int(time.time())}"
    broker = Broker(namespace=namespace, redis_client=shared_redis)
    producer = Producer(broker)
    worker = Worker(broker, worker_id="crasher", visibility_timeout=3)

    attempt_count = [0]
    max_retries = 2

    def always_crash_handler(payload):
        attempt_count[0] += 1
        print(f"  [Worker] 第 {attempt_count[0]} 次执行，1秒后模拟崩溃 (不续期、不完成)")
        time.sleep(1)
        raise SystemExit("worker crash simulation")

    worker.register("crash.task", always_crash_handler)
    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    print(f"\n  步骤1: 投递任务，max_retries={max_retries} (最多尝试 {max_retries+1} 次)")
    task = producer.send("crash.task", {"data": "test"}, max_retries=max_retries)
    print(f"  任务 {task.task_id} 已投递")

    print(f"\n  步骤2: 等待最多 40 秒，观察任务反复崩溃、超时重派、最终进死信")
    start = time.time()
    while time.time() - start < 40:
        t = broker.get_task(task.task_id)
        if t.status == TaskStatus.DEAD_LETTER:
            print(f"  {time.strftime('%H:%M:%S')} 任务已进入死信!")
            break
        time.sleep(1)

    final_task = broker.get_task(task.task_id)
    dead_letters = broker.list_dead_letter_tasks()
    result = producer.wait_for_result(task.task_id, timeout=5)

    worker.stop()
    worker_thread.join(timeout=5)
    broker.stop()

    print(f"\n  结果:")
    print(f"    实际执行次数: {attempt_count[0]}")
    print(f"    retry_count 字段: {final_task.retry_count}")
    print(f"    max_retries 设置: {max_retries}")
    print(f"    最终状态: {final_task.status.name}")
    print(f"    是否在死信队列: {'[OK] 是' if len(dead_letters) > 0 else '[FAIL] 否'}")
    print(f"    wait_for_result 是否拿到失败结果: {'[OK] 是' if result else '[FAIL] 否'}")
    if result:
        print(f"    结果: success={result.success}, error={result.error.split(chr(10))[0]}")
        print(f"    重试次数: {result.retry_count}")

    assert attempt_count[0] == max_retries + 1, f"应该执行 {max_retries+1} 次 (1次首次 + {max_retries}次重试)，实际 {attempt_count[0]} 次"
    assert final_task.retry_count == max_retries, f"retry_count 应该是 {max_retries}"
    assert final_task.status == TaskStatus.DEAD_LETTER, "最终应该在死信队列"
    assert result and not result.success, "应该能拿到失败结果"
    print("  [OK] 测试 2 通过: 崩溃重派计数正确，超重试上限进死信\n")


def test_dead_letter_result_propagation():
    print("=" * 70)
    print("验收测试 3: 任务最终失败/进死信时，生产者能拿到明确失败结果")
    print("=" * 70)

    shared_redis = fakeredis.FakeRedis(decode_responses=True)
    namespace = f"test_result_{int(time.time())}"
    broker = Broker(namespace=namespace, redis_client=shared_redis)
    producer = Producer(broker)
    worker = Worker(broker, visibility_timeout=30)

    def always_fail_handler(payload):
        raise ValueError("业务逻辑出错啦: division by zero")

    worker.register("fail.task", always_fail_handler)
    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    max_retries = 2
    print(f"\n  步骤1: 投递任务，max_retries={max_retries}")
    task = producer.send("fail.task", {"a": 1, "b": 0}, max_retries=max_retries)
    print(f"  任务 {task.task_id} 已投递")

    print(f"\n  步骤2: Producer 调用 wait_for_result(timeout=60)，等待任务最终结果")
    print(f"  (预期: 任务失败重试3次后进入死信，wait_for_result 能立即拿到失败结果，不会等到超时)")

    wait_start = time.time()
    result = producer.wait_for_result(task.task_id, timeout=60)
    wait_elapsed = time.time() - wait_start

    final_task = broker.get_task(task.task_id)

    worker.stop()
    worker_thread.join(timeout=5)
    broker.stop()

    print(f"\n  结果:")
    print(f"    wait_for_result 耗时: {wait_elapsed:.1f}s (远小于 60s 超时说明及时拿到了结果)")
    print(f"    是否拿到结果: {'[OK] 是' if result else '[FAIL] 否'}")
    if result:
        print(f"    success: {result.success}")
        print(f"    error: {result.error.split(chr(10))[0]}")
        print(f"    retry_count: {result.retry_count}")
        print(f"    executed_at: {time.strftime('%H:%M:%S', time.localtime(result.executed_at))}")
    print(f"    任务最终状态: {final_task.status.name}")

    assert result is not None, "应该能拿到结果，不能等到超时"
    assert result.success is False, "结果应该是失败"
    assert "ValueError" in result.error, "错误信息应该包含异常类型"
    assert wait_elapsed < 30, f"应该很快拿到结果，耗时 {wait_elapsed:.1f}s 太长了"
    assert final_task.status == TaskStatus.DEAD_LETTER, "最终应该在死信"
    print("  [OK] 测试 3 通过: 死信结果能及时回传给生产者\n")


def test_cron_weekday_field():
    print("=" * 70)
    print("验收测试 4: Cron 星期字段按标准习惯处理 (1=周一..5=周五..7=周日)")
    print("=" * 70)

    print(f"\n  今天是: {datetime.now().strftime('%Y-%m-%d %A')}")
    print(f"  Python weekday(): {datetime.now().weekday()} (0=周一, 6=周日)")
    print(f"  Cron 习惯: 1=周一, 2=周二, 3=周三, 4=周四, 5=周五, 6=周六, 7=周日 (0也兼容为周日)")

    print(f"\n  [日历] 最近一周的日期和 Cron weekday 对应关系:")
    today = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
    for i in range(7):
        d = today + timedelta(days=i)
        py_wd = d.weekday()
        cron_wd = CronParser._python_weekday_to_cron(py_wd)
        weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        print(f"    {d.strftime('%Y-%m-%d')} {weekday_names[py_wd]} → Python={py_wd}, Cron={cron_wd}")

    test_cases = [
        ("* * * * 1-5", "工作日 (周一到周五)", "1,2,3,4,5"),
        ("* * * * 1,3,5", "周一、周三、周五", "1,3,5"),
        ("* * * * 6,7", "周末 (周六、周日)", "6,7"),
        ("* * * * 0", "周日 (兼容写法 0)", "7"),
        ("* * * * 7", "周日 (标准写法 7)", "7"),
        ("* * * * 0,6,7", "周六+周日 (混合写法)", "6,7"),
        ("30 9 * * 1-5", "工作日 9:30", "1,2,3,4,5"),
    ]

    print(f"\n  [实验] Cron 表达式解析测试:")
    all_pass = True
    for expr, desc, expected_str in test_cases:
        try:
            parser = CronParser(expr)
            parsed = sorted(parser.fields[4])
            expected = sorted([int(x) for x in expected_str.split(",")])
            ok = parsed == expected
            all_pass = all_pass and ok
            status = "[OK]" if ok else "[FAIL]"
            print(f"    {status} {desc:25s} expr={expr:15s} → weekday field={parsed} (expected={expected})")
        except Exception as e:
            print(f"    [FAIL] {desc:25s} expr={expr:15s} → 解析失败: {e}")
            all_pass = False

    print(f"\n  [预测] 下次运行时间预测 (从今天 {today.strftime('%Y-%m-%d')} 开始):")
    predict_cases = [
        ("0 9 * * 1-5", "工作日 9:00"),
        ("0 9 * * 6,7", "周末 9:00"),
        ("30 14 * * 1", "每周一 14:30"),
        ("0 10 * * 5", "每周五 10:00"),
        ("0 12 * * 7", "每周日 12:00"),
    ]
    for expr, desc in predict_cases:
        parser = CronParser(expr)
        nxt = parser.next_run_after(today)
        py_wd = nxt.weekday()
        cron_wd = CronParser._python_weekday_to_cron(py_wd)
        weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        print(f"    {desc:20s} expr={expr:15s} → 下次: {nxt.strftime('%Y-%m-%d %H:%M')} {weekday_names[py_wd]} (cron_weekday={cron_wd})")
        assert cron_wd in parser.fields[4], f"预测的 {cron_wd} 不在 cron 字段 {parser.fields[4]} 中"

    print(f"\n  [OK] 星期字段匹配验证 (验证 1-5 确实不会触发周末):")
    saturday = today + timedelta(days=(5 - today.weekday() + 7) % 7)
    sunday = today + timedelta(days=(6 - today.weekday() + 7) % 7)
    monday = today + timedelta(days=(0 - today.weekday() + 7) % 7)
    friday = today + timedelta(days=(4 - today.weekday() + 7) % 7)

    parser = CronParser("* * * * 1-5")
    test_days = [
        (monday, "周一", True),
        (friday, "周五", True),
        (saturday, "周六", False),
        (sunday, "周日", False),
    ]
    for d, name, should_match in test_days:
        actual = parser.matches(d)
        ok = actual == should_match
        status = "[OK]" if ok else "[FAIL]"
        all_pass = all_pass and ok
        print(f"    {status} {d.strftime('%Y-%m-%d')} {name} → matches={actual} (expected={should_match})")

    assert all_pass, "Cron 星期字段测试不通过"
    print("  [OK] 测试 4 通过: Cron 星期字段按标准习惯正确处理\n")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("  运行 4 项验收测试，请确保本地 Redis 已启动在 6379 端口")
    print("=" * 70 + "\n")

    try:
        test_cron_weekday_field()
        test_delayed_task_restart_recovery()
        test_worker_crash_retry_count()
        test_dead_letter_result_propagation()

        print("=" * 70)
        print("[OK] 所有 4 项验收测试全部通过!")
        print("=" * 70)
    except Exception as e:
        print(f"\n[FAIL] 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
