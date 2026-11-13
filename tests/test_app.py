"""端到端测试：用 TestClient 跑完整 HTTP 链路，数据库指向临时文件。"""
import io
import os
import threading
import time

import pytest

# 必须在导入 app 之前指定数据库路径
_TMP_DB = os.path.join(os.path.dirname(__file__), "_test_sales.db")
os.environ["SALES_DB"] = _TMP_DB

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app import service  # noqa: E402
from app.db import get_conn  # noqa: E402
from app.import_exec import ImportExecutor, import_executor  # noqa: E402


def _wait_all_jobs(client, timeout=30):
    """等待所有后台回填线程到达终态，避免跨用例的守护线程干扰。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = client.get("/api/corrections").json()["jobs"]
        if all(j["status"] in ("succeeded", "failed") for j in jobs):
            return
        time.sleep(0.05)
    raise AssertionError("存在未结束的回填任务")


@pytest.fixture()
def client():
    # 上一个用例可能残留后台线程，先等它结束再删库
    try:
        with TestClient(app) as c:
            _wait_all_jobs(c)
    except Exception:
        pass
    for suffix in ("", "-wal", "-shm"):
        p = _TMP_DB + suffix
        if os.path.exists(p):
            os.remove(p)
    with TestClient(app) as c:
        yield c
    _wait_all_jobs(c)


def _csv(rows: list[tuple]) -> bytes:
    out = "sale_id,customer_id,txn_date,amount_cents\n"
    out += "\n".join(",".join(str(x) for x in r) for r in rows)
    return out.encode()


def _wait_job(client, job_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = client.get(f"/api/corrections/{job_id}").json()
        if d["status"] in ("succeeded", "failed"):
            return d
        time.sleep(0.05)
    raise AssertionError("job timeout")


def test_full_lifecycle_attribution_gap_and_no_timeline(client):
    # 初始时间线：C1 两段，跨 2026-04-01；C2 空档；C3 无时间线
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "华东", "effective_from": "2026-01-01", "effective_to": "2026-04-01"},
            {"region": "华南", "effective_from": "2026-04-01", "effective_to": None},
        ], "backfill_delay_ms": 0})
    assert r.status_code == 200
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    r = client.post("/api/corrections", json={
        "customer_id": "C2", "expected_version": 0,
        "entries": [
            {"region": "华北", "effective_from": "2026-01-01", "effective_to": "2026-05-01"},
            {"region": "西南", "effective_from": "2026-06-01", "effective_to": None},
        ], "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    rows = [
        ("S1", "C1", "2026-03-31", 1000),   # 华东
        ("S2", "C1", "2026-04-01", 2000),   # 华南（边界：左闭右开）
        ("S3", "C2", "2026-05-15", 3000),   # 空档未归属
        ("S4", "C2", "2026-06-01", 4000),   # 西南
        ("S5", "C3", "2026-07-01", 5000),   # 无时间线
    ]
    r = client.post("/api/sales/import",
                    files={"file": ("s.csv", io.BytesIO(_csv(rows)), "text/csv")})
    assert r.status_code == 200
    assert r.json()["inserted"] == 5

    sales = {s["sale_id"]: s for s in client.get("/api/sales").json()["items"]}
    assert sales["S1"]["region"] == "华东" and sales["S1"]["region_version"] == 1
    assert sales["S2"]["region"] == "华南"
    assert sales["S3"]["region"] is None and sales["S3"]["unattributed_reason"] == "gap"
    assert sales["S4"]["region"] == "西南"
    assert sales["S5"]["region"] is None and sales["S5"]["unattributed_reason"] == "no_timeline"

    # 重复导入幂等
    r = client.post("/api/sales/import",
                    files={"file": ("s.csv", io.BytesIO(_csv(rows)), "text/csv")})
    assert r.json()["skipped_duplicate"] == 5


def test_retroactive_correction_backfills_history(client):
    client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "华东", "effective_from": "2026-01-01", "effective_to": "2026-04-01"},
            {"region": "华南", "effective_from": "2026-04-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    # 取 job 并等待
    job = client.get("/api/corrections?customer_id=C1").json()["jobs"][0]
    assert _wait_job(client, job["id"])["status"] == "succeeded"

    client.post("/api/sales/import", files={"file": ("s.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-03-31", 1000), ("S2", "C1", "2026-05-01", 2000)])), "text/csv")})

    # 追溯更正：生效日提前到 2026-03-01
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": [
            {"region": "华东", "effective_from": "2026-01-01", "effective_to": "2026-03-01"},
            {"region": "华南", "effective_from": "2026-03-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    sales = {s["sale_id"]: s for s in client.get("/api/sales?customer_id=C1").json()["items"]}
    assert sales["S1"]["region"] == "华南" and sales["S1"]["region_version"] == 2
    assert sales["S2"]["region"] == "华南" and sales["S2"]["region_version"] == 2

    # 修订记录携带 diff，且查询返回同版时间线快照
    revs = client.get("/api/revisions/C1").json()["revisions"]
    assert revs[-1]["version"] == 2 and revs[-1]["supersedes_version"] == 1
    assert any(d["op"] == "added" and d["effective_from"] == "2026-03-01" for d in revs[-1]["diff"])
    detail = client.get("/api/sales/S1").json()
    assert all(t["version"] == 2 for t in detail["timeline_snapshot"])
    assert client.get("/api/consistency").json()["consistent"] is True


def test_overlap_rejected_and_half_open_boundary(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "A", "effective_from": "2026-01-01", "effective_to": "2026-03-01"},
            {"region": "B", "effective_from": "2026-02-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert r.status_code == 400 and "重叠" in r.json()["detail"]

    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "A", "effective_from": "2026-03-01", "effective_to": "2026-03-01"}],
        "backfill_delay_ms": 0})
    assert r.status_code == 400

    # 背靠背区间合法：[01-01,03-01) + [03-01,...)，边界日归后者
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [
            {"region": "A", "effective_from": "2026-01-01", "effective_to": "2026-03-01"},
            {"region": "B", "effective_from": "2026-03-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert r.status_code == 200
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"


def test_stale_version_rejected(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "A", "effective_from": "2026-01-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    # 当前已为 v1，仍携带 v0 提交 -> 过期拒绝
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "B", "effective_from": "2026-01-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "stale_version"


def test_concurrent_correction_only_one_succeeds(client):
    barrier = threading.Barrier(2)
    results = []

    def submit(delay_ms):
        barrier.wait()
        r = client.post("/api/corrections", json={
            "customer_id": "C1", "expected_version": 0,
            "entries": [{"region": f"R{delay_ms}",
                         "effective_from": "2026-01-01", "effective_to": None}],
            "backfill_delay_ms": delay_ms})
        results.append(r.status_code)

    t1 = threading.Thread(target=submit, args=(1500,))
    t2 = threading.Thread(target=submit, args=(1200,))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(results) == [200, 409]

    # 等两份任务都结束：成功的那份会跑完，被拒那份根本没建任务
    for j in client.get("/api/corrections?customer_id=C1").json()["jobs"]:
        _wait_job(client, j["id"])
    tl = client.get("/api/timeline/C1").json()
    assert tl["version"] == 1 and tl["backfill"] == []


def test_imports_during_backfill_are_included_in_cutover(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "华东", "effective_from": "2026-01-01",
                     "effective_to": "2026-04-01"},
                    {"region": "华南", "effective_from": "2026-04-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100)])), "text/csv")})

    # 带延迟的更正，回填进行中插入新销售
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": [{"region": "华北", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 1500})
    job_id = r.json()["job_id"]
    time.sleep(0.4)
    # 回填期间仍可导入（不被写锁阻塞）
    t0 = time.time()
    r2 = client.post("/api/sales/import", files={"file": ("b.csv", io.BytesIO(_csv([
        ("S2", "C1", "2026-02-10", 200)])), "text/csv")})
    assert r2.status_code == 200
    assert time.time() - t0 < 5  # 导入没有被回填阻塞

    job = _wait_job(client, job_id)
    assert job["status"] == "succeeded" and job["total_sales"] == 2
    sales = {s["sale_id"]: s for s in client.get("/api/sales?customer_id=C1").json()["items"]}
    assert sales["S1"]["region"] == "华北" and sales["S1"]["region_version"] == 2
    assert sales["S2"]["region"] == "华北" and sales["S2"]["region_version"] == 2


def test_failed_correction_keeps_old_version(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "A", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"

    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100)])), "text/csv")})

    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": [{"region": "B", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0, "fail_simulation": True})
    job = _wait_job(client, r.json()["job_id"])
    assert job["status"] == "failed" and job["error"]

    tl = client.get("/api/timeline/C1").json()
    assert tl["version"] == 1 and tl["entries"][0]["region"] == "A"
    sales = client.get("/api/sales").json()["items"]
    assert sales[0]["region"] == "A" and sales[0]["region_version"] == 1
    # 失败后允许重新提交（互斥已释放）
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 1,
        "entries": [{"region": "B", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    assert r.status_code == 200
    assert _wait_job(client, r.json()["job_id"])["status"] == "succeeded"


def test_persistence_across_restart(client, tmp_path):
    r = client.post("/api/corrections", json={
        "customer_id": "C1", "expected_version": 0,
        "entries": [{"region": "A", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    _wait_job(client, r.json()["job_id"])
    client.post("/api/sales/import", files={"file": ("a.csv", io.BytesIO(_csv([
        ("S1", "C1", "2026-01-10", 100)])), "text/csv")})

    # 模拟重启：重新 init_db（恢复逻辑）并新起一个 TestClient
    from app.db import init_db
    init_db()
    with TestClient(app) as c2:
        sales = c2.get("/api/sales").json()["items"]
        assert sales[0]["region"] == "A"
        assert c2.get("/api/timeline/C1").json()["version"] == 1
        assert c2.get("/api/consistency").json()["consistent"] is True


def test_json_import_endpoint(client):
    r = client.post("/api/sales/import-json", json={"rows": [
        {"sale_id": "J1", "customer_id": "C9", "txn_date": "2026-01-05", "amount_cents": 123}]})
    assert r.status_code == 200 and r.json()["inserted"] == 1
    item = client.get("/api/sales").json()["items"][0]
    assert item["sale_id"] == "J1" and item["unattributed_reason"] == "no_timeline"


def test_gap_reason_normalized_after_cutover(client):
    r = client.post("/api/corrections", json={
        "customer_id": "C2", "expected_version": 0,
        "entries": [
            {"region": "华北", "effective_from": "2026-01-01", "effective_to": "2026-05-01"},
            {"region": "西南", "effective_from": "2026-06-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    _wait_job(client, r.json()["job_id"])
    client.post("/api/sales/import", files={"file": ("g.csv", io.BytesIO(_csv([
        ("G1", "C2", "2026-05-15", 1)])), "text/csv")})
    # 再做一次更正触发回填，staging_gap 切换后必须归一为 gap
    r = client.post("/api/corrections", json={
        "customer_id": "C2", "expected_version": 1,
        "entries": [
            {"region": "华北", "effective_from": "2026-01-01", "effective_to": "2026-05-01"},
            {"region": "西南", "effective_from": "2026-06-01", "effective_to": None}],
        "backfill_delay_ms": 0})
    _wait_job(client, r.json()["job_id"])
    item = [s for s in client.get("/api/sales").json()["items"] if s["sale_id"] == "G1"][0]
    assert item["region"] is None and item["unattributed_reason"] == "gap"
    assert client.get("/api/consistency").json()["consistent"] is True


def test_bad_csv_rejected_entirely(client):
    r = client.post("/api/sales/import", files={"file": ("bad.csv", io.BytesIO(
        b"sale_id,customer_id,txn_date,amount_cits\nX,C1,2026-01-01,100"), "text/csv")})
    assert r.status_code == 400
    r = client.post("/api/sales/import", files={"file": ("bad.csv", io.BytesIO(
        b"sale_id,customer_id,txn_date,amount_cents\nX,C1,2026-01-01,abc"), "text/csv")})
    assert r.status_code == 400
    assert client.get("/api/sales").json()["total"] == 0


# ---------------------------------------------------------------------------
# 入仓写锁拥塞：专用执行器、可重试 503、断连收尾、读接口不卡
# ---------------------------------------------------------------------------

def _json_row(sale_id, customer_id="C1", txn_date="2026-01-10", amount=100):
    return {"sale_id": sale_id, "customer_id": customer_id,
            "txn_date": txn_date, "amount_cents": amount}


def test_lock_timeout_returns_503_and_batch_not_imported(client, monkeypatch):
    # 仅测试缩短忙等上限；生产默认保持 SQLite 原 30 秒
    monkeypatch.setenv("SALES_BUSY_TIMEOUT_MS", "300")
    lock = get_conn()
    lock.execute("BEGIN IMMEDIATE")
    try:
        t0 = time.time()
        r = client.post("/api/sales/import", files={"file": ("s.csv", io.BytesIO(_csv([
            ("LT1", "C1", "2026-01-10", 100)])), "text/csv")})
        assert r.status_code == 503
        assert time.time() - t0 < 5, "忙等上限耗尽后应立即失败，而不是挂满 30s"
        assert r.headers["retry-after"] == "1"
        detail = r.json()["detail"]
        assert detail["error"] == "warehouse_lock_timeout"
        assert detail["retryable"] is True
        # 该批整批不入仓
        assert client.get("/api/sales/LT1").status_code == 404
        assert client.get("/api/sales").json()["total"] == 0
    finally:
        lock.execute("ROLLBACK")
        lock.close()
    # 锁释放后导入恢复可用；重传仍按销售ID去重
    r = client.post("/api/sales/import-json", json={"rows": [_json_row("LT1")]})
    assert r.status_code == 200 and r.json()["inserted"] == 1
    r = client.post("/api/sales/import-json", json={"rows": [_json_row("LT1")]})
    assert r.json()["skipped_duplicate"] == 1


def test_reads_stay_fast_during_40_concurrent_imports(client):
    """每进程 40 份并发导入等锁期间，/api/sales 与 /api/health 仍在 1 秒内返回。"""
    lock = get_conn()
    lock.execute("BEGIN IMMEDIATE")
    results, barrier = [], threading.Barrier(40)

    def one(i):
        barrier.wait()
        r = client.post("/api/sales/import-json",
                        json={"rows": [_json_row(f"CF{i}", amount=100 + i)]})
        results.append(r.status_code)

    threads = [threading.Thread(target=one, args=(i,)) for i in range(40)]
    try:
        for t in threads:
            t.start()
        time.sleep(0.5)  # 让 40 份导入全部进入等锁（占满导入执行器并排队）
        max_ms = 0.0
        for _ in range(6):
            for path in ("/api/health", "/api/sales?limit=1"):
                t0 = time.time()
                assert client.get(path).status_code == 200
                max_ms = max(max_ms, (time.time() - t0) * 1000)
            time.sleep(0.1)
        assert max_ms < 1000, f"读接口最大延迟 {max_ms:.0f}ms 超过 1 秒要求"
    finally:
        lock.execute("ROLLBACK")
        lock.close()
        for t in threads:
            t.join()
    # 锁在 30s 忙等内释放：40 份全部入仓
    assert sorted(results) == [200] * 40
    assert client.get("/api/sales?customer_id=C1&limit=100").json()["total"] == 40


def test_disconnect_running_job_completes_and_releases_slot(client):
    """断连（取消等待）不能中断已开始的批次：整批提交后连接与名额才释放。"""
    lock = get_conn()
    lock.execute("BEGIN IMMEDIATE")
    fut = import_executor.submit(service.import_sales, [_json_row("DX1")])
    assert fut is not None
    time.sleep(0.5)  # 工作者已开始等锁
    assert import_executor.pending == 1
    # 模拟客户端断连：运行中的任务不可被取消
    assert fut.cancel() is False
    assert import_executor.pending == 1, "名额不得在批次收尾前释放"
    lock.execute("ROLLBACK")
    lock.close()
    assert fut.result(timeout=10)["inserted"] == 1  # 断连后仍整批提交
    deadline = time.time() + 5
    while import_executor.pending != 0 and time.time() < deadline:
        time.sleep(0.05)
    assert import_executor.pending == 0
    assert client.get("/api/sales/DX1").status_code == 200


def test_disconnect_queued_job_cancelled_frees_slot(client):
    """断连时未开始的排队任务被取消、名额归还；已开始的任务照常完成。"""
    lock = get_conn()
    lock.execute("BEGIN IMMEDIATE")
    f1 = import_executor.submit(service.import_sales, [_json_row("Q1")])
    f2 = import_executor.submit(service.import_sales, [_json_row("Q2")])
    time.sleep(0.5)  # 两个导入工作者都在等锁
    f3 = import_executor.submit(service.import_sales, [_json_row("Q3")])
    assert f3 is not None
    time.sleep(0.2)
    assert import_executor.pending == 3
    assert f3.cancel() is True  # 排队未开始 → 可取消
    lock.execute("ROLLBACK")
    lock.close()
    assert f1.result(timeout=10)["inserted"] == 1
    assert f2.result(timeout=10)["inserted"] == 1
    deadline = time.time() + 5
    while import_executor.pending != 0 and time.time() < deadline:
        time.sleep(0.05)
    assert import_executor.pending == 0
    assert client.get("/api/sales/Q1").status_code == 200
    assert client.get("/api/sales/Q2").status_code == 200
    assert client.get("/api/sales/Q3").status_code == 404  # 未开始的批次不执行


def test_import_queue_full_returns_none_and_recovers():
    """有界排队：在途达上限后拒绝新任务（端点映射为可重试 503），收尾后恢复。"""
    ex = ImportExecutor(workers=1, max_pending=1)
    lock = get_conn()
    lock.execute("BEGIN IMMEDIATE")
    try:
        f1 = ex.submit(service.import_sales, [_json_row("F1")])
        assert f1 is not None
        time.sleep(0.3)
        assert ex.submit(service.import_sales, [_json_row("F2")]) is None
        assert ex.pending == 1
    finally:
        lock.execute("ROLLBACK")
        lock.close()
    assert f1.result(timeout=10)["inserted"] == 1
    deadline = time.time() + 5
    while ex.pending != 0 and time.time() < deadline:
        time.sleep(0.05)
    assert ex.pending == 0


def test_import_queue_full_http_503(client, monkeypatch):
    monkeypatch.setattr(import_executor, "_max_pending", 0)
    r = client.post("/api/sales/import-json", json={"rows": [_json_row("FULL1")]})
    assert r.status_code == 503
    assert r.headers["retry-after"] == "1"
    assert r.json()["detail"]["error"] == "import_queue_full"
    assert r.json()["detail"]["retryable"] is True


def test_demo_lock_endpoint(client):
    assert client.post("/api/demo/lock", json={"hold_ms": 50}).status_code == 400
    r = client.post("/api/demo/lock", json={"hold_ms": 600, "note": "t"})
    assert r.status_code == 200 and r.json()["held"] is True
    # 已持有时重复持锁 → 409
    assert client.post("/api/demo/lock", json={"hold_ms": 600}).status_code == 409
    assert client.get("/api/demo/lock").json()["held"] is True
    time.sleep(0.9)  # 到期自动释放
    assert client.get("/api/demo/lock").json()["held"] is False
    # 持锁期间读接口照常（WAL 读不阻塞）
    r = client.post("/api/demo/lock", json={"hold_ms": 800})
    assert r.status_code == 200
    t0 = time.time()
    assert client.get("/api/sales").status_code == 200
    assert client.get("/api/health").status_code == 200
    assert time.time() - t0 < 1
    client.delete("/api/demo/lock")
    assert client.get("/api/demo/lock").json()["held"] is False
