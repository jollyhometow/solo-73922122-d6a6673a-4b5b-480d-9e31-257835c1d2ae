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
