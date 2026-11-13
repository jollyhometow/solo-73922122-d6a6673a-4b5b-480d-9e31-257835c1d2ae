"""FastAPI 入口：销售导入、查询、地区有效期维护、更正回填与演示入口。"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import socket
import threading
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from . import service
from .db import get_conn, init_db
from .demo_lock import demo_lock
from .import_exec import import_executor
from .sample_data import CORRECTION_SAMPLE_C001, SAMPLE_CSV, SEED_TIMELINES
from .schemas import CorrectionIn

app = FastAPI(title="销售地区归属追溯更正平台", version="1.0.0")

_STATIC_DIR = Path(__file__).parent / "static"

REASON_TEXT = {
    service.REASON_NO_TIMELINE: "客户尚无生效地区时间线",
    service.REASON_GAP: "交易日落在地区有效期空档（左闭右开区间未覆盖）",
    service.REASON_STAGING_GAP: "新时间线空档（回填计算）",
    "pending_cutover": "切换进行中",
}


def _spawn_backfill(job_id: int) -> None:
    # 守护线程：与请求线程独立，sleep 模拟回填延迟期间不持有任何数据库锁。
    threading.Thread(target=service.run_backfill, args=(job_id,), daemon=True).start()


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "host": socket.gethostname()}


# ---------------------------------------------------------------------------
# 销售导入与查询
# ---------------------------------------------------------------------------

def _queue_full_503() -> HTTPException:
    """导入排队已满：瞬态过载，稍后重试（与锁超时同样的可重试 503）。"""
    return HTTPException(
        status_code=503,
        detail={
            "error": "import_queue_full",
            "message": f"导入在途任务已达每进程上限 {import_executor.max_pending}，请稍后重试",
            "retryable": True,
        },
        headers={"Retry-After": "1"},
    )


async def _run_import(fn, *args) -> dict:
    """把整批入仓工作交给导入专用执行器，协程等待结果。

    - 不占 anyio 共享线程池（/api/sales、/api/health 等同步接口不受影响）；
    - 客户端断连时：排队中的任务被取消（名额归还），已开始的任务在执行器
      线程内继续跑到整批提交或回滚，连接关闭后名额才释放。
    """
    fut = import_executor.submit(fn, *args)
    if fut is None:
        raise _queue_full_503()
    return await asyncio.wrap_future(fut)


@app.post("/api/sales/import")
async def import_sales(file: UploadFile) -> dict:
    raw = await file.read()
    result = await _run_import(service.import_sales_csv, raw)
    result["served_by"] = socket.gethostname()
    return result


@app.get("/api/sales/sample.csv", response_class=PlainTextResponse)
def sample_csv() -> str:
    return SAMPLE_CSV


@app.post("/api/sales/import-json")
async def import_sales_json(payload: dict) -> dict:
    """程序化导入入口（无 multipart 依赖），与 CSV 上传共用同一套入库归属逻辑。"""
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="请求体需为 {'rows': [{...}, ...]}")
    result = await _run_import(service.import_sales, rows)
    result["served_by"] = socket.gethostname()
    return result


@app.get("/api/sales")
def list_sales(customer_id: str | None = None,
               unattributed_only: bool = False,
               limit: int = Query(500, ge=1, le=5000),
               offset: int = Query(0, ge=0)) -> dict:
    """销售明细：归属来自入库存储，不做临时重算。"""
    conn = get_conn()
    try:
        where, params = [], []
        if customer_id:
            where.append("customer_id=?")
            params.append(customer_id)
        if unattributed_only:
            where.append("region IS NULL")
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        total = conn.execute(f"SELECT COUNT(*) AS c FROM sales {clause}", params).fetchone()["c"]
        rows = conn.execute(
            f"SELECT sale_id, customer_id, txn_date, amount_cents, region_version, "
            f"region, unattributed_reason, imported_at FROM sales {clause} "
            f"ORDER BY customer_id, txn_date, sale_id LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        items = []
        for r in rows:
            d = dict(r)
            d["unattributed_reason_text"] = (
                REASON_TEXT.get(d["unattributed_reason"], d["unattributed_reason"])
                if d["unattributed_reason"] else None)
            items.append(d)
        return {"total": total, "limit": limit, "offset": offset,
                "served_by": socket.gethostname(), "items": items}
    finally:
        conn.close()


@app.get("/api/sales/{sale_id}")
def sale_detail(sale_id: str) -> dict:
    conn = get_conn()
    try:
        r = conn.execute("SELECT * FROM sales WHERE sale_id=?", (sale_id,)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail=f"销售 {sale_id} 不存在")
        d = dict(r)
        d["unattributed_reason_text"] = (
            REASON_TEXT.get(d["unattributed_reason"], d["unattributed_reason"])
            if d["unattributed_reason"] else None)
        # 同版时间线随明细一起返回，便于核对「同一版本的时间线与销售归属」
        d["timeline_snapshot"] = [
            dict(x) for x in conn.execute(
                "SELECT region, effective_from, effective_to, version, status "
                "FROM region_timeline WHERE customer_id=? AND status='active' "
                "AND version=? ORDER BY effective_from",
                (d["customer_id"], d["region_version"]))
        ]
        d["served_by"] = socket.gethostname()
        return d
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 客户地区时间线
# ---------------------------------------------------------------------------

@app.get("/api/customers")
def list_customers() -> dict:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT customer_id, MAX(CASE WHEN status='active' THEN version END) AS active_version, "
            "SUM(CASE WHEN status='backfill' THEN 1 ELSE 0 END) AS backfill_segments "
            "FROM region_timeline GROUP BY customer_id ORDER BY customer_id"
        ).fetchall()
        customers = {r["customer_id"]: int(r["active_version"] or 0) for r in rows}
        # 有销售但从未建时间线的客户也要展示
        for r in conn.execute("SELECT DISTINCT customer_id FROM sales ORDER BY customer_id"):
            customers.setdefault(r["customer_id"], 0)
        return {"customers": [
            {"customer_id": cid, "active_version": ver,
             "backfill_in_progress": any(
                 r["customer_id"] == cid and (r["backfill_segments"] or 0) > 0 for r in rows)}
            for cid, ver in sorted(customers.items())],
            "served_by": socket.gethostname()}
    finally:
        conn.close()


@app.get("/api/timeline/{customer_id}")
def get_timeline(customer_id: str) -> dict:
    """返回当前生效整套时间线（所见修订版本号由前端随更正提交带回）。"""
    conn = get_conn()
    try:
        version = service.active_version(conn, customer_id)
        entries = [dict(r) for r in service.fetch_timeline(conn, customer_id, "active", version)]
        backfill = [dict(r) for r in conn.execute(
            "SELECT region, effective_from, effective_to, version, job_id "
            "FROM region_timeline WHERE customer_id=? AND status='backfill' ORDER BY effective_from",
            (customer_id,))]
        return {"customer_id": customer_id, "version": version, "entries": entries,
                "backfill": backfill, "served_by": socket.gethostname()}
    finally:
        conn.close()


@app.post("/api/timeline/{customer_id}")
def replace_timeline(customer_id: str, payload: dict, background: BackgroundTasks) -> dict:
    """直接按客户替换整套地区有效期（内部走与更正相同的互斥/回填/切换链路）。

    首次建立时间线时 expected_version=0。
    """
    expected_version = int(payload.get("expected_version", -1))
    entries = payload.get("entries", [])
    delay = int(payload.get("backfill_delay_ms", 0))
    fail_sim = bool(payload.get("fail_simulation", False))
    note = payload.get("note")
    created = service.submit_correction(customer_id, entries, expected_version,
                                        delay, fail_sim, note)
    background.add_task(_spawn_backfill, created["job_id"])
    return created


# ---------------------------------------------------------------------------
# 追溯更正（携带所见修订）
# ---------------------------------------------------------------------------

@app.post("/api/corrections")
def create_correction(payload: CorrectionIn, background: BackgroundTasks) -> dict:
    entries = [e.model_dump() for e in payload.entries]
    created = service.submit_correction(
        payload.customer_id, entries, payload.expected_version,
        payload.backfill_delay_ms, payload.fail_simulation, payload.note)
    background.add_task(_spawn_backfill, created["job_id"])
    return created


@app.get("/api/corrections")
def list_jobs(customer_id: str | None = None) -> dict:
    conn = get_conn()
    try:
        if customer_id:
            rows = conn.execute(
                "SELECT * FROM correction_job WHERE customer_id=? ORDER BY id DESC LIMIT 100",
                (customer_id,))
        else:
            rows = conn.execute(
                "SELECT * FROM correction_job ORDER BY id DESC LIMIT 100")
        return {"jobs": [dict(r) for r in rows], "served_by": socket.gethostname()}
    finally:
        conn.close()


@app.get("/api/corrections/{job_id}")
def job_detail(job_id: int) -> dict:
    conn = get_conn()
    try:
        r = conn.execute("SELECT * FROM correction_job WHERE id=?", (job_id,)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="任务不存在")
        d = dict(r)
        d["staging_preview"] = [dict(x) for x in conn.execute(
            "SELECT sale_id, txn_date, amount_cents, region, reason "
            "FROM region_staging WHERE job_id=? ORDER BY sale_id", (job_id,))]
        return d
    finally:
        conn.close()


@app.get("/api/revisions/{customer_id}")
def list_revisions(customer_id: str) -> dict:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, customer_id, version, supersedes_version, submitted_at, "
            "activated_at, note, payload_json, diff_json FROM region_revisions "
            "WHERE customer_id=? ORDER BY version", (customer_id,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d.pop("payload_json"))
            d["diff"] = json.loads(d.pop("diff_json")) if d["diff_json"] else []
            out.append(d)
        return {"customer_id": customer_id, "revisions": out,
                "served_by": socket.gethostname()}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 一致性自检与演示数据
# ---------------------------------------------------------------------------

@app.get("/api/consistency")
def consistency() -> dict:
    """校验：所有销售归属与其版本对应时间线完全一致（切换原子性的可观测证明）。"""
    conn = get_conn()
    try:
        mismatches, checked = [], 0
        rows = conn.execute(
            "SELECT s.sale_id, s.customer_id, s.txn_date, s.region_version, s.region, "
            "s.unattributed_reason FROM sales s "
            "WHERE s.region_version > 0 ORDER BY s.sale_id").fetchall()
        for s in rows:
            checked += 1
            tl = service.fetch_timeline(conn, s["customer_id"], "active", s["region_version"])
            region, gap = service.attribute(s["txn_date"], tl)
            if region != s["region"] or (gap or None) != (s["unattributed_reason"] or None):
                mismatches.append({"sale_id": s["sale_id"], "stored_region": s["region"],
                                   "expected_region": region,
                                   "stored_reason": s["unattributed_reason"],
                                   "expected_reason": gap})
        unattributed = conn.execute(
            "SELECT COUNT(*) AS c FROM sales WHERE region IS NULL").fetchone()["c"]
        return {"checked": checked, "mismatches": mismatches,
                "unattributed_count": unattributed,
                "consistent": not mismatches,
                "served_by": socket.gethostname()}
    finally:
        conn.close()


@app.post("/api/demo/seed")
def seed_demo(background: BackgroundTasks) -> dict:
    """幂等灌入演示时间线与样例销售（已存在则跳过）。"""
    conn = get_conn()
    try:
        existing_sales = conn.execute("SELECT COUNT(*) AS c FROM sales").fetchone()["c"]
    finally:
        conn.close()
    if existing_sales == 0:
        service.import_sales(list(csv.DictReader(io.StringIO(SAMPLE_CSV))))
    jobs = []
    for cid, entries in SEED_TIMELINES.items():
        conn = get_conn()
        try:
            ver = service.active_version(conn, cid)
        finally:
            conn.close()
        if ver == 0:
            created = service.submit_correction(cid, entries, 0, 0, False, "演示初始时间线")
            # 初始时间线没有历史销售归属需要等待，同步执行回填+切换即可
            service.run_backfill(created["job_id"])
            jobs.append(created)
    return {"seeded_jobs": jobs, "message": "样例客户 C001/C002 时间线与 10 条销售已就绪，C003 无时间线"}


@app.get("/api/demo/correction-template/{customer_id}")
def correction_template(customer_id: str) -> dict:
    if customer_id != "C001":
        raise HTTPException(status_code=404, detail="内置模板仅提供 C001 的追溯更正样例")
    return {"customer_id": "C001", "entries": CORRECTION_SAMPLE_C001,
            "说明": "生效日由 2026-04-01 提前到 2026-03-01，S1002(2026-03-31) 回填后应改挂华南"}


# ---------------------------------------------------------------------------
# 写锁拥塞演示：可控持锁（nginx 已把 /api/demo/lock 固定路由到 web1）
# ---------------------------------------------------------------------------

@app.post("/api/demo/lock")
def demo_lock_acquire(payload: dict) -> dict:
    """持有 SQLite 写锁 hold_ms 毫秒（到期自动释放），复现入仓写锁拥塞。"""
    try:
        hold_ms = int(payload.get("hold_ms", 10000))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="hold_ms 需为整数毫秒")
    result = demo_lock.acquire(hold_ms, payload.get("note"))
    result["hint"] = ("写锁持有中：导入会在 SQLite 忙等上限（30s）内等待，超时整批不入仓、"
                      "返回可重试 503；/api/sales 与 /api/health 不受影响")
    return result


@app.delete("/api/demo/lock")
def demo_lock_release() -> dict:
    """提前释放演示写锁。"""
    return demo_lock.release()


@app.get("/api/demo/lock")
def demo_lock_status() -> dict:
    return demo_lock.status()


@app.get("/api/demo/import-executor")
def import_executor_status() -> dict:
    """导入专用执行器观测：名额、在途任务数（验证断连后名额不提前释放）。"""
    return {"workers": import_executor.workers,
            "max_pending": import_executor.max_pending,
            "pending": import_executor.pending,
            "served_by": socket.gethostname()}


# ---------------------------------------------------------------------------
# 浏览器入口
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")
