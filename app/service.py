"""核心业务逻辑：时间线校验、地区归属、销售导入、更正提交、回填与切换。

并发模型（SQLite WAL + 两个 uvicorn 进程）
------------------------------------------
1. 销售导入：短事务 BEGIN IMMEDIATE，入仓时按当前生效时间线写入归属。
2. 更正提交：BEGIN IMMEDIATE 内做乐观版本检查（expected_version）并插入 backfill
   时间线；region_timeline 上的部分唯一索引 ux_timeline_one_backfill 保证
   同一客户并发更正只有一份能成功。
3. 后台回填：计算本身不持有写锁——读 sales/backfill 时间线做归属计算，
   仅在批量写入 region_staging 时拿短暂写锁；sleep 延迟期间完全不持锁，
   此时销售导入、查询都可正常进行。
4. 切换：一个 BEGIN IMMEDIATE 短事务，把旧 active 置 dead、backfill 置 active、
   按 staging 重写该客户全部销售（含回填期间新增销售，切换前再次查库补算）、
   写修订记录。要么整套生效，要么全部不生效，查询永远看到同一版本的
   时间线与销售归属。
"""
from __future__ import annotations

import csv
import io
import json
import sqlite3
import time
from datetime import date, datetime
from typing import Any

from fastapi import HTTPException

from .db import get_conn, utcnow

# 未归属原因
REASON_NO_TIMELINE = "no_timeline"          # 客户尚无任何生效时间线
REASON_GAP = "gap"                          # 交易日落在有效期空档
REASON_STAGING_GAP = "staging_gap"          # 回填计算时落在新时间线空档


def _safe_rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


# ---------------------------------------------------------------------------
# 日期与时间线工具
# ---------------------------------------------------------------------------

def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"日期格式非法（要求 YYYY-MM-DD）: {value!r}")


def validate_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """校验整套地区有效期：左闭右开、无重叠、无反向区间、地区非空。

    返回按生效日排序后的条目。
    """
    if not entries:
        raise HTTPException(status_code=400, detail="至少需要一条地区有效期")
    cleaned: list[dict[str, Any]] = []
    for i, e in enumerate(entries):
        region = (e.get("region") or "").strip()
        if not region:
            raise HTTPException(status_code=400, detail=f"第 {i + 1} 条记录缺少地区")
        d_from = parse_iso_date(e.get("effective_from"))
        d_to_raw = e.get("effective_to")
        d_to = None
        if d_to_raw not in (None, ""):
            d_to = parse_iso_date(d_to_raw)
            if d_to <= d_from:
                raise HTTPException(
                    status_code=400,
                    detail=f"区间左闭右开，结束日必须晚于开始日：{region} {d_from}~{d_to}",
                )
        cleaned.append(
            {"region": region, "effective_from": d_from.isoformat(),
             "effective_to": d_to.isoformat() if d_to else None}
        )
    cleaned.sort(key=lambda x: x["effective_from"])
    for prev, cur in zip(cleaned, cleaned[1:]):
        if prev["effective_to"] is None:
            raise HTTPException(
                status_code=400,
                detail=f"末段之外的区间不能缺少结束日：{prev['region']} 起始 {prev['effective_from']}",
            )
        if cur["effective_from"] < prev["effective_to"]:
            raise HTTPException(
                status_code=400,
                detail=(f"区间重叠被拒绝：{prev['region']} 到 {prev['effective_to']} 与 "
                        f"{cur['region']} 自 {cur['effective_from']} 重叠"),
            )
    return cleaned


def attribute(txn_date: str, timeline: list[sqlite3.Row] | list[dict[str, Any]]) -> tuple[str | None, str | None]:
    """按左闭右开 [effective_from, effective_to) 判定归属。"""
    for row in timeline:
        ef = row["effective_from"]
        et = row["effective_to"]
        if txn_date >= ef and (et is None or txn_date < et):
            return row["region"], None
    return None, REASON_GAP


def active_version(conn: sqlite3.Connection, customer_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS v FROM region_timeline "
        "WHERE customer_id=? AND status='active'",
        (customer_id,),
    ).fetchone()
    return int(row["v"])


def fetch_timeline(conn: sqlite3.Connection, customer_id: str, status: str, version: int | None = None) -> list[sqlite3.Row]:
    if version is None:
        version = active_version(conn, customer_id)
    return conn.execute(
        "SELECT region, effective_from, effective_to FROM region_timeline "
        "WHERE customer_id=? AND status=? AND version=? ORDER BY effective_from",
        (customer_id, status, version),
    ).fetchall()


def build_diff(old: list[dict[str, Any]], new: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """所见修订：逐条对比新旧整套时间线。"""
    keys = sorted({(e["region"], e["effective_from"], e["effective_to"]) for e in old}
                  | {(e["region"], e["effective_from"], e["effective_to"]) for e in new})
    diff: list[dict[str, Any]] = []
    for region, ef, et in keys:
        in_old = any(e["region"] == region and e["effective_from"] == ef and e["effective_to"] == et for e in old)
        in_new = any(e["region"] == region and e["effective_from"] == ef and e["effective_to"] == et for e in new)
        if in_old and not in_new:
            diff.append({"op": "removed", "region": region, "effective_from": ef, "effective_to": et})
        elif in_new and not in_old:
            diff.append({"op": "added", "region": region, "effective_from": ef, "effective_to": et})
    return diff


# ---------------------------------------------------------------------------
# 销售导入
# ---------------------------------------------------------------------------

def _is_lock_timeout(exc: sqlite3.Error) -> bool:
    """SQLite 忙等上限耗尽（SQLITE_BUSY / database is locked）。"""
    code = getattr(exc, "sqlite_errorcode", None)
    busy_codes = {sqlite3.SQLITE_BUSY}
    for name in ("SQLITE_BUSY_TIMEOUT", "SQLITE_BUSY_SNAPSHOT", "SQLITE_BUSY_RECOVERY"):
        busy_codes.add(getattr(sqlite3, name, sqlite3.SQLITE_BUSY))
    if code in busy_codes:
        return True
    msg = str(exc).lower()
    return "locked" in msg or "busy" in msg


def _lock_timeout_503(batch_size: int) -> HTTPException:
    """等锁超时：整批未入仓，客户端可原样重试（重复销售ID自动跳过）。"""
    return HTTPException(
        status_code=503,
        detail={
            "error": "warehouse_lock_timeout",
            "message": f"入仓写锁等待超过 SQLite 忙等上限，整批 {batch_size} 行未入仓，可安全重试",
            "retryable": True,
        },
        headers={"Retry-After": "1"},
    )


def import_sales_csv(raw: bytes) -> dict[str, int]:
    """CSV 上传入口的入仓工作：解码、表头校验、逐行校验、入库，全部在导入执行器线程内完成。"""
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="文件需为 UTF-8 编码 CSV")
    reader = csv.DictReader(io.StringIO(text))
    required = {"sale_id", "customer_id", "txn_date", "amount_cents"}
    if reader.fieldnames is None or not required.issubset({c.strip() for c in reader.fieldnames}):
        raise HTTPException(status_code=400,
                            detail=f"CSV 表头需包含 {sorted(required)}，实际为 {reader.fieldnames}")
    return import_sales(list(reader))


def import_sales(rows: list[dict[str, str]]) -> dict[str, int]:
    """CSV 入仓：按当前生效时间线直接计算并持久化归属。

    全量校验通过后才写入，任一坏行整批拒绝。重复 sale_id 跳过（幂等再传）。
    """
    if not rows:
        raise HTTPException(status_code=400, detail="CSV 中没有数据行")
    parsed: list[tuple[str, str, date, int]] = []
    seen: set[str] = set()
    for i, r in enumerate(rows):
        sale_id = str(r.get("sale_id") or "").strip()
        customer_id = str(r.get("customer_id") or "").strip()
        txn_raw = str(r.get("txn_date") or "").strip()
        amount_raw = str(r.get("amount_cents") or "").strip()
        if not sale_id or not customer_id or not txn_raw or not amount_raw:
            raise HTTPException(status_code=400, detail=f"第 {i + 2} 行存在空字段（表头算第 1 行）")
        if sale_id in seen:
            raise HTTPException(status_code=400, detail=f"CSV 内销售ID重复：{sale_id}")
        seen.add(sale_id)
        txn = parse_iso_date(txn_raw)
        try:
            amount = int(amount_raw)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"销售 {sale_id} 金额不是整数分：{amount_raw!r}")
        if amount < 0:
            raise HTTPException(status_code=400, detail=f"销售 {sale_id} 金额不能为负：{amount}")
        parsed.append((sale_id, customer_id, txn, amount))

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        inserted = skipped = 0
        for sale_id, customer_id, txn, amount in parsed:
            exists = conn.execute(
                "SELECT 1 FROM sales WHERE sale_id=?", (sale_id,)
            ).fetchone()
            if exists:
                skipped += 1
                continue
            version = active_version(conn, customer_id)
            region: str | None = None
            reason: str | None = None
            if version > 0:
                timeline = fetch_timeline(conn, customer_id, "active", version)
                region, gap = attribute(txn.isoformat(), timeline)
                if gap:
                    reason = REASON_GAP
            else:
                reason = REASON_NO_TIMELINE
            conn.execute(
                "INSERT INTO sales (sale_id, customer_id, txn_date, amount_cents, "
                "region_version, region, unattributed_reason, imported_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (sale_id, customer_id, txn.isoformat(), amount, version, region, reason, utcnow()),
            )
            inserted += 1
        # 归属版本在 BEGIN IMMEDIATE 事务内读取，与提交时生效的地区版本严格一致。
        conn.execute("COMMIT")
        return {"inserted": inserted, "skipped_duplicate": skipped, "total": len(parsed)}
    except sqlite3.OperationalError as exc:
        # 忙等上限耗尽：整批回滚、不入仓，交由调用方拿到可重试的 503。
        _safe_rollback(conn)
        if _is_lock_timeout(exc):
            raise _lock_timeout_503(len(parsed)) from exc
        raise
    except Exception:
        _safe_rollback(conn)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 更正提交（乐观版本 + 数据库级互斥）
# ---------------------------------------------------------------------------

def submit_correction(customer_id: str, entries: list[dict[str, Any]],
                      expected_version: int, backfill_delay_ms: int,
                      fail_simulation: bool, note: str | None) -> dict[str, Any]:
    cleaned = validate_entries(entries)
    if backfill_delay_ms < 0 or backfill_delay_ms > 300000:
        raise HTTPException(status_code=400, detail="backfill_delay_ms 需在 0~300000 之间")

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = active_version(conn, customer_id)
        if expected_version != current:
            _safe_rollback(conn)
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "stale_version",
                    "message": f"修订已过期：所见版本 {expected_version}，当前生效版本 {current}",
                    "current_version": current,
                },
            )
        cur = conn.execute(
            "INSERT INTO correction_job (customer_id, expected_version, new_version, "
            "status, fail_simulation, backfill_delay_ms, note, submitted_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (customer_id, current, current + 1, "pending", 1 if fail_simulation else 0,
             backfill_delay_ms, note, utcnow()),
        )
        job_id = cur.lastrowid
        # 客户级互斥：同客户已有进行中的回填则这里抛 IntegrityError（409），
        # 随事务回滚，刚插入的 job 行也一并撤销。
        try:
            conn.execute(
                "INSERT INTO correction_mutex (customer_id, job_id, locked_at) VALUES (?,?,?)",
                (customer_id, job_id, utcnow()),
            )
        except sqlite3.IntegrityError:
            _safe_rollback(conn)
            raise HTTPException(
                status_code=409,
                detail={"error": "concurrent_correction",
                        "message": f"客户 {customer_id} 已有进行中的更正，并发只允许一份成功"},
            )
        conn.executemany(
            "INSERT INTO region_timeline (customer_id, region, effective_from, "
            "effective_to, version, status, job_id) VALUES (?,?,?,?,?, 'backfill', ?)",
            [(customer_id, e["region"], e["effective_from"], e["effective_to"],
              current + 1, job_id) for e in cleaned],
        )
        conn.execute("COMMIT")
        return {"job_id": job_id, "customer_id": customer_id,
                "expected_version": current, "new_version": current + 1, "status": "pending"}
    except HTTPException:
        raise
    except Exception:
        _safe_rollback(conn)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 后台回填 + 原子切换
# ---------------------------------------------------------------------------

def _fail_job(conn: sqlite3.Connection, job_id: int, message: str) -> None:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE correction_job SET status='failed', finished_at=?, error=? WHERE id=?",
        (utcnow(), message, job_id),
    )
    conn.execute("DELETE FROM region_staging WHERE job_id=?", (job_id,))
    conn.execute("DELETE FROM region_timeline WHERE status='backfill' AND job_id=?", (job_id,))
    conn.execute("DELETE FROM correction_mutex WHERE job_id=?", (job_id,))
    conn.execute("COMMIT")


def run_backfill(job_id: int) -> None:
    """后台线程执行：回填计算（不长期持写锁）→ 原子切换。"""
    conn = get_conn()
    try:
        job = conn.execute("SELECT * FROM correction_job WHERE id=?", (job_id,)).fetchone()
        if job is None or job["status"] not in ("pending", "running"):
            return
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE correction_job SET status='running', started_at=? WHERE id=?",
            (utcnow(), job_id),
        )
        conn.execute("COMMIT")

        customer_id = job["customer_id"]
        new_version = job["new_version"]

        # —— 回填计算阶段：只读，不持有任何写锁；sleep 期间导入/查询照常 ——
        timeline = fetch_timeline(conn, customer_id, "backfill", new_version)
        sales = conn.execute(
            "SELECT sale_id, txn_date, amount_cents FROM sales WHERE customer_id=? ORDER BY sale_id",
            (customer_id,),
        ).fetchall()
        total = len(sales)
        staged: list[tuple[Any, ...]] = []
        delay = max(0, int(job["backfill_delay_ms"]))
        slice_ms = delay // max(total, 1)
        for s in sales:
            region, gap = attribute(s["txn_date"], timeline)
            staged.append((job_id, customer_id, s["sale_id"], s["txn_date"],
                           s["amount_cents"], region, REASON_STAGING_GAP if gap else None))
            if slice_ms:
                time.sleep(slice_ms / 1000.0)

        # staging 批量写入：短暂写锁。
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM region_staging WHERE job_id=?", (job_id,))
        conn.executemany(
            "INSERT INTO region_staging (job_id, customer_id, sale_id, txn_date, "
            "amount_cents, region, reason) VALUES (?,?,?,?,?,?,?)",
            staged,
        )
        conn.execute(
            "UPDATE correction_job SET total_sales=?, staged_sales=? WHERE id=?",
            (total, len(staged), job_id),
        )
        conn.execute("COMMIT")

        if job["fail_simulation"]:
            _fail_job(conn, job_id, "模拟失败：回填结果被丢弃，旧版本继续生效")
            return

        _cutover(conn, job_id)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - 后台任务需兜底，保证状态可观测
        try:
            _fail_job(conn, job_id, f"回填异常：{exc!r}")
        except Exception:
            pass
    finally:
        conn.close()


def _cutover(conn: sqlite3.Connection, job_id: int) -> None:
    """原子切换：时间线、修订、该客户全部销售归属在同一个写事务内一起生效。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = conn.execute("SELECT * FROM correction_job WHERE id=?", (job_id,)).fetchone()
        if job is None or job["status"] != "running":
            raise RuntimeError("任务状态已变化，拒绝切换")
        customer_id = job["customer_id"]
        old_version = int(job["expected_version"])
        new_version = int(job["new_version"])

        # 回填期间新增/漏算的销售：切换事务内再次查库补算，保证一条都不丢。
        staged_ids = {
            r["sale_id"] for r in conn.execute(
                "SELECT sale_id FROM region_staging WHERE job_id=?", (job_id,)
            ).fetchall()
        }
        timeline = fetch_timeline(conn, customer_id, "backfill", new_version)
        extra = conn.execute(
            "SELECT sale_id, txn_date, amount_cents FROM sales "
            "WHERE customer_id=? AND sale_id NOT IN (SELECT sale_id FROM region_staging WHERE job_id=?)",
            (customer_id, job_id),
        ).fetchall()
        extra_rows: list[tuple[Any, ...]] = []
        for s in extra:
            region, gap = attribute(s["txn_date"], timeline)
            extra_rows.append((job_id, customer_id, s["sale_id"], s["txn_date"],
                               s["amount_cents"], region, REASON_STAGING_GAP if gap else None))
        if extra_rows:
            conn.executemany(
                "INSERT INTO region_staging (job_id, customer_id, sale_id, txn_date, "
                "amount_cents, region, reason) VALUES (?,?,?,?,?,?,?)",
                extra_rows,
            )

        # 1) 时间线版本切换
        conn.execute(
            "UPDATE region_timeline SET status='dead' "
            "WHERE customer_id=? AND status='active' AND version=?",
            (customer_id, old_version),
        )
        conn.execute(
            "UPDATE region_timeline SET status='active' "
            "WHERE customer_id=? AND status='backfill' AND version=?",
            (customer_id, new_version),
        )

        # 2) 该客户全部销售按 staging 重写归属
        conn.execute(
            "UPDATE sales SET region_version=?, region=NULL, unattributed_reason=? "
            "WHERE customer_id=?",
            (new_version, "pending_cutover", customer_id),
        )
        conn.execute(
            "UPDATE sales AS s SET region=COALESCE(st.region, s.region), "
            "unattributed_reason=CASE WHEN st.region IS NOT NULL THEN NULL "
            "                          WHEN st.reason='staging_gap' THEN 'gap' "
            "                          ELSE COALESCE(st.reason, ?) END "
            "FROM region_staging AS st "
            "WHERE st.job_id=? AND st.sale_id=s.sale_id",
            (REASON_GAP, job_id),
        )
        leftover = conn.execute(
            "SELECT COUNT(*) AS c FROM sales WHERE customer_id=? AND unattributed_reason='pending_cutover'",
            (customer_id,),
        ).fetchone()["c"]
        if leftover:
            raise RuntimeError(f"切换完整性检查失败：{leftover} 条销售未取得归属")

        # 3) 修订记录（携带所见版本与逐条修订）
        old_rows = conn.execute(
            "SELECT region, effective_from, effective_to FROM region_timeline "
            "WHERE customer_id=? AND version=? ORDER BY effective_from",
            (customer_id, old_version),
        ).fetchall()
        new_rows = conn.execute(
            "SELECT region, effective_from, effective_to FROM region_timeline "
            "WHERE customer_id=? AND version=? ORDER BY effective_from",
            (customer_id, new_version),
        ).fetchall()
        old_payload = [dict(r) for r in old_rows]
        new_payload = [dict(r) for r in new_rows]
        rev_cur = conn.execute(
            "INSERT INTO region_revisions (customer_id, version, supersedes_version, "
            "submitted_at, activated_at, note, payload_json, diff_json) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (customer_id, new_version, old_version, job["submitted_at"], utcnow(),
             job["note"],
             json.dumps(new_payload, ensure_ascii=False),
             json.dumps(build_diff(old_payload, new_payload), ensure_ascii=False)),
        )

        # 4) 任务收尾、清理 staging
        total_sales = conn.execute(
            "SELECT COUNT(*) AS c FROM sales WHERE customer_id=?", (customer_id,)
        ).fetchone()["c"]
        conn.execute(
            "UPDATE correction_job SET status='succeeded', finished_at=?, "
            "total_sales=?, staged_sales=?, revision_id=? WHERE id=?",
            (utcnow(), total_sales, total_sales, rev_cur.lastrowid, job_id),
        )
        conn.execute("DELETE FROM region_staging WHERE job_id=?", (job_id,))
        conn.execute("DELETE FROM correction_mutex WHERE job_id=?", (job_id,))
        conn.execute("COMMIT")
    except Exception:
        _safe_rollback(conn)
        raise
