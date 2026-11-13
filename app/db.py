"""SQLite 连接管理、表结构与启动恢复。

设计要点：
- WAL 模式：多进程（两个 Web 容器）共享同一个数据库文件，读不阻塞写、写不阻塞读。
- 显式事务管理（isolation_level=None），需要写锁时使用 BEGIN IMMEDIATE，
  配合 busy_timeout 让并发写等待而不是立刻报错。
- 地区归属直接落库（sales.region / region_version / unattributed_reason），
  查询时不做任何临时重算。
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS region_timeline (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id    TEXT NOT NULL,
    region         TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to   TEXT,
    version        INTEGER NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('active', 'backfill', 'dead')),
    job_id         INTEGER
);
CREATE INDEX IF NOT EXISTS idx_timeline_lookup
    ON region_timeline (customer_id, status, version);

-- 同一客户同时只允许一份进行中的更正：客户级互斥（一行锁，切走/失败即释放）
CREATE TABLE IF NOT EXISTS correction_mutex (
    customer_id TEXT PRIMARY KEY,
    job_id      INTEGER NOT NULL,
    locked_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
    sale_id             TEXT PRIMARY KEY,
    customer_id         TEXT NOT NULL,
    txn_date            TEXT NOT NULL,
    amount_cents        INTEGER NOT NULL,
    region_version      INTEGER NOT NULL,
    region              TEXT,
    unattributed_reason TEXT,
    imported_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales (customer_id, txn_date);
CREATE INDEX IF NOT EXISTS idx_sales_version  ON sales (region_version);

CREATE TABLE IF NOT EXISTS region_revisions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    supersedes_version  INTEGER NOT NULL,
    submitted_at        TEXT NOT NULL,
    activated_at        TEXT NOT NULL,
    note                TEXT,
    payload_json        TEXT NOT NULL,
    diff_json           TEXT,
    UNIQUE (customer_id, version)
);

CREATE TABLE IF NOT EXISTS correction_job (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id       TEXT NOT NULL,
    expected_version  INTEGER NOT NULL,
    new_version       INTEGER NOT NULL,
    status            TEXT NOT NULL
                        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    fail_simulation   INTEGER NOT NULL DEFAULT 0,
    backfill_delay_ms INTEGER NOT NULL DEFAULT 0,
    total_sales       INTEGER NOT NULL DEFAULT 0,
    staged_sales      INTEGER NOT NULL DEFAULT 0,
    note              TEXT,
    submitted_at      TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT,
    error             TEXT,
    revision_id       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_job_customer ON correction_job (customer_id, id);

CREATE TABLE IF NOT EXISTS region_staging (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL,
    customer_id  TEXT NOT NULL,
    sale_id      TEXT NOT NULL,
    txn_date     TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    region       TEXT,
    reason       TEXT,
    UNIQUE (job_id, sale_id)
);
CREATE INDEX IF NOT EXISTS idx_staging_job ON region_staging (job_id);
"""


def db_path() -> str:
    return os.environ.get("SALES_DB", "/data/sales.db")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_conn() -> sqlite3.Connection:
    """每个调用方（请求线程 / 后台回填线程）使用自己的连接。"""
    conn = sqlite3.connect(db_path(), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # 自动提交模式，事务全部由代码显式 BEGIN/COMMIT，避免隐式事务与 IMMEDIATE 冲突。
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """建表，并对上次进程异常退出时残留的回填任务做恢复。

    SQLite 的 COMMIT 是原子的：若切换事务没提交，旧时间线就仍然完好，
    残留的 backfill 行与 staging 行直接清掉、任务标 failed 即可。
    """
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        stale = conn.execute(
            "SELECT id FROM correction_job WHERE status IN ('pending', 'running')"
        ).fetchall()
        for row in stale:
            job_id = row["id"]
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE correction_job SET status='failed', finished_at=?, "
                    "error=? WHERE id=?",
                    (utcnow(), "进程重启，任务被中断，旧版本继续生效", job_id),
                )
                conn.execute("DELETE FROM region_staging WHERE job_id=?", (job_id,))
                conn.execute(
                    "DELETE FROM region_timeline WHERE status='backfill' AND job_id=?",
                    (job_id,),
                )
                conn.execute(
                    "DELETE FROM correction_mutex WHERE job_id=?", (job_id,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
    finally:
        conn.close()
