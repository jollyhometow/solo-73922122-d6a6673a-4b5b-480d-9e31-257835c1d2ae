"""可控持锁演示：人为占用 SQLite 写锁，复现入仓写锁拥塞。

锁由独立守护线程 + 专属连接持有（BEGIN IMMEDIATE，到期 ROLLBACK），
与请求线程、导入执行器完全隔离。持锁期间：

- 写者（两个导入入口、更正提交、回填切换）在 SQLite 忙等上限内等待；
- 读者（/api/sales、/api/health 等）在 WAL 下不受影响，照常返回已提交状态。

浏览器 / 脚本通过 /api/demo/lock 控制持锁时长或提前释放。
nginx 已把该控制面固定到 web1（锁本身是数据库级写锁，对两个进程都生效）。
"""
from __future__ import annotations

import socket
import threading

from fastapi import HTTPException

from .db import get_conn, utcnow

MAX_HOLD_MS = 180_000


class DemoLock:
    """进程内单例的可控写锁持有器。"""

    def __init__(self) -> None:
        self._mu = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._acquired = threading.Event()
        self._error: str | None = None
        self._info: dict = {}

    def status(self) -> dict:
        with self._mu:
            held = self._thread is not None and self._thread.is_alive()
            info = dict(self._info) if held else {}
        return {"held": held, **info}

    def acquire(self, hold_ms: int, note: str | None) -> dict:
        if not 100 <= hold_ms <= MAX_HOLD_MS:
            raise HTTPException(
                status_code=400, detail=f"hold_ms 需在 100~{MAX_HOLD_MS} 之间")
        with self._mu:
            if self._thread is not None and self._thread.is_alive():
                raise HTTPException(
                    status_code=409, detail="演示锁已被持有，请先释放或等待自动释放")
            self._stop.clear()
            self._acquired.clear()
            self._error = None
            self._thread = threading.Thread(
                target=self._hold, args=(hold_ms, note),
                name="demo-lock-holder", daemon=True)
            self._thread.start()
        # 等持锁线程真正拿到写锁再返回；若库内正有其它写者，最多等忙等上限加余量
        if not self._acquired.wait(timeout=40):
            raise HTTPException(status_code=503, detail="持锁线程未能在限定时间内取得写锁")
        if self._error:
            raise HTTPException(status_code=500, detail=f"演示持锁失败：{self._error}")
        return self.status()

    def release(self) -> dict:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=10)
        return self.status()

    def _hold(self, hold_ms: int, note: str | None) -> None:
        conn = get_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            with self._mu:
                self._info = {
                    "held_since": utcnow(),
                    "hold_ms": hold_ms,
                    "note": note or "",
                    "host": socket.gethostname(),
                }
            self._acquired.set()
            self._stop.wait(timeout=hold_ms / 1000.0)
            conn.execute("ROLLBACK")
        except Exception as exc:  # noqa: BLE001 - 演示设施，异常需可观测
            self._error = repr(exc)
            self._acquired.set()
        finally:
            try:
                conn.close()
            finally:
                with self._mu:
                    self._info = {}


demo_lock = DemoLock()
