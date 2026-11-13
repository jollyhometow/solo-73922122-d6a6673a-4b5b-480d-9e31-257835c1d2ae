"""导入专用执行器：与服务同步接口的共享线程池完全隔离的有界线程池。

为什么需要它
------------
两个导入入口（CSV 上传 / JSON 批量）的入仓工作可能因写锁拥塞等待最长 30 秒
（SQLite 忙等上限保持不变）。若让导入占用 anyio 共享线程池，40 份并发导入就会
耗尽全部名额，/api/sales、/api/health 等同步接口将无线程可用。这里改用
独立的小线程池（默认 2 个导入名额）+ 有界排队：

- 导入端点是 async 协程，await 执行器结果时不占共享线程池、不阻塞事件循环；
- 共享线程池名额一个不扩，读接口永远拿得到线程；
- 名额（pending 计数）只在任务真正结束时归还——客户端断连只能取消
  「尚未开始」的排队任务；「已开始」的任务不可取消，线程内继续跑到整批
  提交或回滚、连接关闭之后，名额才随 done 回调释放。
"""
from __future__ import annotations

import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable


class ImportExecutor:
    """有界导入执行器：workers 个导入名额 + max_pending 个在途任务上限。"""

    def __init__(self, workers: int, max_pending: int) -> None:
        self._workers = workers
        self._max_pending = max_pending
        self._pool = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="sales-import")
        self._pending = 0
        self._pending_lock = threading.Lock()

    @property
    def workers(self) -> int:
        return self._workers

    @property
    def max_pending(self) -> int:
        return self._max_pending

    @property
    def pending(self) -> int:
        """在途任务数（运行中 + 排队中），用于观测与测试。"""
        with self._pending_lock:
            return self._pending

    def submit(self, fn: Callable[..., Any], *args: Any) -> Future | None:
        """提交一批入仓工作；在途任务达到上限时返回 None（调用方转 503）。"""
        with self._pending_lock:
            if self._pending >= self._max_pending:
                return None
            self._pending += 1
        try:
            fut = self._pool.submit(fn, *args)
        except Exception:
            with self._pending_lock:
                self._pending -= 1
            raise
        # 任务结束（完成 / 失败 / 未开始即被取消）才归还名额。
        # 运行中的任务无法被取消：线程继续执行，名额随本回调延迟到收尾才释放。
        fut.add_done_callback(self._release)
        return fut

    def _release(self, _fut: Future) -> None:
        with self._pending_lock:
            self._pending -= 1


# 每进程默认 2 个导入工作者：SQLite 同一时刻只有一个写者，更多工作者
# 只是在忙等上限内排队等锁，对吞吐无益；2 个可让「等锁」与「校验/计算」重叠。
import_executor = ImportExecutor(
    workers=int(os.environ.get("SALES_IMPORT_WORKERS", "2")),
    max_pending=int(os.environ.get("SALES_IMPORT_MAX_PENDING", "128")),
)
