#!/usr/bin/env python3
"""写锁拥塞演示（仅标准库）：可控持锁 → 读接口不卡 → 等锁超时 503 → 断连仍整批提交 → 重传去重。

前提：docker compose up --build 已启动（或本地两个 uvicorn 进程 + 同库）。
用法：
  python3 demo/import_lock_demo.py [base] [web1] [web2]
  默认 base=http://localhost:8080（nginx，/api/demo/lock 已固定到 web1）
       web1=http://localhost:8081  web2=http://localhost:8082
"""
import http.client
import json
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
WEB1 = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8081"
WEB2 = sys.argv[3] if len(sys.argv) > 3 else "http://localhost:8082"


def call(base: str, method: str, path: str, body=None, timeout: float = 90) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data,
        headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def import_rows(base: str, rows: list[tuple]) -> tuple[int, dict]:
    return call(base, "POST", "/api/sales/import-json", {"rows": [
        {"sale_id": r[0], "customer_id": r[1], "txn_date": r[2], "amount_cents": r[3]}
        for r in rows]})


def wait_lock_released(timeout: float = 90) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, st = call(BASE, "GET", "/api/demo/lock")
        if not st.get("held"):
            return
        time.sleep(0.3)
    raise AssertionError("演示锁未在限定时间内释放")


class ReadProbe:
    """后台线程持续探测 /api/health 与 /api/sales 延迟（要求 <1s）。"""

    PATHS = ("/api/health", "/api/sales?limit=1")

    def __init__(self, bases: list[str]) -> None:
        self._bases = bases
        self._stop = threading.Event()
        self.max_ms = {p: 0.0 for p in self.PATHS}
        self.samples = 0

    def __enter__(self) -> "ReadProbe":
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._t.join()

    def _run(self) -> None:
        while not self._stop.is_set():
            for base in self._bases:
                for path in self.PATHS:
                    t0 = time.monotonic()
                    try:
                        call(base, "GET", path, timeout=5)
                        ms = (time.monotonic() - t0) * 1000
                        self.max_ms[path] = max(self.max_ms[path], ms)
                        self.samples += 1
                    except Exception:
                        pass
            time.sleep(0.15)

    def assert_fast(self) -> None:
        for path, ms in self.max_ms.items():
            assert ms < 1000, f"{path} 最大延迟 {ms:.0f}ms 超过 1s 要求"
        print(f"  读接口探测 {self.samples} 次：/api/health 最大 "
              f"{self.max_ms['/api/health']:.0f}ms，/api/sales 最大 "
              f"{self.max_ms['/api/sales?limit=1']:.0f}ms（均 <1s ✓）")


def scenario_a_lock_timeout_503() -> None:
    print("\n== 场景 A：持锁 35s，导入等锁超过 30s 忙等上限 → 可重试 503，整批不入仓 ==")
    code, st = call(BASE, "POST", "/api/demo/lock",
                    {"hold_ms": 35000, "note": "场景A：锁超时503"})
    assert code == 200 and st["held"], st
    print(f"  写锁已持有 35s（进程 {st['host']}）")
    sale_id = f"LA{int(time.time())}"
    out: dict = {}

    def do_import() -> None:
        out["res"] = import_rows(WEB1, [(sale_id, "C001", "2026-02-10", 100)])

    with ReadProbe([WEB1, WEB2]) as probe:
        t0 = time.monotonic()
        t = threading.Thread(target=do_import)
        t.start()
        time.sleep(3)          # 导入等锁期间，确认读接口流畅
        t.join()
        elapsed = time.monotonic() - t0
    code, body = out["res"]
    print(f"  导入响应：HTTP {code}（{elapsed:.1f}s）{body.get('detail', body)}")
    assert code == 503, "等锁超时必须返回 503"
    assert body["detail"]["retryable"] is True
    probe.assert_fast()
    code, _ = call(WEB1, "GET", f"/api/sales/{sale_id}")
    assert code == 404, "等锁超时的批次不得入仓"
    print("  该批未入仓 ✓，等待锁自动释放后重传…")
    wait_lock_released()
    code, body = import_rows(WEB1, [(sale_id, "C001", "2026-02-10", 100)])
    assert code == 200 and body["inserted"] == 1, body
    code, body = import_rows(WEB1, [(sale_id, "C001", "2026-02-10", 100)])
    assert code == 200 and body["skipped_duplicate"] == 1, body
    print("  锁释放后重传入仓成功，再传按销售ID去重 ✓")


def scenario_b_concurrent_imports() -> None:
    print("\n== 场景 B：每进程 40 份并发导入 + 持锁 8s，读接口 <1s，锁释放后全部入仓 ==")
    code, st = call(BASE, "POST", "/api/demo/lock",
                    {"hold_ms": 8000, "note": "场景B：40并发/进程"})
    assert code == 200 and st["held"], st
    ts = int(time.time())
    barrier = threading.Barrier(80)

    def one(base: str, idx: int) -> tuple[int, dict]:
        barrier.wait()
        return import_rows(base, [(f"CB{ts}-{idx}", "C001", "2026-02-10", 100 + idx)])

    with ReadProbe([WEB1, WEB2]) as probe, ThreadPoolExecutor(80) as pool:
        futs = [pool.submit(one, WEB1 if i < 40 else WEB2, i) for i in range(80)]
        outs = [f.result() for f in futs]
    codes = sorted(c for c, _ in outs)
    assert codes == [200] * 80, f"锁 8s 在 30s 忙等内，80 份应全部成功：{codes}"
    probe.assert_fast()
    code, body = call(WEB1, "GET", "/api/sales?customer_id=C001&limit=5000")
    present = {it["sale_id"] for it in body["items"]}
    missing = [i for i in range(80) if f"CB{ts}-{i}" not in present]
    assert not missing, f"有批次未入仓：{missing}"
    hosts = {b.get("served_by") for c, b in outs if c == 200}
    print(f"  两个进程各 40 份全部入仓 ✓（响应进程：{sorted(hosts)}）")


def scenario_c_disconnect_completes() -> None:
    print("\n== 场景 C：客户端断连后，已开始的批次仍整批提交 ==")
    code, st = call(BASE, "POST", "/api/demo/lock",
                    {"hold_ms": 8000, "note": "场景C：断连"})
    assert code == 200 and st["held"], st
    sale_id = f"DC{int(time.time())}"
    body = json.dumps({"rows": [{"sale_id": sale_id, "customer_id": "DC001",
                                 "txn_date": "2026-02-10", "amount_cents": 777}]}).encode()
    u = urllib.parse.urlparse(WEB1)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
    conn.request("POST", "/api/sales/import-json", body=body,
                 headers={"Content-Type": "application/json"})
    time.sleep(1.5)   # 服务端已开始等锁（导入执行器线程内）
    conn.close()      # 客户端直接断连，不读响应
    print(f"  客户端已断连（批次 {sale_id}，未读响应）")
    wait_lock_released()
    deadline = time.time() + 30
    while True:
        code, _ = call(WEB1, "GET", f"/api/sales/{sale_id}")
        if code == 200:
            break
        assert time.time() < deadline, "断连批次未在锁释放后整批入仓"
        time.sleep(0.5)
    _, ex = call(WEB1, "GET", "/api/demo/import-executor")
    assert ex["pending"] == 0, f"执行名额未归还：{ex}"
    print(f"  批次 {sale_id} 已整批提交入仓 ✓，导入名额已如数归还（pending=0）✓")


def main() -> None:
    print(f"== 目标 nginx={BASE} web1={WEB1} web2={WEB2}，先准备演示数据 ==")
    call(BASE, "POST", "/api/demo/seed")
    call(BASE, "DELETE", "/api/demo/lock")   # 清掉上次可能残留的演示锁
    scenario_a_lock_timeout_503()
    scenario_b_concurrent_imports()
    scenario_c_disconnect_completes()
    _, cons = call(BASE, "GET", "/api/consistency")
    print(f"\n一致性自检：consistent={cons['consistent']}，核对 {cons['checked']} 条")
    assert cons["consistent"]
    print("\n全部写锁拥塞演示通过 ✅")


if __name__ == "__main__":
    main()
