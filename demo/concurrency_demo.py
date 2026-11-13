#!/usr/bin/env python3
"""可演示脚本（仅标准库）：并发更正互斥 + 回填期间持续导入/查询。

用法：
  python3 demo/concurrency_demo.py http://localhost:8080
先在浏览器里点「一键准备演示数据」，或让本脚本自动 seed。
"""
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"


def call(method: str, path: str, body=None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def post_csv(rows: list[tuple]) -> tuple[int, dict]:
    body = {"rows": [
        {"sale_id": r[0], "customer_id": r[1], "txn_date": r[2], "amount_cents": r[3]}
        for r in rows]}
    return call("POST", "/api/sales/import-json", body)


def wait_job(job_id: int) -> dict:
    while True:
        _, d = call("GET", f"/api/corrections/{job_id}")
        if d["status"] in ("succeeded", "failed"):
            return d
        time.sleep(0.2)


def main() -> None:
    print(f"== 目标 {BASE}，先准备演示数据 ==")
    call("POST", "/api/demo/seed")

    print("\n== 场景 1：同一客户并发两份更正，只允许一份成功 ==")
    _, tl = call("GET", "/api/timeline/C001")
    expected = tl["version"]
    print(f"  当前 C001 生效版本 v{expected}，两份请求都携带该所见修订")
    payloads = []
    for i in range(2):
        payloads.append({
            "customer_id": "C001", "expected_version": expected,
            "entries": [
                {"region": f"并行地区{i}", "effective_from": "2026-01-01",
                 "effective_to": "2026-03-01"},
                {"region": "华南", "effective_from": "2026-03-01",
                 "effective_to": None}],
            "backfill_delay_ms": 4000,
        })
    with ThreadPoolExecutor(max_workers=2) as pool:
        outs = list(pool.map(lambda p: call("POST", "/api/corrections", p), payloads))
    for code, body in outs:
        print(f"  HTTP {code} {body.get('error') or body.get('job_id')} {body if code != 200 else ''}")
    assert sorted(c for c, _ in outs) == [200, 409], "并发互斥不符合预期"
    job_id = next(b["job_id"] for c, b in outs if c == 200)

    print("\n== 场景 2：回填进行中持续导入销售与查询旧结果 ==")
    stop = False

    def importer() -> None:
        n = 0
        while not stop:
            n += 1
            code, body = post_csv([(f"BG{n:04d}", "C001", "2026-02-15", 9900 + n)])
            print(f"  回填中导入 BG{n:04d}: HTTP {code} {body}")
            time.sleep(0.8)

    def reader() -> None:
        while not stop:
            _, body = call("GET", "/api/sales?customer_id=C001&limit=3")
            print(f"  查询旧结果：{body['total']} 条，响应进程 {body['served_by']}")
            time.sleep(0.8)

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(importer)
        f2 = pool.submit(reader)
        job = wait_job(job_id)
        stop = True
        f1.result(); f2.result()

    print(f"\n回填任务最终状态：{job['status']}，纳入销售 {job['staged_sales']}/{job['total_sales']}")
    _, cons = call("GET", "/api/consistency")
    print(f"一致性自检：consistent={cons['consistent']}，核对 {cons['checked']} 条，"
          f"未归属 {cons['unattributed_count']} 条，不一致 {len(cons['mismatches'])} 条")
    assert job["status"] == "succeeded" and cons["consistent"]

    print("\n== 场景 3：过期版本提交被拒绝 ==")
    _, tl = call("GET", "/api/timeline/C001")
    stale = max(0, tl["version"] - 1)
    code, body = call("POST", "/api/corrections", {
        "customer_id": "C001", "expected_version": stale,
        "entries": [{"region": "过期", "effective_from": "2026-01-01",
                     "effective_to": None}],
        "backfill_delay_ms": 0})
    print(f"  HTTP {code}（携带过期版本 v{stale}，当前 v{tl['version']}）"
          f" {body['detail'] if code != 200 else body}")
    assert code == 409
    print("\n全部演示通过 ✅")


if __name__ == "__main__":
    main()
