"""演示用样例数据：跨生效日销售、空档未归属、无时间线客户。"""
from __future__ import annotations

SAMPLE_CSV = """sale_id,customer_id,txn_date,amount_cents
S1001,C001,2026-01-10,12000
S1002,C001,2026-03-31,8800
S1003,C001,2026-04-01,15000
S1004,C001,2026-06-15,23050
S1005,C001,2026-08-20,9900
S1006,C002,2026-02-14,5000
S1007,C002,2026-05-01,7200
S1008,C002,2026-05-02,7300
S1009,C003,2026-07-01,4200
S1010,C003,2026-09-30,6800
"""

# C001：正常两段，样例销售跨 2026-04-01 生效日（华东 -> 华南）
SEED_C001 = [
    {"region": "华东", "effective_from": "2026-01-01", "effective_to": "2026-04-01"},
    {"region": "华南", "effective_from": "2026-04-01", "effective_to": None},
]

# C002：时间线中间存在空档 2026-05-01 ~ 2026-06-01，落在空档的销售标未归属
SEED_C002 = [
    {"region": "华北", "effective_from": "2026-01-01", "effective_to": "2026-05-01"},
    {"region": "西南", "effective_from": "2026-06-01", "effective_to": None},
]

# C003：故意不建时间线，销售未归属原因 = no_timeline
SEED_TIMELINES = {"C001": SEED_C001, "C002": SEED_C002}


# 追溯更正样例：把 C001 的地区生效日提前到 2026-03-01（左闭右开），
# 2026-03-31 的 S1002 回填后应从「华东」变为「华南」。
CORRECTION_SAMPLE_C001 = [
    {"region": "华东", "effective_from": "2026-01-01", "effective_to": "2026-03-01"},
    {"region": "华南", "effective_from": "2026-03-01", "effective_to": None},
]
