# 销售地区归属 · 追溯更正平台

按**交易发生日**确定客户地区的销售数仓演示系统。地区归属在入仓/回填切换时**入库存储**，
查询不做任何临时重算；支持按客户整套替换地区有效期、乐观修订并发控制、无长写锁的后台回填，
以及两个 Web 进程并发下的原子切换。

技术栈：FastAPI + SQLite（WAL）+ 原生 HTML/JS 前端 + Nginx 负载均衡 + Docker Compose。

## 一键启动

```bash
docker compose up --build
```

浏览器入口：

| 入口 | 说明 |
| --- | --- |
| http://localhost:8080 | Nginx 统一入口，请求在两个 Web 进程间轮询 |
| http://localhost:8081 | 直连 Web 进程 1（容器 sales-web1） |
| http://localhost:8082 | 直连 Web 进程 2（容器 sales-web2） |

SQLite 数据文件位于命名卷 `sales_data`（容器内 `/data/sales.db`），**容器重启/重建后数据保留**。

打开页面后在「④ 演示脚本与自检」页点**一键准备演示数据**，或：

```bash
curl -X POST http://localhost:8080/api/demo/seed
```

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 上传 ERP 销售 CSV（唯一销售ID、客户ID、交易日期、整数分金额） | `POST /api/sales/import`（multipart）/ `POST /api/sales/import-json`（程序化） |
| 按交易日确定地区，归属入库存储，查询不重算 | `sales.region / region_version / unattributed_reason` 落库，查询接口只读取 |
| 按客户替换整套有效期，左闭右开，末段可无结束日 | `validate_entries`：`[from, to)`，末段 `to=NULL`，背靠背合法、重叠/反向 400 拒绝 |
| 空档标为未归属 | 原因码 `gap`；客户从未建时间线为 `no_timeline` |
| 更正携带所见修订，过期拒绝 | 请求必须带 `expected_version`，与当前版本不符返回 `409 stale_version` |
| 同客户并发更正只允许一份成功 | `correction_mutex` 客户级互斥表 + `BEGIN IMMEDIATE`，跨进程生效，败者 `409 concurrent_correction` |
| 后台回填期间仍可导入、查旧结果，回填不持写锁 | 守护线程做只读计算，sleep 延迟期间零锁；仅批量写 staging 与最终切换拿短锁 |
| 切换纳入期间新增销售 | 切换事务内对 staging 缺失的销售再次查库补算，然后整体重写该客户全部销售 |
| 时间线、修订、全部归属一起生效 | 单个 `BEGIN IMMEDIATE` 事务：旧 timeline 置 dead → 新置 active → 销售重写 → 写 revision，原子提交 |
| 失败保留旧版 | 模拟失败/异常时任务标 failed，删除 backfill 行与 staging，释放互斥，active 旧版原样可用 |
| 每次查询返回同版时间线和销售归属 | 原子提交 + WAL 快照读；`/api/sales/{id}` 附带同版时间线快照，`/api/consistency` 可自检 |
| 两个 Web 进程并发不留下旧归属 | 两容器共享同一 WAL 库；切换是单事务，busy_timeout 30s 串行化写者 |
| 重启保留 | 命名卷持久化；启动时把残留 pending/running 任务标记 failed 并清理（原子提交保证不会出现半切换） |
| 可调回填延迟 | 每次更正请求带 `backfill_delay_ms`（0~300000），延迟均匀分摊到每条销售的计算间隔 |
| 跨生效日样例 | C001 样例销售跨 2026-04-01；追溯模板把生效日提前到 2026-03-01，S1002 回填后改挂华南 |
| 写锁拥塞时读接口不卡 | 导入走独立有界执行器（每进程 2 名额 + 128 排队），不占 anyio 共享线程池；40 份并发导入下 `/api/sales`、`/api/health` 仍 <1s 返回已提交状态 |
| 等锁超时可重试 | SQLite 原 30 秒忙等待保持不变；超时整批回滚不入仓，返回 `503 warehouse_lock_timeout` + `Retry-After` |
| 断连不丢批次 | 客户端断连只取消未开始的排队任务；已开始的批次在执行器线程内跑到整批提交/回滚，连接关闭、名额收尾后才释放 |
| 重传去重 / 归属版本 | 重传按销售ID跳过；归属版本在 `BEGIN IMMEDIATE` 事务内读取，与入仓提交时生效的地区版本一致 |

## 浏览器页面功能

1. **销售导入 / 明细**：上传 CSV、下载样例、按客户/未归属筛选、查看销售详情（含同版时间线快照与未归属原因）。
2. **客户地区有效期**：载入当前时间线（显示所见版本）、可视化增删区间、可调回填延迟、可勾选模拟失败、查看逐版修订 diff。
3. **更正与回填任务**：任务状态（pending/running/succeeded/failed）、暂存进度、每 2 秒自动刷新。
4. **演示脚本与自检**：一键 seed、一致性自检、写锁拥塞演示（可控持锁 / 并发导入 40 批 + 读延迟探测 / 断连演示）、推荐演示流程。

## 入仓写锁拥塞设计

- **两个导入入口**（`POST /api/sales/import`、`POST /api/sales/import-json`）均为 async 端点，
  整批入仓工作提交到**导入专用有界执行器**（`app/import_exec.py`，每进程默认 2 个工作线程 +
  128 在途上限），协程 await 结果——不占用 anyio 共享线程池（共享池名额一个不扩），
  也不阻塞事件循环，因此 40 份并发导入等锁期间 `/api/sales`、`/api/health` 仍在 1 秒内返回。
- **SQLite 忙等保持 30 秒**（`PRAGMA busy_timeout=30000`）。等锁超时（`SQLITE_BUSY`）整批回滚、
  该批不入仓，返回可重试 `503`（`Retry-After: 1`，`detail.retryable=true`）；在途队列满同样返回可重试 503。
- **断连语义**：取消只会命中「未开始」的排队任务（名额立即归还）；「已开始」的任务不可取消，
  在执行器线程内继续到整批提交或回滚，`finally` 关闭连接后名额才随 done 回调释放——
  不会提前释放仍在使用的连接或执行名额。
- **可控持锁演示**：`POST /api/demo/lock {"hold_ms": 35000}` 由独立线程持有数据库写锁
  （nginx 已把该控制面固定到 web1；锁是数据库级的，对两个进程都生效），
  `DELETE /api/demo/lock` 提前释放，`GET /api/demo/lock` 查状态，
  `GET /api/demo/import-executor` 观察导入名额占用。

## 命令行演示

```bash
# 仅依赖标准库；先 seed 再自动演示：并发互斥 → 回填中导入/查询 → 过期版本拒绝
python3 demo/concurrency_demo.py http://localhost:8080

# 写锁拥塞演示：持锁 35s → 读接口 <1s → 等锁超时 503 → 每进程 40 并发导入 → 断连仍整批提交 → 重传去重
python3 demo/import_lock_demo.py            # 默认打 :8080/:8081/:8082，约 60 秒
```

## 主要 API

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/sales/import` | CSV 上传（表头 `sale_id,customer_id,txn_date,amount_cents`） |
| `POST /api/sales/import-json` | JSON 批量导入 `{"rows":[...]}` |
| `GET /api/sales` / `GET /api/sales/{sale_id}` | 销售列表 / 详情（含同版时间线快照） |
| `GET /api/timeline/{customer_id}` | 当前生效整套时间线 + 所见版本号 |
| `POST /api/timeline/{customer_id}` | 替换有效期（首次建线 expected_version=0） |
| `POST /api/corrections` | 追溯更正（携带 expected_version、delay、fail_simulation） |
| `GET /api/corrections[/{id}]` | 任务列表/详情（含 staging 预览） |
| `GET /api/revisions/{customer_id}` | 修订历史与逐条 diff |
| `GET /api/consistency` | 销售归属与版本时间线一致性自检 |
| `POST /api/demo/seed` | 幂等灌入演示数据 |
| `POST /api/demo/lock` | 演示用：持有写锁 `hold_ms` 毫秒（100~180000，nginx 固定路由到 web1） |
| `DELETE /api/demo/lock` / `GET /api/demo/lock` | 提前释放演示锁 / 查询持锁状态 |
| `GET /api/demo/import-executor` | 导入专用执行器状态（名额、在途任务数） |

## 数据模型要点

- `region_timeline`：多版本时间线行，`status ∈ active/backfill/dead`，左闭右开区间。
- `correction_mutex`：`customer_id` 主键，进行中的更正一行锁，切换/失败即释放。
- `correction_job`：更正任务与回填状态（进度、延迟、错误、关联修订）。
- `region_staging`：回填计算结果，仅在任务存活期间存在，切换后清理。
- `region_revisions`：每次成功切换一条，含新版本 payload 与新旧逐条 diff。
- `sales`：归属三要素 `region_version / region / unattributed_reason` 持久化。

## 本地测试

```bash
pip install -r requirements.txt
pytest -q
```

覆盖：左闭右开边界、重叠拒绝、空档/no_timeline 未归属、追溯回填、乐观版本过期、
同客户并发互斥、回填期间新增销售纳入切换、失败保留旧版、重启持久化与中断恢复、坏 CSV 整批拒绝，
以及写锁拥塞：等锁超时 503 且整批不入仓、40 并发导入下读接口 <1s、断连后运行中批次整批提交、
排队任务取消释放名额、队列满 503、可控持锁端点。
