# PromptGov — 大模型提示模板版本治理与可重演服务

零第三方依赖（仅 Python 3.11 标准库：`http.server` + `sqlite3` + `hashlib`），
离线即可运行。内建确定性“假模型”，让评估与重演结果稳定可断言。

## 它解决什么

模板 = 系统指令 + 变量契约 + 工具权限 + 模型参数（+ 绑定模型标识）。

- 发布前必须在**固定测试集**上生成**评估证据**（逐用例渲染提示、输出、断言、摘要）。
- 线上请求固化**精确三元组**：模板版本、模型标识、输入摘要（外加模板内容哈希、输出摘要）。
- 重演按**原元数据**复核，能区分四种结论：
  `reproduced` / `input_changed` / `model_changed` / `template_changed`，
  三元组不变但输出不同则为 `output_drift`。
- 热更新 / 契约升级 / 评估迟到 / 并发回滚时，线上流量只看到**一个完整版本**；
  旧请求不受当前指针影响，永远可按原元数据复核。
- **工具权限扩大必须重新审批**（独立的 `tool_expansion` 审批，区别于 `release`）。
- **分批放量**：已审核构件可建立带权重的放量批次，调用方携带 `routing_key`，
  系统用稳定散列决定走当前构件还是候选构件；批次代次不变时同键路由不漂移。
  批次支持升档 / 冻结 / 恢复 / 完成 / 撤销，全部以 `expected_generation` 做 CAS。

## 运行

```bash
python3 -m promptgov
# 可选：PROMPTGOV_PORT=8080 PROMPTGOV_DATA=./data PROMPTGOV_PASS_THRESHOLD=1.0
# 打开 http://127.0.0.1:8080
```

首启自动建表并灌入演示数据：模板族 `support-router`，线上 v1，候选
v1.1.0（已建 20% 权重放量批次）、v1.2.0（扩大了 `refund.execute`，缺专项审批，
建放量批次与发布都会被门禁拦下）、v1.3.0（绑定弱模型 mini，评估 0.5 不达标）。

## 分批放量

- **批次**：`rollouts` 表记录候选构件、基线构件（建批时的线上版本）、权重
  （候选流量百分比 0~100）、状态（`ramping/frozen/completed/cancelled`）与
  **代次 generation**（每次状态/权重切换 +1）。一个族同一时间最多一个进行中批次。
- **路由**：`bucket = sha256(rollout_id | routing_key)[:8] % 100`，
  `bucket < weight` 走候选，否则走当前构件。桶号不含权重与代次，因此
  **代次不变时同键决定绝不漂移**；升档（权重单调增大）时已走候选的 key 不会掉回。
- **冻结**：期间所有新调用（含本应命中候选的 key）一律走当前构件；恢复后权重不变，
  同 key 决定与冻结前一致。**撤销**：线上指针从未移动，流量天然回归。
  **完成**：单事务内翻转线上指针 + 终结批次（要求指针仍等于批次基线）。
- **证据固化**：调用记录保存实际命中构件、推理配置快照、参数指纹、批次代次与
  routing_key；之后复查该调用（`/requests/{id}/replay`）不受权重/批次变化影响。
- **门禁**：仅"已审核构件"（passed 证据 + approved release 审批）可建批；
  候选新增可调用工具时，建批即要求 `tool_expansion` 专项授权就位，
  命中候选的请求路径上还会当场复核——首个请求命中前授权必须存在。
- **并发**：所有状态切换以 `expected_generation` 做原子 CAS
  （`UPDATE ... WHERE generation=? AND status IN (...)`，rowcount≠1 即 409），
  任何失败或冲突都回滚，不改变已生效路由表；原有一步切换接口
  （promote/rollback）继续工作。

## 关键不变量与实现

| 需求 | 实现 |
|---|---|
| 版本不可变 | 四要素规范化后 SHA-256 存 `content_hash`；重复哈希拒绝创建；版本行只做受控状态迁移 |
| 模型即版本一部分 | `model_id` 写入 `model_params._bound_model` 并进哈希；证据模型须等于绑定模型 |
| 固定测试集 + 证据 | `testsets/testcases` 不可变；`eval_runs/eval_cases` 记录每用例渲染提示/输出/断言/摘要 |
| 评估迟到 | 证据永远落库；提交时版本已离开候选态则打 `late=1`；不改变版本历史 |
| 原子热更新/回滚 | `BEGIN IMMEDIATE` 单事务翻转 `families.active_version_id` 唯一指针 + 状态迁移 |
| 并发只一胜 | 发布/回滚带 `expected_active` 做 CAS，指针已变即 409（压测：30 竞争，13 成 17 冲突，0 半状态）|
| 旧请求可复核 | `requests` 行固化版本/模型/输入摘要/模板哈希/输出摘要；重演读版本快照而非当前指针 |
| 放量同键稳定 | 路由桶号只依赖 `(rollout_id, routing_key)` 的稳定散列，不含权重/代次 |
| 放量并发只一胜 | 批次切换带 `expected_generation` 做代次 CAS，冲突 409 且路由表不变 |
| 调用可复查 | `requests` 固化命中构件/推理配置/参数指纹/批次代次；复查不读当前批次状态 |
| 权限扩大重新审批 | 相对线上基线新增工具即要求覆盖该工具集的 `tool_expansion` approved；回滚只收窄故免新审批 |
| 契约升级分级 | 删变量/加必填/改类型=breaking；仅加可选变量=compatible；网关按声明字段投影输入 |
| 审计 | 所有生命周期动作写 `audit_log`（总览页可见） |

发布门禁（单事务内全部满足才翻指针）：
1. 存在 `passed` 评估证据，且证据模型 = 版本绑定模型、通过率 ≥ 门槛（默认 1.0）；
2. 存在 approved 的 `release` 审批；
3. 工具权限若扩大，存在覆盖全部工具的 approved `tool_expansion` 审批；
4. CAS：当前线上指针 == 调用方提供的 `expected_active`。

## 页面

- `/` 总览：模板族/线上版本（含放量中标记）、模型注册表、审计流。
- `/families/{id}` 版本治理：建版本（四要素）、热发布/回滚、线上调用（可带 routing_key）、最近请求。
- `/families/{id}/rollout` **放量操作页**：建批次、升档/冻结/恢复/完成/撤销、批次历史、最近命中。
- `/versions/{id}/eval` **评估页面**：证据列表（含迟到标记）、逐用例渲染提示与输出、审批。
- `/requests/{id}/replay` **调用复查页**：原三元组与放量命中证据（命中构件/批次代次/参数指纹/
  推理配置快照）、精确重演、单轴反事实、漂移注入、历史重演记录与结论。

## JSON API（摘要）

```
GET  /api/models
GET  /api/families            POST /api/families
GET  /api/families/{fid}
GET  /api/families/{fid}/versions
POST /api/families/{fid}/versions            # 四要素 + model_id
POST /api/families/{fid}/invoke              # {inputs, routing_key?} 稳定散列分流
POST /api/families/{fid}/promote             # {version_id, expected_active}
POST /api/families/{fid}/rollback            # {target_version_id, expected_active}
POST /api/families/{fid}/rollouts            # {candidate_version_id, weight} 建批次
GET  /api/families/{fid}/rollouts
GET  /api/rollouts/{rid}
POST /api/rollouts/{rid}/ramp                # {weight, expected_generation} 升档
POST /api/rollouts/{rid}/freeze              # {expected_generation} 冻结
POST /api/rollouts/{rid}/resume              # {expected_generation} 恢复
POST /api/rollouts/{rid}/complete            # {expected_generation} 完成（候选转正）
POST /api/rollouts/{rid}/cancel              # {expected_generation} 撤销（流量回归）
POST /api/versions/{vid}/eval/start
POST /api/eval-runs/{rid}/run
GET  /api/versions/{vid}/eval-runs
POST /api/versions/{vid}/approve             # {kind: release|tool_expansion, decision, ...}
GET  /api/eval-runs/{rid}
GET  /api/requests[?family=]
GET  /api/requests/{rid}
POST /api/requests/{rid}/replay              # override_version_id/override_model_id/override_inputs/inject_drift
GET  /api/requests/{rid}/replays
GET  /api/audit
```

## 代码地图

- `db.py`：SQLite schema（含 `rollouts` 批次表与 `requests` 放量证据列）、WAL、
  `write_tx()` 立即写锁事务、审计、老库增量迁移。
- `core.py`：内容/输入/输出哈希、参数指纹、放量稳定散列分桶、变量契约校验与升级分级、
  权限集合比较、受限渲染入口。
- `jinja_compat.py`：零依赖沙箱模板（`{{ }}`、`{% if/for %}`、过滤器、比较/三元）。
- `llm.py`：模型注册表 + 确定性假模型（输出由 model/template/input 三轴决定）+ 断言器。
- `services.py`：版本生命周期、评估证据、审批、CAS 发布/回滚、分批放量
  （建批门禁、代次 CAS 状态机、稳定散列路由）、调用、重演归因。
- `app.py`：HTTP 路由（JSON API + 服务端渲染页面）。
- `seed.py`：幂等演示数据。
- `tests/test_rollout.py`：放量机制端到端验证（同键稳定/按比例分流/并发升档/
  冻结/撤销/复查复现/专项授权/HTTP 冒烟），`python3 -m unittest discover -s tests`。
