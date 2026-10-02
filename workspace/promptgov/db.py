"""SQLite 存储层。

并发模型：
- 读连接：默认延迟事务，多线程共享只读快照。
- 写连接：统一使用 ``BEGIN IMMEDIATE`` 立即拿写锁，发布/审批/回滚/评估提交等
  关键写操作天然串行化，配合 families.active_version_id 上的 CAS 条件，
  保证"线上流量只能看到一个完整版本"。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS families (
    family_id        TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    description      TEXT NOT NULL DEFAULT '',
    active_version_id TEXT,                 -- 当前线上唯一指针，NULL=未发布
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS versions (
    version_id       TEXT PRIMARY KEY,
    family_id        TEXT NOT NULL REFERENCES families(family_id),
    semver           TEXT NOT NULL,         -- 1.4.0
    status           TEXT NOT NULL DEFAULT 'draft',
        -- draft / evaluating / evaluated / failed_eval / approved / active / superseded / abandoned
    system_instruction TEXT NOT NULL,
    variable_contract TEXT NOT NULL,        -- JSON: {name: {type, required}}
    tool_permissions TEXT NOT NULL,         -- JSON: [tool_id,...]
    model_params     TEXT NOT NULL,         -- JSON: {temperature, top_p, ...}
    content_hash     TEXT NOT NULL,        -- 四要素规范化哈希
    contract_change  TEXT NOT NULL DEFAULT 'none',
        -- none / compatible / breaking（相对上一版本）
    permission_expanded INTEGER NOT NULL DEFAULT 0, -- 相对当时线上版本
    pass_rate        REAL,
    created_by       TEXT NOT NULL DEFAULT 'cli',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    promoted_at      TEXT,
    UNIQUE(family_id, semver)
);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id      TEXT PRIMARY KEY,
    version_id       TEXT NOT NULL REFERENCES versions(version_id),
    kind             TEXT NOT NULL,         -- release / tool_expansion
    decision         TEXT NOT NULL,         -- approved / rejected
    approver         TEXT NOT NULL,
    reason           TEXT NOT NULL DEFAULT '',
    granted_tools    TEXT NOT NULL DEFAULT '[]', -- 审批时确认的权限快照
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 固定测试集：一个模板族一份不可变集合（可用新集合版本，演示中每族一份）
CREATE TABLE IF NOT EXISTS testsets (
    testset_id       TEXT PRIMARY KEY,
    family_id        TEXT NOT NULL REFERENCES families(family_id),
    name             TEXT NOT NULL,
    immutable        INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS testcases (
    case_id          TEXT PRIMARY KEY,
    testset_id       TEXT NOT NULL REFERENCES testsets(testset_id),
    seq              INTEGER NOT NULL,
    name             TEXT NOT NULL,
    inputs           TEXT NOT NULL,         -- JSON
    expect           TEXT NOT NULL          -- JSON: {assert: contains|equals|json_field,...}
);

-- 评估证据：一次评估运行一条头记录，逐用例明细在 eval_cases
CREATE TABLE IF NOT EXISTS eval_runs (
    run_id           TEXT PRIMARY KEY,
    version_id       TEXT NOT NULL REFERENCES versions(version_id),
    testset_id       TEXT NOT NULL REFERENCES testsets(testset_id),
    model_id         TEXT NOT NULL,         -- 证据绑定的模型标识
    status           TEXT NOT NULL DEFAULT 'running', -- running / passed / failed
    pass_rate        REAL,
    total            INTEGER NOT NULL DEFAULT 0,
    passed           INTEGER NOT NULL DEFAULT 0,
    late             INTEGER NOT NULL DEFAULT 0,  -- 提交时版本是否已离开 evaluated 候选态
    note             TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at      TEXT
);
CREATE TABLE IF NOT EXISTS eval_cases (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           TEXT NOT NULL REFERENCES eval_runs(run_id),
    case_id          TEXT NOT NULL,
    seq              INTEGER NOT NULL,
    name             TEXT NOT NULL,
    input_digest     TEXT NOT NULL,
    rendered_prompt  TEXT NOT NULL,
    output           TEXT NOT NULL,
    output_digest    TEXT NOT NULL,
    ok               INTEGER NOT NULL,
    detail           TEXT NOT NULL DEFAULT ''
);

-- 线上推理记录（绑定精确三元组 + 放量命中证据）
CREATE TABLE IF NOT EXISTS requests (
    request_id       TEXT PRIMARY KEY,
    family_id        TEXT NOT NULL,
    version_id       TEXT NOT NULL,           -- 实际命中的构件（可能是放量候选）
    model_id         TEXT NOT NULL,
    input_digest     TEXT NOT NULL,
    inputs_json      TEXT NOT NULL,
    rendered_prompt  TEXT NOT NULL,
    output_digest    TEXT NOT NULL,
    output           TEXT NOT NULL,
    template_hash_at_call TEXT NOT NULL,
    routing_key      TEXT,                    -- 调用方携带的路由键（无则未参与分流）
    rollout_id       TEXT,                    -- 命中时所属放量批次
    rollout_generation INTEGER,               -- 命中时的批次代次
    params_json      TEXT NOT NULL DEFAULT '',   -- 调用时推理配置快照
    params_fingerprint TEXT NOT NULL DEFAULT '', -- 推理配置指纹
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 分批放量批次：一个模板族同一时间最多一个进行中批次（ramping/frozen）
CREATE TABLE IF NOT EXISTS rollouts (
    rollout_id       TEXT PRIMARY KEY,
    family_id        TEXT NOT NULL REFERENCES families(family_id),
    candidate_version_id TEXT NOT NULL REFERENCES versions(version_id), -- 候选构件
    base_version_id  TEXT NOT NULL,          -- 建批时的线上构件（对照组）
    weight           INTEGER NOT NULL,       -- 候选分流权重 0~100（百分比）
    status           TEXT NOT NULL DEFAULT 'ramping',
        -- ramping / frozen / completed / cancelled
    generation       INTEGER NOT NULL DEFAULT 1,  -- 批次代次：每次状态/权重切换 +1
    created_by       TEXT NOT NULL DEFAULT 'api',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 自动止损配置：每个放量批次一条（upsert），作用于当前及之后代次的回执统计
CREATE TABLE IF NOT EXISTS rollout_guards (
    guard_id         TEXT PRIMARY KEY,
    rollout_id       TEXT NOT NULL UNIQUE REFERENCES rollouts(rollout_id),
    window_seconds   INTEGER NOT NULL,     -- 观察窗口（秒，滑动）
    min_samples      INTEGER NOT NULL,     -- 最小样本量（窗口内入账回执数）
    max_fail_ratio   REAL NOT NULL,        -- 失败占比上限 [0,1)，越过即触发
    enabled          INTEGER NOT NULL DEFAULT 1,
    created_by       TEXT NOT NULL DEFAULT 'api',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 生产效果回执：feedback_id 幂等（只入账一次）；counted=1 才计入止损统计，
-- 旧代次/无关请求/未命中候选的回执 counted=0，仅留审计记录，不触发状态变化
CREATE TABLE IF NOT EXISTS feedback_receipts (
    feedback_id      TEXT PRIMARY KEY,
    request_id       TEXT NOT NULL,          -- 不回填外键：无关请求也允许留痕
    family_id        TEXT,                   -- 请求已知时冗余其族，便于审计检索
    rollout_id       TEXT,                   -- 请求命中时固化的批次（可能已是旧代次）
    generation       INTEGER,                -- 请求命中时固化的批次代次
    outcome          TEXT NOT NULL,          -- success / failure
    counted          INTEGER NOT NULL DEFAULT 0,
    disposition      TEXT NOT NULL,          -- counted / unknown_request / not_candidate /
                                             -- stale_generation / rollout_inactive /
                                             -- no_guard / guard_disabled / request_already_counted
    note             TEXT NOT NULL DEFAULT '',
    actor            TEXT NOT NULL DEFAULT 'api',
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 止损触发快照：窗口统计 + 当时配置 + 触发回执，单事务内随冻结一并落库
CREATE TABLE IF NOT EXISTS guard_events (
    event_id         TEXT PRIMARY KEY,
    rollout_id       TEXT NOT NULL REFERENCES rollouts(rollout_id),
    generation       INTEGER NOT NULL,       -- 触发时（被冻结）的代次
    trigger_feedback_id TEXT NOT NULL,       -- 越界的那张回执
    window_seconds   INTEGER NOT NULL,
    min_samples      INTEGER NOT NULL,
    max_fail_ratio   REAL NOT NULL,
    samples          INTEGER NOT NULL,
    failures         INTEGER NOT NULL,
    fail_ratio       REAL NOT NULL,
    action           TEXT NOT NULL DEFAULT 'freeze',  -- 冻结试投，新流量导回基线
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 重演报告
CREATE TABLE IF NOT EXISTS replays (
    replay_id        TEXT PRIMARY KEY,
    request_id       TEXT NOT NULL REFERENCES requests(request_id),
    mode             TEXT NOT NULL,         -- exact / counterfactual
    changed_axis     TEXT NOT NULL DEFAULT 'none', -- input/model/template/none
    ov_version_id    TEXT,
    ov_model_id      TEXT,
    ov_input_digest  TEXT,
    verdict          TEXT NOT NULL,         -- reproduced / input_changed / model_changed / template_changed / output_drift
    same_template    INTEGER NOT NULL,
    same_model       INTEGER NOT NULL,
    same_input       INTEGER NOT NULL,
    same_output      INTEGER NOT NULL,
    replay_output_digest TEXT NOT NULL,
    replay_output    TEXT NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS audit_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    actor            TEXT NOT NULL DEFAULT 'system',
    action           TEXT NOT NULL,
    entity           TEXT NOT NULL,
    entity_id        TEXT NOT NULL,
    detail           TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_versions_family ON versions(family_id);
CREATE INDEX IF NOT EXISTS idx_eval_version ON eval_runs(version_id);
CREATE INDEX IF NOT EXISTS idx_requests_version ON requests(version_id);
CREATE INDEX IF NOT EXISTS idx_replays_request ON replays(request_id);
CREATE INDEX IF NOT EXISTS idx_rollouts_family ON rollouts(family_id);
CREATE INDEX IF NOT EXISTS idx_feedback_rollout ON feedback_receipts(rollout_id, generation, counted);
CREATE INDEX IF NOT EXISTS idx_feedback_request ON feedback_receipts(request_id);
CREATE INDEX IF NOT EXISTS idx_feedback_family ON feedback_receipts(family_id);
CREATE INDEX IF NOT EXISTS idx_guard_events_rollout ON guard_events(rollout_id);
"""

# 老库增量迁移：requests 的放量证据列（新库已由 _SCHEMA 建全）
_REQUESTS_NEW_COLUMNS = {
    "routing_key": "ALTER TABLE requests ADD COLUMN routing_key TEXT",
    "rollout_id": "ALTER TABLE requests ADD COLUMN rollout_id TEXT",
    "rollout_generation": "ALTER TABLE requests ADD COLUMN rollout_generation INTEGER",
    "params_json": "ALTER TABLE requests ADD COLUMN params_json TEXT NOT NULL DEFAULT ''",
    "params_fingerprint":
        "ALTER TABLE requests ADD COLUMN params_fingerprint TEXT NOT NULL DEFAULT ''",
}

_lock = threading.Lock()
_tls = threading.local()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    with _lock:
        conn = _connect()
        try:
            conn.executescript(_SCHEMA)
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(requests)")}
            for col, ddl in _REQUESTS_NEW_COLUMNS.items():
                if col not in cols:
                    conn.execute(ddl)
            conn.commit()
        finally:
            conn.close()


def get_conn() -> sqlite3.Connection:
    """每线程一个连接（HTTP 服务器为多线程）。"""
    conn = getattr(_tls, "conn", None)
    if conn is None:
        conn = _connect()
        _tls.conn = conn
    return conn


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """立即取写锁的串行写事务。"""
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def audit(actor: str, action: str, entity: str, entity_id: str, detail: str = "") -> None:
    get_conn().execute(
        "INSERT INTO audit_log(actor,action,entity,entity_id,detail) VALUES (?,?,?,?,?)",
        (actor, action, entity, entity_id, detail),
    )
    get_conn().commit()
