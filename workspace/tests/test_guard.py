"""自动止损（生产效果回执 → 单事务冻结）端到端验证（标准库 unittest，离线可跑）。

覆盖：
- 重复回执：同一 feedback_id 只入账一次，重复提交返回原回执且不改变统计
- 无关请求：未知请求 / 基线流量请求只留审计记录，不计数不触发
- 阈值临界：样本未达门槛不触发；失败占比等于上限不触发，越过才触发
- 触发快照：单事务冻结试投 + 新流量导回基线 + 快照固化窗口统计与当时配置
- 并发触发：多线程越界回执只让状态变化一次（写事务串行化 + 代次 CAS）
- 迟到回执：触发/恢复后旧代次请求的回执仅审计（stale_generation）
- 人工恢复：开启新代次并重置统计窗口，旧窗口回执与历史请求仍可复核
- 观察窗口：窗口外的旧回执不计入统计
- 一请求一样本：同一请求的第二张回执不重复计数
- 未配置/停用止损：回执仅审计；配置校验；JSON API 与页面冒烟
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.request
import uuid

_TMP = tempfile.mkdtemp(prefix="promptgov-guard-test-")
os.environ["PROMPTGOV_DATA"] = _TMP
os.environ["PROMPTGOV_SEED"] = "0"
os.environ["PROMPTGOV_PORT"] = "0"
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from promptgov import core, db, services  # noqa: E402

db.init_db()

CONTRACT = {
    "customer_text": {"type": "string", "required": True},
    "lang": {"type": "string", "required": False},
}
PARAMS = {"temperature": 0.2, "top_p": 0.9, "max_tokens": 1024}
INSTRUCTION_V1 = "你是客服分流助手。客户原话：{{ customer_text }} 请给出 label 与 score。"
INSTRUCTION_V2 = "你是客服分流助手（v2）。客户原话：{{ customer_text }} 优先识别 positive。"
INPUTS = {"customer_text": "客服非常耐心，问题一次解决", "lang": "zh"}
TEST_CASES = [
    {"name": "满意", "inputs": {"customer_text": "非常满意，点赞", "lang": "zh"},
     "expect": {"assert": "json_field", "field": "label", "equals": "positive"}},
]
GUARD = {"window_seconds": 3600, "min_samples": 2, "max_fail_ratio": 0.5}


def _uid() -> str:
    return uuid.uuid4().hex[:8]


def fbid(prefix: str = "fb") -> str:
    """每个用例使用全局唯一的 feedback_id（共享库内避免幂等键碰撞）。"""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def make_family_with_candidate():
    """建族：v1 上线，v2 评估+审批为已审核候选。返回 (fid, v1_id, v2_id)。"""
    fid = f"f-{_uid()}"
    services.create_family(fid, f"族{fid}", "", "test")
    services.create_testset(fid, "固定测试集", TEST_CASES, "test")
    v1 = services.create_version(fid, "1.0.0", INSTRUCTION_V1, CONTRACT,
                                 ["ticket.read"], PARAMS, "langfang-3-64k", "test")
    services.run_evaluation(services.start_evaluation(v1["version_id"], "test"), "test")
    services.approve(v1["version_id"], "release", "ops", "approved", "首发")
    services.promote(fid, v1["version_id"], "test")
    v2 = services.create_version(fid, "1.1.0", INSTRUCTION_V2, CONTRACT,
                                 ["ticket.read"], PARAMS, "langfang-3-64k", "test")
    services.run_evaluation(services.start_evaluation(v2["version_id"], "test"), "test")
    services.approve(v2["version_id"], "release", "ops", "approved", "候选")
    return fid, v1["version_id"], v2["version_id"]


def make_trial(guard=GUARD, weight=100):
    """建族 + 批次（默认 100% 权重：所有 key 都走候选）。返回 (fid, v1, v2, ro)。"""
    fid, v1, v2 = make_family_with_candidate()
    ro = services.create_rollout(fid, v2, weight, "test", guard=guard)
    return fid, v1, v2, services.get_rollout(ro["rollout_id"])


def candidate_requests(fid, ro, n):
    """产生 n 笔命中候选的线上请求，返回 request_id 列表。"""
    reqs = []
    i = 0
    while len(reqs) < n:
        key = f"cand-{_uid()}-{i}"
        i += 1
        if core.stable_bucket(ro["rollout_id"], key) < ro["weight"]:
            res = services.invoke(fid, INPUTS, "t", routing_key=key)
            assert res["version_id"] == ro["candidate_version_id"]
            reqs.append(res["request_id"])
    return reqs


class GuardReceiptTest(unittest.TestCase):
    """回执入账规则：幂等、分类、一请求一样本。"""

    def test_duplicate_feedback_recorded_once(self):
        fid, v1, v2, ro = make_trial()
        rid = candidate_requests(fid, ro, 1)[0]
        fb_id = fbid()
        r1 = services.submit_feedback(rid, fb_id, "failure", "t")
        self.assertEqual((r1["counted"], r1["disposition"]), (1, "counted"))
        self.assertFalse(r1["duplicate"])
        # 同一 feedback_id 再次上报（即使结论不同）：返回首次入账结果，不产生变化
        r2 = services.submit_feedback(rid, fb_id, "success", "t")
        self.assertTrue(r2["duplicate"])
        self.assertEqual(r2["outcome"], "failure", "重复回执不得覆盖首次入账结论")
        self.assertIsNone(r2["triggered"])
        rows = services.list_feedback(rollout_id=ro["rollout_id"])
        self.assertEqual(len(rows), 1, "同一 feedback_id 只能入账一次")
        st = services.guard_status(ro["rollout_id"])["stats"]
        self.assertEqual((st["samples"], st["failures"]), (1, 1), "重复回执不得重复计数")

    def test_unrelated_and_baseline_requests_audit_only(self):
        fid, v1, v2, ro = make_trial()
        # 未知请求：只留审计
        r = services.submit_feedback("req_不存在", fbid(), "failure", "t")
        self.assertEqual((r["counted"], r["disposition"]), (0, "unknown_request"))
        self.assertIsNone(r["rollout_id"])
        # 基线流量（无 routing_key，未参与试投）：只留审计
        base_req = services.invoke(fid, INPUTS, "t")
        r = services.submit_feedback(base_req["request_id"], fbid(), "failure", "t")
        self.assertEqual((r["counted"], r["disposition"]), (0, "not_candidate"))
        # 分流未命中候选的请求（weight=50 的另一试投）
        fid2, vb1, vb2 = make_family_with_candidate()
        ro2 = services.create_rollout(fid2, vb2, 50, "test", guard=GUARD)
        i = 0
        while True:
            key = f"miss-{i}"
            i += 1
            if core.stable_bucket(ro2["rollout_id"], key) >= 50:
                miss_req = services.invoke(fid2, INPUTS, "t", routing_key=key)
                break
        self.assertEqual(miss_req["version_id"], vb1)
        r = services.submit_feedback(miss_req["request_id"], fbid(), "failure", "t")
        self.assertEqual((r["counted"], r["disposition"]), (0, "not_candidate"))
        # 两批次都仍在放量，无任何触发
        self.assertEqual(services.get_rollout(ro["rollout_id"])["status"], "ramping")
        self.assertEqual(services.get_rollout(ro2["rollout_id"])["status"], "ramping")
        self.assertEqual(services.list_guard_events(family_id=fid), [])
        self.assertEqual(services.list_guard_events(family_id=fid2), [])
        # 审计记录可查
        fb_unknown = services.submit_feedback("req_不存在2", fbid(), "failure", "t")
        fb = services.get_feedback(fb_unknown["feedback_id"])
        self.assertEqual(fb["disposition"], "unknown_request")

    def test_same_request_counted_once(self):
        guard = {"window_seconds": 3600, "min_samples": 10, "max_fail_ratio": 0.9}
        fid, v1, v2, ro = make_trial(guard=guard)
        rid = candidate_requests(fid, ro, 1)[0]
        a = services.submit_feedback(rid, fbid(), "success", "t")
        self.assertEqual(a["disposition"], "counted")
        b = services.submit_feedback(rid, fbid(), "failure", "t")
        self.assertEqual((b["counted"], b["disposition"]), (0, "request_already_counted"))
        st = services.guard_status(ro["rollout_id"])["stats"]
        self.assertEqual((st["samples"], st["failures"]), (1, 0),
                         "同一请求只能贡献一个样本")

    def test_receipts_without_guard_not_counted(self):
        fid, v1, v2, ro = make_trial(guard=None)
        r1, r2 = candidate_requests(fid, ro, 2)
        res = services.submit_feedback(r1, fbid(), "failure", "t")
        self.assertEqual((res["counted"], res["disposition"]), (0, "no_guard"))
        # 补上配置后新回执正常计数；旧的 no_guard 回执不追溯入账
        services.set_rollout_guard(ro["rollout_id"], 3600, 1, 0.0, "t")
        res2 = services.submit_feedback(r2, fbid(), "failure", "t")
        self.assertEqual(res2["counted"], 1)
        self.assertIsNotNone(res2["triggered"])
        ev = services.list_guard_events(ro["rollout_id"])[0]
        self.assertEqual(ev["samples"], 1, "no_guard 时期的回执不得追溯计入")

    def test_disabled_guard_not_counted(self):
        fid, v1, v2, ro = make_trial()
        services.set_rollout_guard(ro["rollout_id"], 3600, 1, 0.0, "t", enabled=0)
        rid = candidate_requests(fid, ro, 1)[0]
        res = services.submit_feedback(rid, fbid(), "failure", "t")
        self.assertEqual((res["counted"], res["disposition"]), (0, "guard_disabled"))
        self.assertEqual(services.get_rollout(ro["rollout_id"])["status"], "ramping")

    def test_guard_config_validation(self):
        fid, v1, v2, ro = make_trial(guard=None)
        rid = ro["rollout_id"]
        for bad in ((0, 5, 0.5), (60, 0, 0.5), (60, 5, 1.0), (60, 5, -0.1),
                    ("x", 5, 0.5)):
            with self.assertRaises(services.DomainError, msg=f"非法配置 {bad}"):
                services.set_rollout_guard(rid, *bad, "t")
        with self.assertRaises(services.NotFound):
            services.set_rollout_guard("ro_不存在", 60, 5, 0.5, "t")
        # upsert：同一批次只有一条配置，更新不回溯历史
        g = services.set_rollout_guard(rid, 60, 5, 0.5, "t")
        g2 = services.set_rollout_guard(rid, 120, 3, 0.3, "t")
        self.assertEqual(g["guard_id"], g2["guard_id"])
        self.assertEqual((g2["window_seconds"], g2["min_samples"]), (120, 3))
        # 非法 outcome / 缺参数
        with self.assertRaises(services.DomainError):
            services.submit_feedback("req-x", fbid(), "maybe", "t")
        with self.assertRaises(services.DomainError):
            services.submit_feedback("", fbid(), "failure", "t")


class GuardTriggerTest(unittest.TestCase):
    """阈值临界、触发快照、并发只一胜、观察窗口。"""

    def test_threshold_boundary(self):
        # min=2, 上限 0.5：1 失败 1 成功 → 0.5 等于上限不触发；再 1 失败 → 2/3 越过
        fid, v1, v2, ro = make_trial()
        r1, r2, r3 = candidate_requests(fid, ro, 3)
        services.submit_feedback(r1, fbid(), "failure", "t")
        res = services.submit_feedback(r2, fbid(), "success", "t")
        self.assertIsNone(res["triggered"], "失败占比等于上限（未越过）不得触发")
        self.assertEqual(services.get_rollout(ro["rollout_id"])["status"], "ramping")
        res = services.submit_feedback(r3, fbid(), "failure", "t")
        self.assertIsNotNone(res["triggered"], "失败占比越过上限必须触发")
        ro2 = services.get_rollout(ro["rollout_id"])
        self.assertEqual(ro2["status"], "frozen")
        self.assertEqual(ro2["generation"], ro["generation"] + 1)

    def test_below_min_samples_no_trigger(self):
        # min=3, 上限 0：2/2 全失败但样本未达门槛 → 不触发
        guard = {"window_seconds": 3600, "min_samples": 3, "max_fail_ratio": 0.0}
        fid, v1, v2, ro = make_trial(guard=guard)
        r1, r2 = candidate_requests(fid, ro, 2)
        for rid in (r1, r2):
            res = services.submit_feedback(rid, fbid(), "failure", "t")
            self.assertIsNone(res["triggered"], "样本未达门槛不得触发")
        self.assertEqual(services.get_rollout(ro["rollout_id"])["status"], "ramping")

    def test_trigger_snapshot_and_baseline_reroute(self):
        guard = {"window_seconds": 3600, "min_samples": 2, "max_fail_ratio": 0.4}
        fid, v1, v2, ro = make_trial(guard=guard)
        r1, r2 = candidate_requests(fid, ro, 2)
        fb1, fb2 = fbid(), fbid()
        services.submit_feedback(r1, fb1, "failure", "t")
        res = services.submit_feedback(r2, fb2, "failure", "t")
        self.assertIsNotNone(res["triggered"])
        # 触发快照：窗口统计 + 当时配置 + 触发回执，全部固化
        events = services.list_guard_events(ro["rollout_id"])
        self.assertEqual(len(events), 1)
        snap = events[0]
        self.assertEqual(snap["generation"], ro["generation"])
        self.assertEqual(snap["trigger_feedback_id"], fb2)
        self.assertEqual((snap["samples"], snap["failures"]), (2, 2))
        self.assertAlmostEqual(snap["fail_ratio"], 1.0)
        self.assertEqual(snap["window_seconds"], 3600)
        self.assertEqual(snap["min_samples"], 2)
        self.assertAlmostEqual(snap["max_fail_ratio"], 0.4)
        self.assertEqual(snap["action"], "freeze")
        # 单事务效果：批次冻结 + 新流量导回基线
        ro2 = services.get_rollout(ro["rollout_id"])
        self.assertEqual(ro2["status"], "frozen")
        key = f"post-{_uid()}"
        hit = services.invoke(fid, INPUTS, "t", routing_key=key)
        self.assertEqual(hit["version_id"], v1, "触发后新流量必须导回基线构件")
        self.assertIsNone(hit["rollout_id"])
        self.assertEqual(services.get_family(fid)["active_version_id"], v1,
                         "止损不得移动线上指针")

    def test_concurrent_receipts_trigger_once(self):
        guard = {"window_seconds": 3600, "min_samples": 1, "max_fail_ratio": 0.0}
        fid, v1, v2, ro = make_trial(guard=guard)
        reqs = candidate_requests(fid, ro, 6)
        fb_ids = [fbid() for _ in range(6)]
        barrier = threading.Barrier(6)
        results, errors = [], []

        def fire(i):
            barrier.wait()
            try:
                results.append(services.submit_feedback(reqs[i], fb_ids[i], "failure", "race"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=fire, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [], f"并发回执不应产生异常: {errors}")
        self.assertEqual(len(results), 6)
        triggered = [r for r in results if r["triggered"]]
        counted = [r for r in results if r["counted"] == 1]
        stale = [r for r in results if r["disposition"] == "stale_generation"]
        self.assertEqual(len(triggered), 1, "并发越界回执只能触发一次状态变化")
        self.assertEqual(len(counted), 1, "触发后其余回执落入旧代次，不再入账计数")
        self.assertEqual(len(stale), 5)
        events = services.list_guard_events(ro["rollout_id"])
        self.assertEqual(len(events), 1, "触发快照只能有一份")
        ro2 = services.get_rollout(ro["rollout_id"])
        self.assertEqual(ro2["generation"], ro["generation"] + 1, "代次只能 +1 一次")
        self.assertEqual(ro2["status"], "frozen")

    def test_window_expires_old_receipts(self):
        guard = {"window_seconds": 60, "min_samples": 2, "max_fail_ratio": 0.5}
        fid, v1, v2, ro = make_trial(guard=guard)
        r1, r2, r3 = candidate_requests(fid, ro, 3)
        fb1 = fbid()
        services.submit_feedback(r1, fb1, "failure", "t")
        # 把 fb1 的入账时间拨到观察窗口之外
        conn = db.get_conn()
        conn.execute("UPDATE feedback_receipts SET created_at=datetime('now','-3600 seconds') "
                     "WHERE feedback_id=?", (fb1,))
        conn.commit()
        res = services.submit_feedback(r2, fbid(), "failure", "t")
        self.assertIsNone(res["triggered"], "窗口外旧回执不计入：窗口内仅 1 样本 < 门槛 2")
        res = services.submit_feedback(r3, fbid(), "failure", "t")
        self.assertIsNotNone(res["triggered"], "窗口内 2/2 失败越过上限必须触发")
        ev = services.list_guard_events(ro["rollout_id"])[0]
        self.assertEqual(ev["samples"], 2, "触发快照只含窗口内样本")


class GuardRecoveryTest(unittest.TestCase):
    """迟到回执、人工恢复、旧窗口与历史请求复核。"""

    def _triggered_trial(self):
        guard = {"window_seconds": 3600, "min_samples": 2, "max_fail_ratio": 0.4}
        fid, v1, v2, ro = make_trial(guard=guard)
        reqs = candidate_requests(fid, ro, 3)
        services.submit_feedback(reqs[0], fbid(), "failure", "t")
        res = services.submit_feedback(reqs[1], fbid(), "failure", "t")
        assert res["triggered"], "前置：两张失败回执应已触发止损"
        return fid, v1, v2, ro, reqs

    def test_late_receipt_after_trigger_audit_only(self):
        fid, v1, v2, ro, reqs = self._triggered_trial()
        # 迟到回执：请求属于被冻结的旧代次 → 只留审计，不再触发
        late = services.submit_feedback(reqs[2], fbid(), "failure", "t")
        self.assertEqual((late["counted"], late["disposition"]), (0, "stale_generation"))
        self.assertIsNone(late["triggered"])
        self.assertEqual(len(services.list_guard_events(ro["rollout_id"])), 1)
        ro2 = services.get_rollout(ro["rollout_id"])
        self.assertEqual(ro2["generation"], ro["generation"] + 1, "迟到回执不得再次改代次")
        # 审计记录仍在，可按批次检索
        rows = services.list_feedback(rollout_id=ro["rollout_id"])
        self.assertEqual(len(rows), 3)

    def test_manual_recovery_resets_window_and_keeps_history(self):
        fid, v1, v2, ro, reqs = self._triggered_trial()
        ro_frozen = services.get_rollout(ro["rollout_id"])
        self.assertEqual(ro_frozen["status"], "frozen")
        # 人工恢复：开启新代次
        resumed = services.resume_rollout(ro["rollout_id"], ro_frozen["generation"], "ops")
        self.assertEqual(resumed["status"], "ramping")
        self.assertEqual(resumed["generation"], ro_frozen["generation"] + 1)
        # 统计窗口重置：新代次窗口内无任何入账样本
        st = services.guard_status(ro["rollout_id"])["stats"]
        self.assertEqual((st["samples"], st["failures"]), (0, 0))
        # 新代次的第一张失败回执入账但不触发（旧代次的 2 次失败不得累计过来）
        r3, r4 = candidate_requests(fid, resumed, 2)
        res = services.submit_feedback(r3, fbid(), "failure", "t")
        self.assertEqual(res["counted"], 1)
        self.assertIsNone(res["triggered"], "旧代次失败不得计入新窗口")
        # 旧代次请求的迟到回执仍然只留审计
        late = services.submit_feedback(reqs[2], fbid(), "failure", "t")
        self.assertEqual(late["disposition"], "stale_generation")
        # 旧窗口回执可按旧代次过滤复核
        old = services.list_feedback(rollout_id=ro["rollout_id"],
                                     generation=ro["generation"])
        self.assertEqual(len(old), 3, "旧窗口回执必须保留可复核")
        # 触发历史保留
        events = services.list_guard_events(ro["rollout_id"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["generation"], ro["generation"])
        # 历史请求复核能力保留：按原元数据重演仍复现
        rep = services.replay(reqs[0], "t")
        self.assertEqual(rep["verdict"], "reproduced")
        self.assertEqual(rep["replayed"]["version_id"], v2)
        req_row = services.get_request(reqs[0])
        self.assertEqual(req_row["rollout_generation"], ro["generation"],
                         "历史请求固化的命中代次不受恢复影响")

    def test_retrigger_after_recovery(self):
        """恢复后候选仍不达标：新窗口内再次越界可再次触发（各自落快照）。"""
        fid, v1, v2, ro, reqs = self._triggered_trial()
        ro_frozen = services.get_rollout(ro["rollout_id"])
        resumed = services.resume_rollout(ro["rollout_id"], ro_frozen["generation"], "ops")
        r3, r4 = candidate_requests(fid, resumed, 2)
        services.submit_feedback(r3, fbid(), "failure", "t")
        res = services.submit_feedback(r4, fbid(), "failure", "t")
        self.assertIsNotNone(res["triggered"], "新窗口内再次越界应再次触发")
        events = services.list_guard_events(ro["rollout_id"])
        self.assertEqual(len(events), 2)
        self.assertEqual({e["generation"] for e in events},
                         {ro["generation"], resumed["generation"]},
                         "两次触发分属不同代次，快照各自保留")


class GuardHttpTest(unittest.TestCase):
    """JSON API 与页面冒烟。"""

    @classmethod
    def setUpClass(cls):
        os.environ["PROMPTGOV_SEED"] = "0"
        from promptgov import app
        cls.httpd = app.build_server()
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def _req(self, method, path, body=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def test_guard_api_and_pages(self):
        fid, v1, v2 = make_family_with_candidate()
        # 建批时一并配置止损
        code, ro = self._req("POST", f"/api/families/{fid}/rollouts",
                             {"candidate_version_id": v2, "weight": 100, "guard": GUARD})
        self.assertEqual(code, 201, ro)
        rid = ro["rollout_id"]
        code, st = self._req("GET", f"/api/rollouts/{rid}/guard")
        self.assertEqual(code, 200)
        self.assertEqual(st["guard"]["min_samples"], GUARD["min_samples"])
        self.assertEqual(st["stats"]["samples"], 0)
        # 两笔命中候选的调用
        reqs = []
        for i in range(2):
            code, res = self._req("POST", f"/api/families/{fid}/invoke",
                                  {"inputs": INPUTS, "routing_key": f"hk-{i}"})
            self.assertEqual(code, 201, res)
            self.assertEqual(res["version_id"], v2)
            reqs.append(res["request_id"])
        # 回执：201 入账；同 feedback_id 重报 → 200 幂等返回
        fb1, fb2 = fbid("http"), fbid("http")
        code, fb = self._req("POST", "/api/feedback",
                             {"request_id": reqs[0], "feedback_id": fb1,
                              "outcome": "failure"})
        self.assertEqual(code, 201, fb)
        self.assertEqual(fb["counted"], 1)
        code, dup = self._req("POST", "/api/feedback",
                              {"request_id": reqs[0], "feedback_id": fb1,
                               "outcome": "failure"})
        self.assertEqual(code, 200)
        self.assertTrue(dup["duplicate"])
        # 第二张回执越界 → 单事务冻结 + 快照
        code, fb2res = self._req("POST", "/api/feedback",
                                 {"request_id": reqs[1], "feedback_id": fb2,
                                  "outcome": "failure"})
        self.assertEqual(code, 201)
        self.assertIsNotNone(fb2res["triggered"])
        code, ro2 = self._req("GET", f"/api/rollouts/{rid}")
        self.assertEqual(ro2["status"], "frozen")
        # 新流量导回基线
        code, res = self._req("POST", f"/api/families/{fid}/invoke",
                              {"inputs": INPUTS, "routing_key": "hk-9"})
        self.assertEqual(res["version_id"], v1)
        # 触发快照 / 回执列表 / 单条回执
        code, events = self._req("GET", f"/api/rollouts/{rid}/guard/events")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["trigger_feedback_id"], fb2)
        code, fbs = self._req("GET", f"/api/rollouts/{rid}/feedback")
        self.assertEqual(len(fbs), 2)
        code, one = self._req("GET", f"/api/feedback/{fb1}")
        self.assertEqual(one["disposition"], "counted")
        # 无关请求回执 → 仅审计
        code, bad = self._req("POST", "/api/feedback",
                              {"request_id": "req_无", "feedback_id": fbid("http"),
                               "outcome": "failure"})
        self.assertEqual(code, 201)
        self.assertEqual(bad["disposition"], "unknown_request")
        # 人工恢复（API）：新代次，统计重置
        code, ro3 = self._req("POST", f"/api/rollouts/{rid}/resume",
                              {"expected_generation": ro2["generation"]})
        self.assertEqual(code, 200)
        self.assertEqual(ro3["status"], "ramping")
        self.assertEqual(ro3["generation"], ro2["generation"] + 1)
        code, st = self._req("GET", f"/api/rollouts/{rid}/guard")
        self.assertEqual(st["stats"]["samples"], 0, "恢复后统计窗口必须重置")
        # 配置更新（upsert）
        code, g = self._req("POST", f"/api/rollouts/{rid}/guard",
                            {"window_seconds": 300, "min_samples": 4,
                             "max_fail_ratio": 0.25})
        self.assertEqual(code, 201)
        self.assertEqual((g["window_seconds"], g["min_samples"]), (300, 4))
        # 非法配置 → 400
        code, err = self._req("POST", f"/api/rollouts/{rid}/guard",
                              {"window_seconds": 0, "min_samples": 4,
                               "max_fail_ratio": 0.25})
        self.assertEqual(code, 400)
        # 页面冒烟：止损页 / 放量页 / 调用详情页
        for path in (f"/families/{fid}/guard", f"/families/{fid}/rollout",
                     f"/requests/{reqs[0]}/replay"):
            req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}")
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200, path)
                self.assertIn("text/html", resp.headers["Content-Type"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
