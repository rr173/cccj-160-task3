"""幂等演示种子：一个模板族 + 固定测试集 + 覆盖治理全链路的版本与流量。"""
from __future__ import annotations

import json

from . import services
from .db import get_conn

FAMILY = "support-router"

CONTRACT_V1 = {
    "customer_text": {"type": "string", "required": True},
    "lang": {"type": "string", "required": False},
}
CONTRACT_V2 = {
    "customer_text": {"type": "string", "required": True},
    "lang": {"type": "string", "required": False},
    "channel": {"type": "string", "required": False},   # 新增可选变量：兼容升级
}
PARAMS = {"temperature": 0.2, "top_p": 0.9, "max_tokens": 1024}

INSTRUCTION_V1 = """你是客服分流助手。仅依据客户文本输出结构化判断。
客户原话：{{ customer_text }}
{% if lang %}语言：{{ lang }}{% endif %}
请给出 label 与 score。"""

INSTRUCTION_V2 = """你是客服分流助手（v2 增强版）。综合渠道信息做判断。
渠道：{{ channel | default("web") }}
客户原话：{{ customer_text }}
{% if lang %}语言：{{ lang }}{% endif %}
优先识别 positive 反馈。请给出 label 与 score。"""

INSTRUCTION_V3 = INSTRUCTION_V2  # v3 仅扩大工具权限，模板内容相同，演示权限门禁

TEST_CASES = [
    {"name": "满意-中文", "inputs": {"customer_text": "非常满意，解决很快，点赞", "lang": "zh",
                                    "channel": "app"},
     "expect": {"assert": "json_field", "field": "label", "equals": "positive"}},
    {"name": "满意-英文", "inputs": {"customer_text": "Great support, very happy", "lang": "en",
                                    "channel": "web"},
     "expect": {"assert": "json_field", "field": "label", "equals": "positive"}},
    {"name": "高置信", "inputs": {"customer_text": "太棒了，强烈推荐给朋友", "lang": "zh",
                                 "channel": "app"},
     "expect": {"assert": "json_field", "field": "score", "min": 60}},
    {"name": "包含正向词", "inputs": {"customer_text": "positive experience overall",
                                    "lang": "en", "channel": "web"},
     "expect": {"assert": "json_field", "field": "label", "equals": "positive"}},
]

# 演示用输入
ONLINE_INPUTS = {"customer_text": "客服非常耐心，问题一次解决", "lang": "zh", "channel": "app"}


def run() -> None:
    conn = get_conn()
    if conn.execute("SELECT 1 FROM families WHERE family_id=?", (FAMILY,)).fetchone():
        return
    actor = "seed"
    services.create_family(FAMILY, "客服分流模板", "把客户反馈分流为 positive/neutral/negative",
                           actor)
    services.create_testset(FAMILY, "固定回归测试集 v1（不可变）", TEST_CASES, actor)

    # v1：评估 -> 发布审批 -> 发布为线上版本
    v1 = services.create_version(FAMILY, "1.0.0", INSTRUCTION_V1, CONTRACT_V1,
                                 ["ticket.read", "kb.search"], PARAMS,
                                 "langfang-3-64k", actor)
    run1 = services.start_evaluation(v1["version_id"], actor)
    services.run_evaluation(run1, actor)
    services.approve(v1["version_id"], "release", "ops-lead", "approved",
                     "首发：评估全绿，权限最小", )
    services.promote(FAMILY, v1["version_id"], actor)
    # 产生一条线上请求，供后续重演
    services.invoke(FAMILY, ONLINE_INPUTS, actor)

    # v2：契约兼容升级 + 指令增强（权限不变），评估通过并审批，但暂不发布（候选）
    v2 = services.create_version(FAMILY, "1.1.0", INSTRUCTION_V2, CONTRACT_V2,
                                 ["ticket.read", "kb.search"], PARAMS,
                                 "langfang-3-64k", actor)
    run2 = services.start_evaluation(v2["version_id"], actor)
    services.run_evaluation(run2, actor)
    services.approve(v2["version_id"], "release", "ops-lead", "approved",
                     "兼容新增 channel 变量，评估通过")

    # v3：工具权限扩大版本（新增 refund.execute），演示"必须重新审批"
    v3 = services.create_version(FAMILY, "1.2.0", INSTRUCTION_V2, CONTRACT_V2,
                                 ["ticket.read", "kb.search", "refund.execute"], PARAMS,
                                 "langfang-3-64k", actor)
    run3 = services.start_evaluation(v3["version_id"], actor)
    services.run_evaluation(run3, actor)
    services.approve(v3["version_id"], "release", "ops-lead", "approved",
                     "发布审批通过（注意：权限扩大仍需专项审批）")

    # v4：能力较弱模型的草稿，评估可能不达标，演示模型差异/失败证据
    v4 = services.create_version(FAMILY, "1.3.0", INSTRUCTION_V2, CONTRACT_V2,
                                 ["ticket.read", "kb.search"], PARAMS,
                                 "langfang-3-mini", actor)
    run4 = services.start_evaluation(v4["version_id"], actor)
    services.run_evaluation(run4, actor)

    # 分批放量演示：v2（已审核、权限未扩大）建立 20% 权重批次；
    # 两笔带 routing_key 的线上调用，固化命中构件/批次代次/参数指纹
    ro = services.create_rollout(FAMILY, v2["version_id"], 20, actor)
    # 自动止损演示：为试投配置观察窗口/最小样本量/失败占比上限
    services.set_rollout_guard(ro["rollout_id"], 900, 5, 0.4, actor)
    r1 = services.invoke(FAMILY, ONLINE_INPUTS, actor, routing_key="user-10086")
    r2 = services.invoke(FAMILY, ONLINE_INPUTS, actor, routing_key="user-20431")
    # 命中候选的请求补一条成功回执，演示回执入账（未命中候选的只留审计）
    for r in (r1, r2):
        if r["rollout_id"]:
            services.submit_feedback(r["request_id"], f"seed-fb-{r['request_id'][-6:]}",
                                     "success", actor, "演示回执")

    conn.commit()
    print("[seed] 演示数据就绪：family=%s 线上=v1，候选 v2（20%% 放量中，已配止损）、"
          "v3（需工具权限审批）、v4（弱模型证据）" % FAMILY)
