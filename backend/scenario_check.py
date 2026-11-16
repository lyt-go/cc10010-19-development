"""端到端场景校验：分区检测 -> 受控交接 -> 冲突仲裁 -> 原子提交 -> 异常重跑 -> 任务文件。"""
import json
import sys
import threading
import time
import urllib.request

import uvicorn

from app.main import app

BASE = "http://127.0.0.1:8011"

config = uvicorn.Config(app, host="127.0.0.1", port=8011, log_level="warning")
server = uvicorn.Server(config)
thread = threading.Thread(target=server.run, daemon=True)
thread.start()
for _ in range(50):
    try:
        urllib.request.urlopen(BASE + "/api/health", timeout=1)
        break
    except OSError:
        time.sleep(0.1)



def call(method: str, path: str, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as resp:
        raw = resp.read().decode("utf-8")
        ctype = resp.headers.get("Content-Type", "")
        return resp.status, (raw if "text/csv" in ctype else json.loads(raw))


def must(ok, message):
    if not ok:
        print("ASSERT FAIL:", message)
        sys.exit(1)


# 规则 3 在 seed 中是「已启用」，打开自动补齐开关、设优先级（直接改内存仓库模拟配置生效）
from app.store import store
rule3 = store.find("partitioncheck", 3)
rule3["自动补齐开关"] = "是"
rule3["补齐优先级"] = "高"

# 1) 第一次分区检测：两个缺失分区
_, body = call("POST", "/api/partitionbackfill/detections", {
    "values": {"规则id": 3, "缺失分区清单": [
        {"分区标识": "dt=2026-11-15", "优先级": "高"},
        {"分区标识": "dt=2026-11-16"},
    ]}
})
must(body["ok"], body["message"])
batch1 = body["entry"]
must(batch1["缺失分区数"] == 2, "批次1应有2个缺失分区")
must(len(batch1["清单校验和"]) == 16, "清单应有校验和")

# 2) 重复检测同一缺失分区 + 一个新分区
_, body = call("POST", "/api/partitionbackfill/detections", {
    "values": {"规则id": 3, "缺失分区清单": ["dt=2026-11-15", "dt=2026-11-17"]}
})
batch2 = body["entry"]
must(batch2["检测批次号"] != batch1["检测批次号"], "新旧检测批次必须区分")

# 3) 两个批次分别受控交接
_, b = call("POST", f"/api/partitionbackfill/detections/{batch1['id']}/handoff")
must(b["ok"], b["message"])
must(b["entry"]["新建任务数"] == 2, f"首次交接应新建2份任务，实际 {b['entry']}")

_, b = call("POST", f"/api/partitionbackfill/detections/{batch2['id']}/handoff")
must(b["ok"], b["message"])
must(b["entry"]["新建任务数"] == 1 and b["entry"]["复用任务数"] == 1, "重复分区只复用不新建，新分区新建")

# 4) 同一分区始终只有一份任务
_, page = call("GET", "/api/partitionbackfill/tasks?size=200")
tasks = page["items"]
p15 = [t for t in tasks if t["分区标识"] == "dt=2026-11-15"]
must(len(p15) == 1, "同一缺失分区只允许一份补齐任务")
must(set(p15[0]["关联检测批次"]) == {batch1["检测批次号"], batch2["检测批次号"]}, "复用任务需挂接两个检测批次")
must(p15[0]["任务状态"] == "待补齐", "自动补齐开关开启时任务直接进入待补齐")
must(p15[0]["运行实例分区状态"] == "未下发", "运行实例状态与补齐任务状态必须分开")

# 5) 快照校验：篡改清单后拒绝交接（直接改内存 store，模拟快照被污染）
target = store.rows("partition_detection_batch")[0]
original = list(target["缺失分区清单"])
target["缺失分区清单"][0]["分区标识"] = "dt=tampered"
_, err = call("POST", f"/api/partitionbackfill/detections/{target['id']}/handoff")
must(not err.get("ok", True) and "校验失败" in err.get("message", ""), "快照被篡改必须拒绝交接")
target["缺失分区清单"] = original

# 6) 任务文件下载：含分区标识与优先级，重复下载一致
status, csv1 = call("GET", f"/api/partitionbackfill/tasks/{p15[0]['id']}/download")
must(status == 200 and "dt=2026-11-15" in csv1 and "高" in csv1, "任务文件必须包含分区标识和优先级")
_, csv_again = call("GET", f"/api/partitionbackfill/tasks/{p15[0]['id']}/download")
must(csv_again == csv1, "重新下载任务文件内容必须保持完整一致")

# 7) 冲突仲裁：自动补齐先成功，人工处理晚到（人工优先覆盖）；随后自动结果再被驳回
_, b = call("POST", f"/api/partitionbackfill/tasks/{p15[0]['id']}/results", {
    "values": {"处理来源": "自动补齐", "处理成功": True, "快照校验和": p15[0]["快照校验和"]}
})
must(b["ok"], b["message"])
_, task = call("GET", f"/api/partitionbackfill/tasks/{p15[0]['id']}")
must(task["任务状态"] == "已补齐" and task["完整性结论"] == "分区完整", "成功结果与完整性结论应同时落库")

_, b = call("POST", f"/api/partitionbackfill/tasks/{p15[0]['id']}/results", {
    "values": {"处理来源": "人工处理", "处理成功": False, "备注": "人工核验数据缺失"}
})
must(b["ok"], b["message"])
_, task = call("GET", f"/api/partitionbackfill/tasks/{p15[0]['id']}")
must(task["任务状态"] == "补齐失败" and task["完整性结论"] == "分区不完整"
    and task["采纳来源"] == "人工处理", "人工处理晚到应覆盖自动补齐结论并覆盖检测规则口径")

_, rejected_body = call("POST", f"/api/partitionbackfill/tasks/{p15[0]['id']}/results",
                        {"values": {"处理来源": "自动补齐", "处理成功": True}})
must("驳回" in rejected_body["message"], "已采纳人工结论后，自动补齐结果必须被冲突规则驳回")
_, task_after = call("GET", f"/api/partitionbackfill/tasks/{p15[0]['id']}")
must(task_after["完整性结论"] == "分区不完整" and task_after["采纳来源"] == "人工处理",
    "被驳回的自动结果不得改变既有完整性结论")

_, results_page = call("GET", f"/api/partitionbackfill/tasks/{p15[0]['id']}/results")
must(len(results_page["items"]) == 3, "三条处理记录都要留痕")
must(any(not x["是否采纳"] for x in results_page["items"]), "被覆盖/驳回的记录需明确标记")

# 8) 同源晚到冲突：较晚到达的自动结果覆盖较早自动结果（另一分区）
_, page = call("GET", "/api/partitionbackfill/tasks?size=200")
p16 = [t for t in page["items"] if t["分区标识"] == "dt=2026-11-16"][0]
call("POST", f"/api/partitionbackfill/tasks/{p16['id']}/results",
     {"values": {"处理来源": "自动补齐", "处理成功": False}})
_, b = call("POST", f"/api/partitionbackfill/tasks/{p16['id']}/results",
            {"values": {"处理来源": "自动补齐", "处理成功": True}})
must(b["ok"], b["message"])
_, task = call("GET", f"/api/partitionbackfill/tasks/{p16['id']}")
must(task["完整性结论"] == "分区完整", "同源晚到成功结果应接管结论")

# 9) 异常重跑：只消费仍未补齐的分区（p15 失败未补齐，p17 待补齐；p16 已补齐）
_, b = call("POST", "/api/rerun/partition-snapshot/consume", {
    "values": {"检测批次id": batch2["id"], "快照校验和": batch2["清单校验和"]}
})
must(b["ok"], b["message"])
entry = b["entry"]
consumed = {x["分区标识"] for x in entry["待重跑分区"]}
must(consumed == {"dt=2026-11-15", "dt=2026-11-17"}, f"重跑只能消费未补齐分区: {consumed}")
must(all(x["跳过原因"] == "分区已补齐" for x in entry["跳过分区"]), "已补齐分区标记跳过")
rerun_no = entry["重跑清单编号"]

_, rejected_body = call("POST", "/api/rerun/partition-snapshot/consume",
                        {"values": {"检测批次id": batch2["id"], "快照校验和": "deadbeefdeadbeef"}})
must(not rejected_body["ok"] and "校验失败" in rejected_body["message"], "快照过期必须拒绝消费")

# 10) 重跑协作业务面：向未完成分区写运行实例处理状态
_, page = call("GET", "/api/partitionbackfill/tasks?size=200")
p15_task = [t for t in page["items"] if t["分区标识"] == "dt=2026-11-15"][0]
_, b = call("POST", f"/api/rerun/partition-tasks/{p15_task['id']}/status", {
    "values": {"运行实例分区状态": "运行实例处理中", "重跑清单编号": rerun_no}
})
must(b["ok"], b["message"])
_, task = call("GET", f"/api/partitionbackfill/tasks/{p15_task['id']}")
must(task["运行实例分区状态"] == "运行实例处理中", "运行实例状态独立字段回写")

_, rejected_body = call("POST", f"/api/rerun/partition-tasks/{p16['id']}/status",
                        {"values": {"运行实例分区状态": "运行实例处理中"}})
must(not rejected_body["ok"], "已补齐分区不允许重跑重复消费")

# 11) 业务结果落重跑清单并随检测重算：p15 重跑成功，原子提交完整性
_, b = call("POST", f"/api/rerun/partition-rerun/items/{rerun_no}/results", {
    "values": {"业务结果": "重跑补齐成功，数据回刷完成", "成功分区": ["dt=2026-11-15"]}
})
must(b["ok"], b["message"])
_, task = call("GET", f"/api/partitionbackfill/tasks/{p15_task['id']}")
must(task["任务状态"] == "已补齐" and task["完整性结论"] == "分区完整"
    and task["采纳来源"] == "异常重跑", "重跑成功必须原子提交补齐完成与完整性结论")
_, items_page = call("GET", "/api/rerun/partition-rerun/items")
item = items_page["items"][-1]
must(item["业务结果"] != "" and item["清单状态"] == "已重算", "业务结果必须落到重跑清单")

# 12) 再次消费快照：p15 已补齐，应自动跳过
_, b = call("POST", "/api/rerun/partition-snapshot/consume",
            {"values": {"检测批次id": batch2["id"]}})
consumed = {x["分区标识"] for x in b["entry"]["待重跑分区"]}
must("dt=2026-11-15" not in consumed, "核对结果时重跑只消费仍未补齐的分区")

# 13) 检测批次收尾：批次1全部补齐
_, b1 = call("GET", f"/api/partitionbackfill/detections/{batch1['id']}")
must(b1["批次状态"] == "全部补齐", f"批次内分区全部补齐后应收尾，实际 {b1['批次状态']}")

# 14) 分区补齐后再次检测到缺失：重开同一任务而非新建第二份
_, body = call("POST", "/api/partitionbackfill/detections", {
    "values": {"规则id": 3, "缺失分区清单": ["dt=2026-11-15"]}
})
batch3 = body["entry"]
call("POST", f"/api/partitionbackfill/detections/{batch3['id']}/handoff")
_, page = call("GET", "/api/partitionbackfill/tasks?size=200")
p15_all = [t for t in page["items"] if t["分区标识"] == "dt=2026-11-15"]
must(len(p15_all) == 1 and p15_all[0]["任务状态"] != "已补齐", "再次缺失必须重开原任务，保持一份任务")

# 15) 待交接任务（关闭自动补齐的规则）走人工确认
_, rule2 = call("GET", "/api/partitioncheck/1")
_, body = call("POST", "/api/partitionbackfill/detections", {
    "values": {"规则id": 1, "数据表名": rule2["数据表名"], "缺失分区清单": ["dt=2026-11-10"]}
})
b4 = body["entry"]
_, b = call("POST", f"/api/partitionbackfill/detections/{b4['id']}/handoff")
new_task = b["entry"]["任务"][0]
must(new_task["任务状态"] == "待交接", "未开启自动补齐时任务应停在待交接")
_, b = call("POST", f"/api/partitionbackfill/tasks/{new_task['id']}/confirm")
must(b["ok"] and b["entry"]["任务状态"] == "待补齐", "人工确认后进入待补齐")

# 16) 批量任务文件下载
status, bundle = call("GET", "/api/partitionbackfill/tasks-file/bundle")
must(status == 200 and "分区标识" in bundle and "优先级" in bundle, "批量任务文件需包含分区标识与优先级列")

print("ALL SCENARIOS PASSED")
