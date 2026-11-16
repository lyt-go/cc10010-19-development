"""分区补齐业务规则。

承接分区检测产生的缺失分区清单，形成可执行的分区补齐任务，并处理：

- 受控交接：检测批次的缺失分区清单生成补齐任务数据快照（含校验和），交接时校验快照；
- 幂等去重：同一数据表分区在未补齐期间只存在一份补齐任务，重复检测不会重复建单；
- 冲突仲裁：自动补齐与人工处理可能同时命中同一分区，采用「人工优先、同源较晚到达优先」，
  被采纳的处理记录决定分区完整性结论，并覆盖原先检测规则口径；
- 原子提交：补齐完成与分区完整性结论在同一事务里落库，中途异常整体回滚；
- 异常重跑对接：重跑只消费仍未补齐的分区，业务结果落到重跑清单并随分区检测重算。

状态流转只允许在本模块修改，路由层不做业务判断。
"""
from __future__ import annotations

import copy
import hashlib
import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from app.store import store

MODULE = "partitionbackfill"
BATCH_TABLE = "partition_detection_batch"
TASK_TABLE = "partition_backfill_task"
RESULT_TABLE = "partition_process_result"
RERUN_TABLE = "partition_rerun_item"

# 补齐任务自身的处理状态（与运行实例里的分区处理状态分开存储、分开呈现）
TASK_STATUSES = ["待交接", "待补齐", "补齐中", "已补齐", "补齐失败"]
# 运行实例侧回写的分区处理状态
RUNTIME_STATUSES = ["未下发", "运行实例处理中", "运行实例已完成", "运行实例失败"]
# 处理来源
SOURCE_AUTO = "自动补齐"
SOURCE_MANUAL = "人工处理"
SOURCE_RERUN = "异常重跑"
# 完整性结论
CONCLUSION_PENDING = "待判定"
CONCLUSION_COMPLETE = "分区完整"
CONCLUSION_INCOMPLETE = "分区不完整"
CONCLUSION_CONFLICT = "结论冲突待人工复核"

BATCH_OPEN = "待补齐"
BATCH_DONE = "全部补齐"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _next_id(table: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1


def canonical_payload(payload: Any) -> str:
    """稳定序列化：字典按 key 排序、确保中文不转义，供快照校验和与复算使用。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def checksum_of(payload: Any) -> str:
    return hashlib.sha256(canonical_payload(payload).encode("utf-8")).hexdigest()[:16]


@contextmanager
def transaction(tables: list[str]) -> Iterator[None]:
    """内存仓库上的最小事务：进入时深拷贝相关表，异常时整体回滚。

    补齐完成与分区完整性结论必须原子提交，任何一步抛错都不能留下中间态。
    """
    snapshot = {name: copy.deepcopy(store.rows(name)) for name in tables}
    try:
        yield
    except Exception:
        for name, rows in snapshot.items():
            store._tables[name] = rows  # type: ignore[attr-defined]
        raise


def _rule_priority(rule_id: int) -> str:
    rule = store.find("partitioncheck", rule_id)
    if rule is not None and str(rule.get("补齐优先级") or "").strip():
        return str(rule["补齐优先级"])
    return "普通"


def _auto_backfill_enabled(rule_id: int) -> bool:
    rule = store.find("partitioncheck", rule_id)
    if rule is None:
        return False
    flag = str(rule.get("自动补齐开关") or "").strip()
    return flag in ("是", "开启", "true", "True", "1", "on", "ON")


class PartitionBackfillService:
    # ------------------------------------------------------------------ 检测批次

    def create_detection(
        self,
        *,
        rule_id: int,
        table_name: str | None = None,
        missing_partitions: list[Any] | None = None,
        remark: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """分区检测重算：登记一个检测批次并固化缺失分区清单快照。

        每次检测都会生成新批次（用于区分新旧检测批次），但同一缺失分区只对应一份补齐任务。
        """
        rule = store.find("partitioncheck", rule_id)
        if rule is None:
            return None, f"检测规则 {rule_id} 不存在或已归档，不能发起分区检测"
        resolved_table = str(table_name or rule.get("数据表名") or "").strip()
        if not resolved_table:
            return None, "检测规则缺少数据表名，无法定位缺失分区"

        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in missing_partitions or []:
            if isinstance(item, dict):
                partition = str(item.get("分区标识") or item.get("partition") or "").strip()
                priority = str(item.get("优先级") or "").strip() or _rule_priority(rule_id)
            else:
                partition = str(item).strip()
                priority = _rule_priority(rule_id)
            if not partition or partition in seen:
                continue
            seen.add(partition)
            normalized.append({"分区标识": partition, "优先级": priority})
        if not normalized:
            return None, "缺失分区清单为空，无需生成补齐任务"

        with transaction([BATCH_TABLE]):
            batch = {
                "id": _next_id(BATCH_TABLE),
                "检测批次号": f"DET-{datetime.now().strftime('%Y%m%d')}-{_next_id(BATCH_TABLE):04d}",
                "规则编号": rule.get("规则编号"),
                "规则id": rule_id,
                "数据表名": resolved_table,
                "期望分区数": rule.get("期望分区数"),
                "缺失分区清单": normalized,
                "缺失分区数": len(normalized),
                "检测时间": _now(),
                "批次状态": BATCH_OPEN,
                "备注": remark or "",
            }
            batch["清单校验和"] = checksum_of(normalized)
            store.rows(BATCH_TABLE).append(batch)
        return batch, "分区检测完成，缺失分区清单已固化"

    def list_detections(
        self,
        *,
        keyword: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(BATCH_TABLE))
        if keyword:
            rows = [
                row
                for row in rows
                if keyword in str(row.get("检测批次号", "")) or keyword in str(row.get("数据表名", ""))
            ]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_detection(self, batch_id: int) -> dict[str, Any] | None:
        return store.find(BATCH_TABLE, batch_id)

    # ---------------------------------------------------------- 受控交接：建补齐任务

    def handoff(self, batch_id: int) -> tuple[dict[str, Any] | None, str]:
        """把检测批次的缺失分区清单受控交接为可执行的分区补齐任务。

        - 交接前重新计算清单校验和，快照被篡改则拒绝交接；
        - 同一数据表分区只保留一份补齐任务：已有未补齐任务则复用，已补齐后再次缺失则重开；
        - 自动补齐开关关闭的规则只生成待交接任务，等待人工确认后再进入待补齐。
        """
        batch = store.find(BATCH_TABLE, batch_id)
        if batch is None:
            return None, f"检测批次 {batch_id} 不存在，无法交接"
        if checksum_of(batch.get("缺失分区清单")) != batch.get("清单校验和"):
            return None, "缺失分区清单快照校验失败，清单可能已被篡改，拒绝交接"

        tasks = store.rows(TASK_TABLE)
        created: list[dict[str, Any]] = []
        reused: list[dict[str, Any]] = []
        next_task_id = _next_id(TASK_TABLE)
        next_task_seq = next_task_id
        with transaction([BATCH_TABLE, TASK_TABLE]):
            for item in batch["缺失分区清单"]:
                partition = item["分区标识"]
                existing = next(
                    (
                        row
                        for row in tasks
                        if row["数据表名"] == batch["数据表名"]
                        and row["分区标识"] == partition
                    ),
                    None,
                )
                if existing is not None and existing["任务状态"] != "已补齐":
                    # 重复检测同一缺失分区：只保留一份补齐任务，挂接最新批次即可
                    existing["关联检测批次"] = sorted(
                        set(list(existing.get("关联检测批次") or [])) | {batch["检测批次号"]}
                    )
                    existing["快照版本"] = batch["检测批次号"]
                    reused.append(existing)
                    continue

                if existing is not None and existing["任务状态"] == "已补齐":
                    # 分区补齐后又在新检测批次中缺失：重开原任务而不是新建第二份
                    existing["任务状态"] = "待补齐" if _auto_backfill_enabled(batch["规则id"]) else "待交接"
                    existing["完整性结论"] = CONCLUSION_PENDING
                    existing["快照版本"] = batch["检测批次号"]
                    existing["关联检测批次"] = sorted(
                        set(list(existing.get("关联检测批次") or [])) | {batch["检测批次号"]}
                    )
                    existing["更新时间"] = _now()
                    reused.append(existing)
                    continue

                task = {
                    "id": next_task_id,
                    "补齐任务编号": f"FILL-{datetime.now().strftime('%Y%m%d')}-{next_task_seq:04d}",
                    "数据表名": batch["数据表名"],
                    "分区标识": partition,
                    "优先级": item["优先级"],
                    "规则编号": batch["规则编号"],
                    "规则id": batch["规则id"],
                    "来源检测批次": batch["检测批次号"],
                    "关联检测批次": [batch["检测批次号"]],
                    "快照版本": batch["检测批次号"],
                    "任务状态": "待补齐" if _auto_backfill_enabled(batch["规则id"]) else "待交接",
                    "运行实例分区状态": "未下发",
                    "完整性结论": CONCLUSION_PENDING,
                    "创建时间": _now(),
                    "更新时间": _now(),
                }
                snapshot_payload = {
                    "补齐任务编号": task["补齐任务编号"],
                    "数据表名": task["数据表名"],
                    "分区标识": task["分区标识"],
                    "优先级": task["优先级"],
                    "规则编号": task["规则编号"],
                    "来源检测批次": task["来源检测批次"],
                }
                task["任务快照"] = snapshot_payload
                task["快照校验和"] = checksum_of(snapshot_payload)
                tasks.append(task)
                created.append(task)
                next_task_id += 1
                next_task_seq += 1
        return (
            {
                "检测批次号": batch["检测批次号"],
                "新建任务数": len(created),
                "复用任务数": len(reused),
                "任务": created + reused,
            },
            f"交接完成：新建补齐任务 {len(created)} 份，复用既有任务 {len(reused)} 份",
        )

    def confirm_manual(self, task_id: int) -> tuple[dict[str, Any] | None, str]:
        """人工确认待交接任务，使其进入可执行的待补齐队列。"""
        task = store.find(TASK_TABLE, task_id)
        if task is None:
            return None, f"补齐任务 {task_id} 不存在"
        if task["任务状态"] != "待交接":
            return None, f"任务当前状态为「{task['任务状态']}」，无需人工确认"
        task["任务状态"] = "待补齐"
        task["更新时间"] = _now()
        return task, "人工确认完成，任务已进入待补齐队列"

    def list_tasks(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        table_name: str | None = None,
        include_backfilled: bool = True,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(TASK_TABLE))
        if keyword:
            rows = [
                row
                for row in rows
                if keyword in str(row.get("补齐任务编号", "")) or keyword in str(row.get("分区标识", ""))
            ]
        if status:
            rows = [row for row in rows if row.get("任务状态") == status]
        if table_name:
            rows = [row for row in rows if row.get("数据表名") == table_name]
        if not include_backfilled:
            rows = [row for row in rows if row.get("任务状态") != "已补齐"]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_task(self, task_id: int) -> dict[str, Any] | None:
        return store.find(TASK_TABLE, task_id)

    # ------------------------------------------------------------- 处理结果与冲突仲裁

    def submit_result(
        self,
        task_id: int,
        *,
        source: str,
        success: bool,
        remark: str | None = None,
        snapshot_checksum: str | None = None,
        operator: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """接收自动补齐或人工处理结果。

        冲突仲裁：人工处理优先于自动补齐；同来源时较晚到达的结果覆盖较早结果。
        被采纳的记录决定分区完整性结论，并覆盖原先检测规则口径；被驳回的记录留痕但不改结论。
        成功结果与完整性结论在同一事务内原子提交。
        """
        task = store.find(TASK_TABLE, task_id)
        if task is None:
            return None, f"补齐任务 {task_id} 不存在"
        if source not in (SOURCE_AUTO, SOURCE_MANUAL):
            return None, f"处理来源「{source}」不支持，仅支持自动补齐/人工处理"
        if snapshot_checksum and snapshot_checksum != task.get("快照校验和"):
            return None, "任务数据快照校验失败：提交内容与交接快照不一致，拒绝写入处理结果"

        existing = [
            row
            for row in store.rows(RESULT_TABLE)
            if int(row.get("补齐任务id", 0)) == task_id
        ]
        manual_adopted = any(
            row.get("处理来源") == SOURCE_MANUAL and row.get("是否采纳") for row in existing
        )
        blocked_reason = ""
        if source == SOURCE_AUTO and manual_adopted:
            # 人工优先：自动结果不能再改变完整性结论，但仍留痕，保证多条冲突记录可核对
            blocked_reason = "该分区已采纳人工处理记录，自动补齐结果被冲突规则驳回，不参与完整性判定"

        with transaction([TASK_TABLE, RESULT_TABLE, BATCH_TABLE]):
            # 同源晚到结果：先到的同源记录失效，新记录接管结论
            for row in existing:
                if row.get("处理来源") == source and row.get("是否采纳"):
                    row["是否采纳"] = False
                    row["驳回原因"] = f"被更晚到达的{source}结果覆盖"
            result = {
                "id": _next_id(RESULT_TABLE),
                "补齐任务id": task_id,
                "补齐任务编号": task["补齐任务编号"],
                "数据表名": task["数据表名"],
                "分区标识": task["分区标识"],
                "处理来源": source,
                "处理结论": "补齐成功" if success else "补齐失败",
                "是否采纳": not blocked_reason,
                "驳回原因": blocked_reason,
                "处理人": operator or ("系统自动" if source == SOURCE_AUTO else "值班人工"),
                "备注": remark or "",
                "到达时间": _now(),
            }
            store.rows(RESULT_TABLE).append(result)

            if blocked_reason:
                return task, blocked_reason
            if success:
                # 原子提交：任务状态与完整性结论一起落库，任一失败整体回滚
                task["任务状态"] = "已补齐"
                task["完整性结论"] = CONCLUSION_COMPLETE
                task["采纳来源"] = source
                task["采纳结果id"] = result["id"]
                task["更新时间"] = _now()
                self._refresh_batch_status(task)
                message = f"{source}结果已采纳，分区补齐完成与完整性结论已原子提交"
            else:
                task["任务状态"] = "补齐失败"
                task["完整性结论"] = CONCLUSION_INCOMPLETE
                task["采纳来源"] = source
                task["采纳结果id"] = result["id"]
                task["更新时间"] = _now()
                message = f"{source}结果已采纳，分区判定为不完整，可进入异常重跑"
        return task, message

    def _refresh_batch_status(self, task: dict[str, Any]) -> None:
        """任务完成后回写检测批次：批次内分区全部补齐时收尾，旧批次同样按最新任务状态重算。"""
        batch_numbers = list(task.get("关联检测批次") or [])
        for batch in store.rows(BATCH_TABLE):
            if batch["检测批次号"] not in batch_numbers:
                continue
            partitions = {item["分区标识"] for item in batch["缺失分区清单"]}
            related = [
                row
                for row in store.rows(TASK_TABLE)
                if row["数据表名"] == batch["数据表名"] and row["分区标识"] in partitions
            ]
            if related and all(row["任务状态"] == "已补齐" for row in related):
                batch["批次状态"] = BATCH_DONE
                batch["收尾时间"] = _now()

    def list_results(self, task_id: int | None = None) -> list[dict[str, Any]]:
        rows = store.rows(RESULT_TABLE)
        if task_id is not None:
            rows = [row for row in rows if int(row.get("补齐任务id", 0)) == task_id]
        return rows

    # ------------------------------------------------- 运行实例分区处理状态（分开呈现）

    def write_runtime_status(
        self,
        task_id: int,
        *,
        runtime_status: str,
        runtime_record_id: str | None = None,
        remark: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """协作业务面回写运行实例中的分区处理状态。

        运行实例分区状态与补齐任务自身状态分字段存储，避免把两边的处理状态混为一谈。
        """
        task = store.find(TASK_TABLE, task_id)
        if task is None:
            return None, f"补齐任务 {task_id} 不存在"
        if runtime_status not in RUNTIME_STATUSES:
            return None, f"运行实例分区状态「{runtime_status}」不合法，允许：{'、'.join(RUNTIME_STATUSES)}"
        task["运行实例分区状态"] = runtime_status
        task["关联运行记录"] = runtime_record_id or task.get("关联运行记录") or ""
        task["运行实例备注"] = remark or task.get("运行实例备注") or ""
        if runtime_status == "运行实例处理中" and task["任务状态"] in ("待补齐",):
            task["任务状态"] = "补齐中"
        task["更新时间"] = _now()
        return task, f"运行实例分区状态已回写为「{runtime_status}」"

    # ------------------------------------------------------------- 任务文件下载

    def export_task_file(self, task_id: int) -> tuple[dict[str, Any] | None, str, str]:
        """生成可下载的补齐任务文件（CSV），导出内容至少包含分区标识和优先级。

        提交后再次查询或重新下载，均从持久化任务快照读取，内容保持完整一致。
        """
        task = store.find(TASK_TABLE, task_id)
        if task is None:
            return None, "", f"补齐任务 {task_id} 不存在"
        if task.get("快照校验和") != checksum_of(task.get("任务快照")):
            return None, "", "任务快照校验失败，导出文件可能不完整，已拒绝下载"
        rows = [
            {
                "补齐任务编号": task["补齐任务编号"],
                "分区标识": task["分区标识"],
                "优先级": task["优先级"],
                "数据表名": task["数据表名"],
                "规则编号": task["规则编号"],
                "任务状态": task["任务状态"],
                "来源检测批次": task["来源检测批次"],
                "快照版本": task["快照版本"],
            }
        ]
        return rows[0], self._to_csv(rows), f"{task['补齐任务编号']}.csv"

    def export_task_bundle(self) -> tuple[str, str]:
        """导出当前全部未补齐任务的任务文件，供批量下载交接。"""
        rows = [
            {
                "补齐任务编号": task["补齐任务编号"],
                "分区标识": task["分区标识"],
                "优先级": task["优先级"],
                "数据表名": task["数据表名"],
                "规则编号": task["规则编号"],
                "任务状态": task["任务状态"],
                "来源检测批次": task["来源检测批次"],
                "快照版本": task["快照版本"],
            }
            for task in store.rows(TASK_TABLE)
            if task.get("快照校验和") == checksum_of(task.get("任务快照"))
        ]
        return self._to_csv(rows), "partition-backfill-tasks.csv"

    @staticmethod
    def _to_csv(rows: list[dict[str, Any]]) -> str:
        def esc(value: Any) -> str:
            text = str(value if value is not None else "")
            if any(ch in text for ch in [",", '"', "\n"]):
                text = '"' + text.replace('"', '""') + '"'
            return text

        header = ["补齐任务编号", "分区标识", "优先级", "数据表名", "规则编号", "任务状态", "来源检测批次", "快照版本"]
        lines = [",".join(header)]
        lines.extend(",".join(esc(row.get(col, "")) for col in header) for row in rows)
        # UTF-8 BOM，方便 Excel 直接打开中文
        return "﻿" + "\n".join(lines) + "\n"

    # ------------------------------------------------------------- 异常重跑对接

    def consume_snapshot(
        self,
        *,
        batch_id: int | None = None,
        rerun_task_id: str | None = None,
        snapshot_checksum: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """异常重跑读取分区补齐任务数据快照，只消费仍未补齐的数据表分区。

        - 可校验调用方持有的快照校验和，快照过期/被篡改则拒绝；
        - 已补齐分区自动过滤并在重跑清单里标记为跳过；
        - 业务结果落到异常重跑清单，并随新的分区检测批次重算。
        """
        candidate_tasks = list(store.rows(TASK_TABLE))
        if batch_id is not None:
            batch = store.find(BATCH_TABLE, batch_id)
            if batch is None:
                return None, f"检测批次 {batch_id} 不存在，无法读取补齐快照"
            partitions = {item["分区标识"] for item in batch["缺失分区清单"]}
            candidate_tasks = [
                task
                for task in candidate_tasks
                if task["数据表名"] == batch["数据表名"] and task["分区标识"] in partitions
            ]
            if snapshot_checksum and snapshot_checksum != batch.get("清单校验和"):
                return None, "补齐任务数据快照校验失败：检测批次清单已重算，请重新拉取快照"

        unbackfilled = [task for task in candidate_tasks if task["任务状态"] != "已补齐"]
        skipped = [
            {"补齐任务编号": task["补齐任务编号"], "分区标识": task["分区标识"], "跳过原因": "分区已补齐"}
            for task in candidate_tasks
            if task["任务状态"] == "已补齐"
        ]
        snapshot = [
            {
                "补齐任务编号": task["补齐任务编号"],
                "数据表名": task["数据表名"],
                "分区标识": task["分区标识"],
                "优先级": task["优先级"],
                "任务状态": task["任务状态"],
                "快照版本": task["快照版本"],
                "快照校验和": task["快照校验和"],
            }
            for task in unbackfilled
        ]
        rerun_no = rerun_task_id or f"RR-PART-{_next_id(RERUN_TABLE):04d}"
        with transaction([RERUN_TABLE]):
            item = {
                "id": _next_id(RERUN_TABLE),
                "重跑清单编号": rerun_no,
                "消费时间": _now(),
                "消费分区数": len(snapshot),
                "跳过已补齐分区数": len(skipped),
                "消费分区": [entry["分区标识"] for entry in snapshot],
                "快照校验和": checksum_of(snapshot),
                "业务结果": "",
                "清单状态": "待重跑",
            }
            store.rows(RERUN_TABLE).append(item)
        return (
            {
                "重跑清单编号": rerun_no,
                "快照校验和": item["快照校验和"],
                "待重跑分区": snapshot,
                "跳过分区": skipped,
            },
            f"快照消费完成：仅纳入仍未补齐分区 {len(snapshot)} 个，跳过已补齐 {len(skipped)} 个",
        )

    def write_rerun_status(
        self,
        task_id: int,
        *,
        runtime_status: str,
        rerun_task_id: str | None = None,
        remark: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """异常重跑协作业务面：把处理状态写入未完成分区，不触碰已补齐分区。"""
        task = store.find(TASK_TABLE, task_id)
        if task is None:
            return None, f"补齐任务 {task_id} 不存在"
        if task["任务状态"] == "已补齐":
            return None, "该分区已补齐，异常重跑不再消费该数据表分区"
        return self.write_runtime_status(
            task_id,
            runtime_status=runtime_status,
            runtime_record_id=rerun_task_id,
            remark=remark,
        )

    def land_rerun_result(
        self,
        rerun_no: str,
        *,
        business_result: str,
        success_partitions: list[str] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """业务结果落到异常重跑清单，并随分区检测重算。

        成功分区通过正常的处理结果通道原子提交完整性结论；清单本身记录业务结果与重算状态。
        """
        item = next(
            (row for row in store.rows(RERUN_TABLE) if row.get("重跑清单编号") == rerun_no),
            None,
        )
        if item is None:
            return None, f"重跑清单 {rerun_no} 不存在"
        with transaction([RERUN_TABLE, TASK_TABLE, RESULT_TABLE, BATCH_TABLE]):
            item["业务结果"] = business_result
            item["结果到达时间"] = _now()
            item["清单状态"] = "已重算"
            landed: list[str] = []
            for partition in success_partitions or []:
                task = next(
                    (
                        row
                        for row in store.rows(TASK_TABLE)
                        if row["分区标识"] == partition and row["任务状态"] != "已补齐"
                    ),
                    None,
                )
                if task is None:
                    continue
                result = {
                    "id": _next_id(RESULT_TABLE),
                    "补齐任务id": task["id"],
                    "补齐任务编号": task["补齐任务编号"],
                    "数据表名": task["数据表名"],
                    "分区标识": task["分区标识"],
                    "处理来源": SOURCE_RERUN,
                    "处理结论": "补齐成功",
                    "是否采纳": True,
                    "驳回原因": "",
                    "处理人": "异常重跑",
                    "备注": business_result,
                    "到达时间": _now(),
                }
                store.rows(RESULT_TABLE).append(result)
                task["任务状态"] = "已补齐"
                task["完整性结论"] = CONCLUSION_COMPLETE
                task["采纳来源"] = SOURCE_RERUN
                task["采纳结果id"] = result["id"]
                task["更新时间"] = _now()
                landed.append(partition)
                self._refresh_batch_status(task)
            item["重算补齐分区"] = landed
        return item, f"业务结果已落入重跑清单，重算补齐分区 {len(item['重算补齐分区'])} 个"

    def list_rerun_items(
        self, *, page: int = 1, size: int = 20
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(RERUN_TABLE))
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total


service = PartitionBackfillService()
