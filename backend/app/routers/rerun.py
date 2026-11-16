"""异常重跑接口：维护重跑任务，覆盖提交重跑审批、启动重跑、取消重跑等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.partitionbackfill import service as backfill_service
from app.services.rerun import RerunService

router = APIRouter(prefix="/api/rerun", tags=["异常重跑"])

service = RerunService()

LIST_FIELDS = ["任务编号", "原始运行编号", "重跑区间开始", "重跑区间结束", "重跑原因", "覆盖策略", "依赖级联", "重跑状态"]
STATUSES = ["草稿", "重跑审批中", "执行中", "已完成", "已取消"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号检索"),
    status: str | None = Query(default=None, description="草稿、重跑审批中、执行中、已完成、已取消"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤异常重跑列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条重跑任务，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="重跑任务已登记", entry=entry)


# ------------------------------------------------ 分区补齐对接：字面量路由必须放在 /{entry_id} 之前


@router.get("/partition-tasks", response_model=PageResult[dict])
def list_partition_tasks(
    keyword: str | None = Query(default=None, description="按补齐任务编号/分区标识检索"),
    table_name: str | None = Query(default=None, description="按数据表名过滤"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """异常重跑视角的分区清单：固定排除已补齐分区，只暴露仍未补齐的数据表分区。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = backfill_service.list_tasks(
        keyword=keyword,
        table_name=table_name,
        include_backfilled=False,
        page=page,
        size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/partition-snapshot/consume", response_model=ActionResult)
def consume_partition_snapshot(payload: EntryPayload) -> ActionResult:
    """异常重跑读取分区补齐任务数据快照；只消费仍未补齐的分区，已补齐分区自动跳过。"""
    values = payload.values
    batch_raw = values.get("检测批次id") or values.get("batch_id")
    try:
        batch_id = int(batch_raw) if batch_raw is not None else None
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="检测批次id 格式不正确")
    result, message = backfill_service.consume_snapshot(
        batch_id=batch_id,
        rerun_task_id=values.get("重跑清单编号"),
        snapshot_checksum=values.get("快照校验和"),
    )
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.post("/partition-tasks/{task_id}/status", response_model=ActionResult)
def write_partition_status(task_id: int, payload: EntryPayload) -> ActionResult:
    """协作业务面把异常重跑的处理状态写入未完成分区；已补齐分区不会被重复消费。"""
    values = payload.values
    task, message = backfill_service.write_rerun_status(
        task_id,
        runtime_status=str(values.get("运行实例分区状态") or "").strip(),
        rerun_task_id=values.get("重跑清单编号"),
        remark=values.get("备注"),
    )
    if task is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=task)


@router.post("/partition-rerun/items/{rerun_no}/results", response_model=ActionResult)
def land_partition_rerun_result(rerun_no: str, payload: EntryPayload) -> ActionResult:
    """业务结果落到异常重跑清单并随分区检测重算。"""
    values = payload.values
    item, message = backfill_service.land_rerun_result(
        rerun_no,
        business_result=str(values.get("业务结果") or "").strip() or "重跑完成",
        success_partitions=list(values.get("成功分区") or []),
    )
    if item is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=item)


@router.get("/partition-rerun/items", response_model=PageResult[dict])
def list_partition_rerun_items(page: int = 1, size: int = 20) -> PageResult[dict]:
    """异常重跑清单：快照消费记录、业务结果与随检测重算的状态。"""
    items, total = backfill_service.list_rerun_items(page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出异常重跑清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "rerun", "total": total, "items": items}


# ------------------------------------------------ 单条资源路由放最后，避免吞掉字面量路径


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条重跑任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"重跑任务 {entry_id} 不存在或已归档")
    return entry


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条重跑任务执行提交重跑审批、启动重跑、取消重跑；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
