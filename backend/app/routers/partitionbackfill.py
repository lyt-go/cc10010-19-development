"""分区补齐接口：检测批次、受控交接、补齐任务、冲突仲裁与异常重跑对接。

路由层只做参数接收与结果包装，业务判断全部在 PartitionBackfillService。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.partitionbackfill import service

router = APIRouter(prefix="/api/partitionbackfill", tags=["分区补齐"])


# ---------------------------------------------------------------- 检测批次


@router.post("/detections", response_model=ActionResult)
def create_detection(payload: EntryPayload) -> ActionResult:
    """发起一次分区检测：按检测规则重算并固化缺失分区清单快照。"""
    values = payload.values
    try:
        rule_id = int(values.get("规则id") or values.get("rule_id"))
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="缺少规则id，无法定位检测规则")
    missing = values.get("缺失分区清单") or values.get("missing_partitions") or []
    batch, message = service.create_detection(
        rule_id=rule_id,
        table_name=values.get("数据表名"),
        missing_partitions=list(missing),
        remark=payload.remark,
    )
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.get("/detections", response_model=PageResult[dict])
def list_detections(
    keyword: str | None = Query(default=None, description="按检测批次号/数据表名检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """检测批次列表：与运行实例处理状态分开，仅呈现分区检测自身的批次口径。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_detections(keyword=keyword, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/detections/{batch_id}", response_model=dict)
def get_detection(batch_id: int) -> dict:
    batch = service.get_detection(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"检测批次 {batch_id} 不存在")
    return batch


# ---------------------------------------------------------------- 受控交接


@router.post("/detections/{batch_id}/handoff", response_model=ActionResult)
def handoff(batch_id: int) -> ActionResult:
    """缺失分区清单受控交接为可执行补齐任务；快照校验失败或重复检测都有明确口径。"""
    result, message = service.handoff(batch_id)
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


# ---------------------------------------------------------------- 补齐任务


@router.get("/tasks", response_model=PageResult[dict])
def list_tasks(
    keyword: str | None = Query(default=None, description="按补齐任务编号/分区标识检索"),
    status: str | None = Query(default=None, description="待交接、待补齐、补齐中、已补齐、补齐失败"),
    table_name: str | None = Query(default=None, description="按数据表名过滤"),
    include_backfilled: bool = Query(default=True, description="是否包含已补齐分区，异常重跑场景传 false"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_tasks(
        keyword=keyword,
        status=status,
        table_name=table_name,
        include_backfilled=include_backfilled,
        page=page,
        size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/tasks/{task_id}", response_model=dict)
def get_task(task_id: int) -> dict:
    task = service.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"补齐任务 {task_id} 不存在")
    return task


@router.post("/tasks/{task_id}/confirm", response_model=ActionResult)
def confirm_manual(task_id: int) -> ActionResult:
    """人工确认待交接任务，使其进入待补齐队列。"""
    task, message = service.confirm_manual(task_id)
    if task is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=task)


@router.post("/tasks/{task_id}/results", response_model=ActionResult)
def submit_result(task_id: int, payload: EntryPayload) -> ActionResult:
    """提交自动补齐/人工处理结果；冲突仲裁、完整性结论与原子提交由服务层保证。"""
    values = payload.values
    source = str(values.get("处理来源") or "").strip()
    success = bool(values.get("处理成功", True))
    task, message = service.submit_result(
        task_id,
        source=source,
        success=success,
        remark=values.get("备注"),
        snapshot_checksum=values.get("快照校验和"),
        operator=values.get("处理人"),
    )
    if task is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=task)


@router.get("/tasks/{task_id}/results", response_model=dict)
def list_results(task_id: int) -> dict:
    """查看同一分区的多条处理记录及采纳/驳回留痕。"""
    if service.get_task(task_id) is None:
        raise HTTPException(status_code=404, detail=f"补齐任务 {task_id} 不存在")
    items = service.list_results(task_id)
    return {"补齐任务id": task_id, "total": len(items), "items": items}


@router.post("/tasks/{task_id}/runtime-status", response_model=ActionResult)
def write_runtime_status(task_id: int, payload: EntryPayload) -> ActionResult:
    """运行实例侧回写分区处理状态，与补齐任务状态分字段存储。"""
    values = payload.values
    runtime_status = str(values.get("运行实例分区状态") or "").strip()
    task, message = service.write_runtime_status(
        task_id,
        runtime_status=runtime_status,
        runtime_record_id=values.get("关联运行记录"),
        remark=values.get("备注"),
    )
    if task is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=task)


# ---------------------------------------------------------------- 任务文件下载


@router.get("/tasks-file/bundle")
def download_task_bundle() -> Response:
    """批量下载当前全部补齐任务（快照校验通过的任务）。"""
    content, filename = service.export_task_bundle()
    return Response(
        content=content.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/tasks/{task_id}/download")
def download_task_file(task_id: int) -> Response:
    """下载单份补齐任务文件，内容含分区标识与优先级；重复下载结果保持完整一致。"""
    _row, content, message = service.export_task_file(task_id)
    if not content:
        raise HTTPException(status_code=404, detail=message)
    return Response(
        content=content.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="task-{task_id}.csv"'},
    )


# ---------------------------------------------------------------- 异常重跑对接


@router.post("/rerun/consume", response_model=ActionResult)
def consume_snapshot(payload: EntryPayload) -> ActionResult:
    """异常重跑读取补齐任务数据快照，只消费仍未补齐的数据表分区。"""
    values = payload.values
    batch_raw = values.get("检测批次id") or values.get("batch_id")
    try:
        batch_id = int(batch_raw) if batch_raw is not None else None
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="检测批次id 格式不正确")
    result, message = service.consume_snapshot(
        batch_id=batch_id,
        rerun_task_id=values.get("重跑清单编号"),
        snapshot_checksum=values.get("快照校验和"),
    )
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.post("/rerun/tasks/{task_id}/status", response_model=ActionResult)
def write_rerun_status(task_id: int, payload: EntryPayload) -> ActionResult:
    """异常重跑协作业务面：向未完成分区写入处理状态，已补齐分区直接拒绝。"""
    values = payload.values
    task, message = service.write_rerun_status(
        task_id,
        runtime_status=str(values.get("运行实例分区状态") or "").strip(),
        rerun_task_id=values.get("重跑清单编号"),
        remark=values.get("备注"),
    )
    if task is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=task)


@router.post("/rerun/items/{rerun_no}/results", response_model=ActionResult)
def land_rerun_result(rerun_no: str, payload: EntryPayload) -> ActionResult:
    """业务结果落到异常重跑清单并随分区检测重算，成功分区原子提交完整性结论。"""
    values = payload.values
    item, message = service.land_rerun_result(
        rerun_no,
        business_result=str(values.get("业务结果") or "").strip() or "重跑完成",
        success_partitions=list(values.get("成功分区") or []),
    )
    if item is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=item)


@router.get("/rerun/items", response_model=PageResult[dict])
def list_rerun_items(page: int = 1, size: int = 20) -> PageResult[dict]:
    """异常重跑清单：记录每次快照消费的分区范围、业务结果与重算状态。"""
    items, total = service.list_rerun_items(page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)
