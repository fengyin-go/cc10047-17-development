"""拒收数据批量补交接口：批量读取重放、逐行/整批确认、待办与版本详情。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body

from app.services.ingest_correction import IngestCorrectionService, correction_service

router = APIRouter(prefix="/api/ingest-correction", tags=["拒收补交"])


def _service() -> IngestCorrectionService:
    return correction_service


@router.get("/todos")
def list_todos(status: str | None = None, keyword: str | None = None) -> dict[str, Any]:
    """质量纠正待办：每次拒收、补交、确认后都会刷新。"""
    return _service().todos(status=status, keyword=keyword)


@router.post("/todos/refresh")
def refresh_todos() -> dict[str, Any]:
    """刷新质量纠正待办，返回最新待办快照。"""
    return _service().refresh_todos()


@router.post("/replay")
def replay(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """按分区偏移批量读取拒收数据并重放，生成拒收纠正数据记录。"""
    return _service().replay(payload)


@router.post("/batches/{batch_id}/fix")
def fix_correction(
    batch_id: int, payload: dict[str, Any] = Body(default_factory=dict)
) -> dict[str, Any]:
    """为批次内的指定行补齐缺失字段（金额、日期）。"""
    return _service().fix_correction(batch_id, payload)


@router.post("/batches/{batch_id}/confirm-row")
def confirm_row(
    batch_id: int, payload: dict[str, Any] = Body(default_factory=dict)
) -> dict[str, Any]:
    """逐行确认：只要求被确认的那一行已补齐即可放行。"""
    return _service().confirm_row(batch_id, payload)


@router.post("/batches/{batch_id}/confirm-all")
def confirm_all(batch_id: int) -> dict[str, Any]:
    """整批确认：所有行都补齐后才一起放行，否则记录一次冲突。"""
    return _service().confirm_all(batch_id)


@router.post("/rejects/new-version")
def add_new_version(
    payload: dict[str, Any] = Body(default_factory=dict)
) -> dict[str, Any]:
    """登记同业务键的新版本；按新偏移独立建档，不替代原拒绝偏移。"""
    return _service().add_new_version(payload)


@router.get("/versions/{version_id}")
def version_detail(version_id: int) -> dict[str, Any]:
    """版本详情：分栏返回已落湖行、仍被拒绝行，以及确认边界。"""
    return _service().version_detail(version_id)


@router.get("/checkpoint")
def checkpoint() -> dict[str, Any]:
    """检查点恢复：按逐行回执重排每个分区的下一次读取偏移。"""
    return _service().checkpoint()


@router.post("/reset")
def reset() -> dict[str, Any]:
    """重置演示数据，便于重复走一遍题面场景。"""
    return _service().reset()


@router.post("/scenario")
def run_scenario(
    mode: str = "逐行确认", new_version_offset: int = 206
) -> dict[str, Any]:
    """按题面顺序跑完整场景：p1 缺金额、p2 缺日期，修正 p1 后新版本先到。"""
    return _service().run_scenario(mode=mode, new_version_offset=new_version_offset)
