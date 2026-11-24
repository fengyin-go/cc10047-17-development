"""流式摄入拒收数据批量补交领域服务。

围绕“按分区偏移批量读取 → 重放 → 逐行/整批确认 → 回写质量纠正待办与检查点”
这条链路，把散落在多个分区的拒收行合并成一个可审计的纠正批次：

* 拒收行以 ``partition + offset`` 作为唯一身份，业务键相同也不能互相替代；
* 每个原偏移最多只有一个有效纠正结果，重复重放只累计重复次数；
* 批次携带幂等键，并发提交同一纠正批次只真正生效一次；
* 逐行确认与整批确认都会写入确认边界，检查点按逐行回执重排下一次读取。
"""
from __future__ import annotations

import threading
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

PIPELINE = {
    "id": "pipe-orders",
    "name": "订单流实时摄入",
    "source": "kafka://orders-topic",
    "target_table": "dwd_order_detail",
}

VERSION = {
    "id": 7,
    "label": "v7",
    "table": "dwd_order_detail",
    "pipeline": "订单流实时摄入",
    "committed_at": "2026-11-22T10:30:00Z",
}

# 拒收原因字段：中文标签与数据字段一一对应。
REQUIRED_FIELDS: tuple[tuple[str, str], ...] = (("金额", "amount"), ("日期", "order_date"))
REQUIRED_KEYS = tuple(key for _, key in REQUIRED_FIELDS)
FIELD_LABELS = {key: label for label, key in REQUIRED_FIELDS}
STATUS_PENDING = "待补交"
STATUS_WAITING = "待确认"
STATUS_LANDED = "已入湖"
STATUS_CORRECTED = "已纠正"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, dict, set)):
        return not value
    return False


def _offset_key(partition: str, offset: int) -> str:
    return f"{partition}:{int(offset)}"


class IngestCorrectionService:
    """内存版批量补交服务；生产环境可替换为数据库与分布式幂等表。"""

    VERSION_ID = 7
    PIPELINE_ID = PIPELINE["id"]

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters = {"reject": 0, "correction": 0, "batch": 0}
        self._boundary_seq = 0
        self._reset_locked()

    # ------------------------------------------------------------------
    # 初始化与演示数据
    # ------------------------------------------------------------------

    def _next(self, kind: str) -> int:
        self._counters[kind] += 1
        return self._counters[kind]

    def _reset_locked(self) -> None:
        self._counters = {"reject": 0, "correction": 0, "batch": 0}
        self._boundary_seq = 0
        self._rejects: list[dict[str, Any]] = []
        self._corrections: list[dict[str, Any]] = []
        self._batches: list[dict[str, Any]] = []
        self._idempotency: dict[str, int] = {}
        self._todos_refreshed_at = _now()
        self._scenario_runs = 0

        self._seed_reject(
            partition="p1",
            offset=101,
            business_key="ORD-1001",
            payload={"order_id": "ORD-1001", "order_date": "2026-11-20", "amount": None},
            source_version="v7",
        )
        self._seed_reject(
            partition="p2",
            offset=205,
            business_key="ORD-2001",
            payload={"order_id": "ORD-2001", "amount": 88.5, "order_date": None},
            source_version="v7",
        )

    def _seed_reject(
        self,
        *,
        partition: str,
        offset: int,
        business_key: str,
        payload: dict[str, Any],
        source_version: str,
        kind: str = "原始拒收",
        same_key_of: int | None = None,
    ) -> dict[str, Any]:
        stamp = _now() if self._counters["reject"] else "2026-11-22T10:29:00Z"
        row = {
            "id": self._next("reject"),
            "partition": partition,
            "offset": int(offset),
            "business_key": business_key,
            "kind": kind,
            "same_key_of": same_key_of,
            "source_version": source_version,
            "payload": deepcopy(payload),
            "landed": False,
            "landed_at": None,
            "confirmation_mode": None,
            "duplicate_count": 0,
            "received_at": stamp,
        }
        self._rejects.append(row)
        return row

    def reset(self) -> dict[str, Any]:
        with self._lock:
            self._reset_locked()
            return {
                "ok": True,
                "message": "拒收补交演示数据已重置",
                "rejects": [self._reject_view(row) for row in self._rejects],
            }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def _reject_by_id(self, reject_id: int) -> dict[str, Any] | None:
        for row in self._rejects:
            if int(row["id"]) == int(reject_id):
                return row
        return None

    def _reject_by_offset(self, partition: str, offset: int) -> dict[str, Any] | None:
        for row in self._rejects:
            if row["partition"] == partition and int(row["offset"]) == int(offset):
                return row
        return None

    def _batch_by_id(self, batch_id: int) -> dict[str, Any] | None:
        for batch in self._batches:
            if int(batch["id"]) == int(batch_id):
                return batch
        return None

    def _correction_by_id(self, correction_id: int) -> dict[str, Any] | None:
        for item in self._corrections:
            if int(item["id"]) == int(correction_id):
                return item
        return None

    def _active_correction(self, reject: dict[str, Any]) -> dict[str, Any] | None:
        """一个原偏移只允许存在一个有效纠正结果。"""
        for item in self._corrections:
            if (
                item["valid"]
                and int(item["reject_id"]) == int(reject["id"])
                and item["disposition"] != "已失效"
            ):
                return item
        return None

    def _missing_keys(self, payload: dict[str, Any]) -> list[str]:
        return [key for key in REQUIRED_KEYS if _blank(payload.get(key))]

    def _merged_values(self, correction: dict[str, Any]) -> dict[str, Any]:
        reject = self._reject_by_id(int(correction["reject_id"]))
        merged = deepcopy(reject["payload"]) if reject else {}
        merged.update({key: value for key, value in correction["values"].items() if not _blank(value)})
        return merged

    def _correction_fixed(self, correction: dict[str, Any]) -> bool:
        return not self._missing_keys(self._merged_values(correction))

    def _reject_status(self, reject: dict[str, Any]) -> str:
        if reject["landed"]:
            return STATUS_LANDED
        correction = self._active_correction(reject)
        if correction and self._correction_fixed(correction):
            return STATUS_WAITING
        return STATUS_PENDING

    def _reject_reason(self, reject: dict[str, Any]) -> str:
        if reject["landed"]:
            return "已确认入湖"
        correction = self._active_correction(reject)
        if correction:
            missing = self._missing_keys(self._merged_values(correction))
            if missing:
                return "缺少" + "、".join(FIELD_LABELS[key] for key in missing) + "，等待修正"
            return "已修正，等待逐行或整批确认"
        if reject["kind"] == "同业务键新版本":
            return "同业务键新版本按新偏移登记，等待单独补交，不能替代原拒绝偏移"
        return "尚未进入补交批次，等待批量读取并重放"

    def _reject_view(self, reject: dict[str, Any]) -> dict[str, Any]:
        correction = self._active_correction(reject)
        missing = self._missing_keys(reject["payload"])
        return {
            "id": reject["id"],
            "pipeline_id": PIPELINE["id"],
            "pipeline": PIPELINE["name"],
            "table": VERSION["table"],
            "partition": reject["partition"],
            "offset": reject["offset"],
            "offset_key": _offset_key(reject["partition"], reject["offset"]),
            "business_key": reject["business_key"],
            "kind": reject["kind"],
            "same_key_of": reject["same_key_of"],
            "source_version": reject["source_version"],
            "payload": deepcopy(reject["payload"]),
            "missing_fields": [FIELD_LABELS[key] for key in missing],
            "status": self._reject_status(reject),
            "reason": self._reject_reason(reject),
            "landed": reject["landed"],
            "landed_at": reject["landed_at"],
            "confirmation_mode": reject["confirmation_mode"],
            "duplicate_count": reject["duplicate_count"],
            "correction_id": correction["id"] if correction else None,
            "batch_id": correction["batch_id"] if correction else None,
            "version_id": self.VERSION_ID,
            "received_at": reject["received_at"],
        }

    def _correction_view(self, correction: dict[str, Any]) -> dict[str, Any]:
        reject = self._reject_by_id(int(correction["reject_id"]))
        merged = self._merged_values(correction)
        missing = self._missing_keys(merged)
        return {
            "id": correction["id"],
            "batch_id": correction["batch_id"],
            "reject_id": correction["reject_id"],
            "partition": correction["partition"],
            "offset": correction["offset"],
            "offset_key": _offset_key(correction["partition"], correction["offset"]),
            "business_key": correction["business_key"],
            "kind": reject["kind"] if reject else "原始拒收",
            "missing_fields": [FIELD_LABELS[key] for key in missing],
            "corrected_values": deepcopy(correction["values"]),
            "merged_values": merged,
            "fixed": not missing,
            "valid": correction["valid"],
            "disposition": correction["disposition"],
            "duplicate_count": correction["duplicate_count"],
            "replay_count": correction["replay_count"],
            "confirmation_mode": correction["confirmation_mode"],
            "created_at": correction["created_at"],
            "updated_at": correction["updated_at"],
            "confirmed_at": correction["confirmed_at"],
            "version_id": self.VERSION_ID,
        }

    def _batch_status(self, batch: dict[str, Any]) -> str:
        corrections = [
            self._correction_by_id(int(item_id)) for item_id in batch["items"]
        ]
        corrections = [item for item in corrections if item is not None]
        if corrections and all(item["disposition"] == STATUS_LANDED for item in corrections):
            return "已入湖"
        if any(item["disposition"] == STATUS_LANDED for item in corrections):
            return "部分确认"
        if corrections and all(self._correction_fixed(item) for item in corrections):
            return STATUS_WAITING
        return "待修正"

    def _batch_view(self, batch: dict[str, Any]) -> dict[str, Any]:
        items = [
            self._correction_view(item)
            for item in (self._correction_by_id(int(item_id)) for item_id in batch["items"])
            if item is not None
        ]
        modes = [boundary["mode"] for boundary in batch["boundaries"]]
        strategy = "混合确认" if len(set(modes)) > 1 else (modes[-1] if modes else "未确认")
        return {
            "id": batch["id"],
            "batch_no": batch["batch_no"],
            "idempotency_key": batch["idempotency_key"],
            "pipeline": PIPELINE["name"],
            "table": VERSION["table"],
            "status": self._batch_status(batch),
            "mode": batch["mode"],
            "confirmation_strategy": strategy,
            "items": items,
            "item_count": len(items),
            "landed_count": sum(1 for item in items if item["disposition"] == STATUS_LANDED),
            "pending_count": sum(1 for item in items if item["disposition"] != STATUS_LANDED),
            "duplicate_count": sum(item["duplicate_count"] for item in items),
            "boundaries": deepcopy(batch["boundaries"]),
            "effective_submissions": batch["effective_submissions"],
            "duplicate_submissions": batch["duplicate_submissions"],
            "batch_confirm_attempts": batch["batch_confirm_attempts"],
            "last_conflict_at": batch["last_conflict_at"],
            "created_at": batch["created_at"],
            "updated_at": batch["updated_at"],
        }

    def _todo_view(self, reject: dict[str, Any]) -> dict[str, Any]:
        correction = self._active_correction(reject)
        status = STATUS_CORRECTED if reject["landed"] else self._reject_status(reject)
        return {
            "id": reject["id"],
            "todo_no": f"QC-{int(reject['id']):03d}",
            "pipeline_id": PIPELINE["id"],
            "pipeline": PIPELINE["name"],
            "table": VERSION["table"],
            "partition": reject["partition"],
            "offset": reject["offset"],
            "offset_key": _offset_key(reject["partition"], reject["offset"]),
            "business_key": reject["business_key"],
            "kind": reject["kind"],
            "same_key_of": reject["same_key_of"],
            "missing_fields": [
                FIELD_LABELS[key] for key in self._missing_keys(reject["payload"])
            ],
            "status": status,
            "reason": self._reject_reason(reject),
            "duplicate_count": reject["duplicate_count"],
            "correction_id": correction["id"] if correction else None,
            "batch_id": correction["batch_id"] if correction else None,
            "version_id": self.VERSION_ID,
            "updated_at": (
                correction["updated_at"]
                if correction
                else reject["received_at"]
            ),
        }

    # ------------------------------------------------------------------
    # 批量读取并重放
    # ------------------------------------------------------------------

    def _normalize_reads(self, payload: dict[str, Any]) -> list[tuple[str, int]]:
        reads: list[tuple[str, int]] = []

        def add(partition: str, offset: Any) -> None:
            partition = str(partition or "").strip()
            if not partition or _blank(offset):
                return
            try:
                reads.append((partition, int(offset)))
            except (TypeError, ValueError):
                return

        raw_partitions = payload.get("partitions")
        if isinstance(raw_partitions, dict):
            for partition, raw_offsets in raw_partitions.items():
                offsets = raw_offsets if isinstance(raw_offsets, list) else [raw_offsets]
                for offset in offsets:
                    add(partition, offset)
        elif isinstance(raw_partitions, list):
            for entry in raw_partitions:
                if isinstance(entry, dict):
                    offsets = entry.get("offsets")
                    if offsets is None:
                        offsets = [entry.get("offset")]
                    if not isinstance(offsets, list):
                        offsets = [offsets]
                    for offset in offsets:
                        add(entry.get("partition"), offset)
                elif isinstance(entry, str) and ":" in entry:
                    partition, _, offset = entry.partition(":")
                    add(partition, offset)

        raw_reads = payload.get("reads")
        if isinstance(raw_reads, list):
            for read in raw_reads:
                if not isinstance(read, dict):
                    continue
                raw_offsets = read.get("offsets")
                if raw_offsets is None:
                    raw_offsets = [read.get("offset")]
                if isinstance(raw_offsets, (str, int)):
                    raw_offsets = [raw_offsets]
                for offset in raw_offsets or []:
                    add(read.get("partition"), offset)
        raw_offsets = payload.get("offsets")
        if isinstance(raw_offsets, list):
            for entry in raw_offsets:
                if isinstance(entry, dict):
                    add(entry.get("partition"), entry.get("offset"))
                else:
                    add(payload.get("partition"), entry)
        # 去重并保序
        ordered: list[tuple[str, int]] = []
        seen: set[str] = set()
        for partition, offset in reads:
            key = _offset_key(partition, offset)
            if key not in seen:
                seen.add(key)
                ordered.append((partition, offset))
        return ordered

    def _count_replay_duplicates(
        self, reads: list[tuple[str, int]]
    ) -> tuple[list[dict[str, Any]], int]:
        """重复读取原偏移时累计重复次数，不新建纠正结果。"""
        stamp = _now()
        duplicated: list[dict[str, Any]] = []
        not_found = 0
        for partition, offset in reads:
            reject = self._reject_by_offset(partition, offset)
            if reject is None:
                not_found += 1
                continue
            reject["duplicate_count"] += 1
            correction = self._active_correction(reject)
            if correction is not None:
                correction["duplicate_count"] += 1
                correction["replay_count"] += 1
                correction["updated_at"] = stamp
                duplicated.append(correction)
        return duplicated, not_found

    def replay(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            token = str(
                payload.get("idempotency_key")
                or payload.get("client_token")
                or payload.get("batch_key")
                or ""
            ).strip()
            reads = self._normalize_reads(payload)
            if not reads:
                return {
                    "ok": False,
                    "effective": False,
                    "message": "请至少指定一个分区偏移再执行批量读取",
                    "batch": None,
                }
            # 未显式给幂等键时，用分区偏移集合推导批次身份：同一批偏移只对应一个批次。
            if not token:
                token = "offsets:" + ",".join(
                    sorted(_offset_key(partition, offset) for partition, offset in reads)
                )

            batch = self._batch_by_id(self._idempotency[token]) if token in self._idempotency else None
            if batch is not None:
                batch["duplicate_submissions"] += 1
                batch["updated_at"] = _now()
                duplicated, _ = self._count_replay_duplicates(reads)
                self._todos_refreshed_at = batch["updated_at"]
                return {
                    "ok": True,
                    "effective": False,
                    "duplicate": True,
                    "message": "同一纠正批次已提交，本次未重复生效",
                    "batch": self._batch_view(batch),
                    "duplicate_submissions": batch["duplicate_submissions"],
                    "duplicate_corrections": len(duplicated),
                    "not_found": [
                        {"partition": partition, "offset": offset}
                        for partition, offset in reads
                        if self._reject_by_offset(partition, offset) is None
                    ],
                }

            found: list[dict[str, Any]] = []
            not_found: list[dict[str, Any]] = []
            for partition, offset in reads:
                reject = self._reject_by_offset(partition, offset)
                if reject is None:
                    not_found.append({"partition": partition, "offset": offset})
                elif reject not in found:
                    found.append(reject)

            if not found:
                return {
                    "ok": False,
                    "effective": False,
                    "message": "指定的分区偏移没有拒收数据，未生成纠正批次",
                    "not_found": not_found,
                    "batch": None,
                }

            stamp = _now()
            batch_id = self._next("batch")
            batch: dict[str, Any] = {
                "id": batch_id,
                "batch_no": f"CB-{batch_id:04d}",
                "idempotency_key": token or f"auto-{batch_id}",
                "items": [],
                "mode": None,
                "boundaries": [],
                "effective_submissions": 1,
                "duplicate_submissions": 0,
                "batch_confirm_attempts": 0,
                "last_conflict_at": None,
                "created_at": stamp,
                "updated_at": stamp,
            }

            created: list[dict[str, Any]] = []
            reused: list[dict[str, Any]] = []
            for reject in found:
                existing = self._active_correction(reject)
                if existing is not None:
                    # 同一原偏移已有有效纠正结果：不新建，只累计重复次数。
                    reject["duplicate_count"] += 1
                    existing["duplicate_count"] += 1
                    existing["replay_count"] += 1
                    existing["updated_at"] = stamp
                    batch["items"].append(existing["id"])
                    reused.append(existing)
                    continue

                correction = {
                    "id": self._next("correction"),
                    "batch_id": batch_id,
                    "reject_id": reject["id"],
                    "partition": reject["partition"],
                    "offset": reject["offset"],
                    "business_key": reject["business_key"],
                    "values": {},
                    "valid": True,
                    "disposition": STATUS_WAITING if not self._missing_keys(reject["payload"]) else STATUS_PENDING,
                    "duplicate_count": 0,
                    "replay_count": 1,
                    "confirmation_mode": None,
                    "created_at": stamp,
                    "updated_at": stamp,
                    "confirmed_at": None,
                }
                self._corrections.append(correction)
                batch["items"].append(correction["id"])
                created.append(correction)

            self._batches.append(batch)
            if token:
                self._idempotency[token] = batch_id
            self._todos_refreshed_at = stamp

            message = "批量读取并重放完成"
            if reused:
                message += f"；{len(reused)} 个原偏移复用已有有效纠正结果，仅累计重复次数"
            if not_found:
                message += f"；{len(not_found)} 个偏移未找到拒收数据已跳过"

            return {
                "ok": True,
                "effective": True,
                "duplicate": False,
                "message": message,
                "batch": self._batch_view(batch),
                "created_corrections": len(created),
                "duplicate_corrections": len(reused),
                "not_found": not_found,
                "reads": [
                    {"partition": partition, "offset": offset}
                    for partition, offset in reads
                ],
            }

    # ------------------------------------------------------------------
    # 修正
    # ------------------------------------------------------------------

    def _normalize_values(self, values: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        normalized: dict[str, Any] = {}
        errors: list[str] = []
        for key in REQUIRED_KEYS:
            if key not in values:
                continue
            value = values[key]
            if _blank(value):
                errors.append(f"{FIELD_LABELS[key]}不能为空")
                continue
            if key == "amount":
                try:
                    amount = round(float(value), 2)
                except (TypeError, ValueError):
                    errors.append("金额必须是数字")
                    continue
                if amount < 0:
                    errors.append("金额不能为负数")
                    continue
                normalized[key] = amount
            else:
                text = str(value).strip()
                try:
                    datetime.strptime(text, "%Y-%m-%d")
                except ValueError:
                    errors.append("日期必须是 YYYY-MM-DD 格式")
                    continue
                normalized[key] = text
        return normalized, errors

    def _resolve_corrections(
        self, batch: dict[str, Any], payload: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[str]]:
        batch_items = [
            item
            for item in (self._correction_by_id(int(item_id)) for item_id in batch["items"])
            if item is not None
        ]
        correction_id = payload.get("correction_id")
        if correction_id is not None:
            try:
                target = self._correction_by_id(int(correction_id))
            except (TypeError, ValueError):
                target = None
            if target is None or target not in batch_items:
                return [], [f"纠正结果 {correction_id} 不属于批次 {batch['batch_no']}"]
            return [target], []

        partition = str(payload.get("partition") or "").strip()
        offset = payload.get("offset")
        if partition and offset is not None:
            matches = [
                item
                for item in batch_items
                if item["partition"] == partition and int(item["offset"]) == int(offset)
            ]
            if not matches:
                return [], [f"分区偏移 {partition}:{offset} 不在批次 {batch['batch_no']} 中"]
            return matches, []

        reject_id = payload.get("reject_id")
        if reject_id is not None:
            matches = [item for item in batch_items if int(item["reject_id"]) == int(reject_id)]
            if not matches:
                return [], [f"拒收行 {reject_id} 不在批次 {batch['batch_no']} 中"]
            return matches, []

        return [], ["请指定 correction_id 或分区偏移"]

    def _apply_fix(
        self, correction: dict[str, Any], values: dict[str, Any]
    ) -> tuple[bool, str]:
        if correction["disposition"] == STATUS_LANDED:
            return False, "该行已确认入湖，不能再修改修正值"
        normalized, errors = self._normalize_values(values)
        if errors:
            return False, "；".join(errors)
        if not normalized:
            return False, "没有可保存的修正字段"
        correction["values"].update(normalized)
        correction["updated_at"] = _now()
        if self._correction_fixed(correction):
            correction["disposition"] = STATUS_WAITING
            return True, "修正值已保存，该行已补齐，等待逐行或整批确认"
        missing = self._missing_keys(self._merged_values(correction))
        labels = "、".join(FIELD_LABELS[key] for key in missing)
        return True, f"修正值已保存，仍缺少{labels}"

    def fix_correction(self, batch_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            batch = self._batch_by_id(batch_id)
            if batch is None:
                return {"ok": False, "message": f"纠正批次 {batch_id} 不存在", "batch": None}

            entries = payload.get("items")
            if isinstance(entries, list) and entries:
                results: list[dict[str, Any]] = []
                all_ok = True
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    targets, errors = self._resolve_corrections(batch, entry)
                    if errors:
                        all_ok = False
                        results.append({"ok": False, "message": "；".join(errors)})
                        continue
                    values = entry.get("values") or entry.get("corrected_values") or {}
                    for target in targets:
                        ok, message = self._apply_fix(target, values)
                        all_ok = all_ok and ok
                        results.append({
                            "ok": ok,
                            "message": message,
                            "item": self._correction_view(target),
                        })
                return {
                    "ok": all_ok,
                    "message": "批量修正已保存" if all_ok else "部分修正未保存",
                    "batch": self._batch_view(batch),
                    "results": results,
                }

            corrections, errors = self._resolve_corrections(batch, payload)
            if errors:
                return {
                    "ok": False,
                    "message": "；".join(errors),
                    "batch": self._batch_view(batch),
                }

            values = payload.get("values") or payload.get("corrected_values") or {}
            if not values:
                values = {key: payload[key] for key in REQUIRED_KEYS if key in payload}
            results = []
            all_ok = True
            for correction in corrections:
                ok, message = self._apply_fix(correction, values)
                all_ok = all_ok and ok
                results.append({
                    "ok": ok,
                    "message": message,
                    "item": self._correction_view(correction),
                })
            self._todos_refreshed_at = _now()
            return {
                "ok": all_ok,
                "message": results[0]["message"] if results else "没有可修正的行",
                "batch": self._batch_view(batch),
                "results": results,
                "item": results[0]["item"] if results else None,
            }

    # ------------------------------------------------------------------
    # 逐行确认与整批确认
    # ------------------------------------------------------------------

    def _land_locked(
        self,
        correction: dict[str, Any],
        mode: str,
    ) -> dict[str, Any]:
        if correction["disposition"] == STATUS_LANDED:
            return self._correction_view(correction)
        reject = self._reject_by_id(int(correction["reject_id"]))
        stamp = _now()
        correction["disposition"] = STATUS_LANDED
        correction["confirmation_mode"] = mode
        correction["confirmed_at"] = stamp
        correction["updated_at"] = stamp
        if reject is not None:
            reject["landed"] = True
            reject["landed_at"] = stamp
            reject["confirmation_mode"] = mode
        return self._correction_view(correction)

    def _append_boundary(
        self,
        batch: dict[str, Any],
        mode: str,
        corrections: list[dict[str, Any]],
        note: str,
    ) -> dict[str, Any]:
        self._boundary_seq += 1
        stamp = _now()
        offsets = [
            _offset_key(item["partition"], item["offset"]) for item in corrections
        ]
        boundary = {
            "seq": self._boundary_seq,
            "batch_id": batch["id"],
            "batch_no": batch["batch_no"],
            "mode": mode,
            "offsets": offsets,
            "partition": corrections[0]["partition"] if len(corrections) == 1 else None,
            "offset": corrections[0]["offset"] if len(corrections) == 1 else None,
            "count": len(corrections),
            "note": note,
            "at": stamp,
        }
        batch["boundaries"].append(boundary)
        batch["mode"] = mode
        batch["updated_at"] = stamp
        return boundary

    def _waiting_item(self, correction: dict[str, Any]) -> dict[str, Any]:
        missing = self._missing_keys(self._merged_values(correction))
        return {
            "correction_id": correction["id"],
            "partition": correction["partition"],
            "offset": correction["offset"],
            "offset_key": _offset_key(correction["partition"], correction["offset"]),
            "missing_fields": [FIELD_LABELS[key] for key in missing],
            "disposition": correction["disposition"],
        }

    def confirm_row(self, batch_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            batch = self._batch_by_id(batch_id)
            if batch is None:
                return {"ok": False, "effective": False, "message": f"纠正批次 {batch_id} 不存在"}
            corrections, errors = self._resolve_corrections(batch, payload)
            if errors or not corrections:
                return {
                    "ok": False,
                    "effective": False,
                    "message": "；".join(errors) or "请指定要逐行确认的分区偏移",
                    "batch": self._batch_view(batch),
                }

            correction = corrections[0]
            if correction["disposition"] == STATUS_LANDED:
                return {
                    "ok": True,
                    "effective": False,
                    "duplicate": True,
                    "message": "该行已确认入湖，本次确认未重复生效",
                    "item": self._correction_view(correction),
                    "batch": self._batch_view(batch),
                }
            if not self._correction_fixed(correction):
                return {
                    "ok": False,
                    "effective": False,
                    "message": "该行尚未补齐，不能逐行确认",
                    "waiting_for": [self._waiting_item(correction)],
                    "batch": self._batch_view(batch),
                }

            item = self._land_locked(correction, "逐行确认")
            boundary = self._append_boundary(
                batch,
                "逐行确认",
                [correction],
                f"逐行确认先放行 {_offset_key(correction['partition'], correction['offset'])}",
            )
            self._todos_refreshed_at = _now()
            return {
                "ok": True,
                "effective": True,
                "duplicate": False,
                "message": f"{_offset_key(correction['partition'], correction['offset'])} 已逐行确认入湖",
                "item": item,
                "boundary": boundary,
                "batch": self._batch_view(batch),
                "checkpoint": self.checkpoint(),
                "version": self._version_summary(),
            }

    def confirm_all(self, batch_id: int) -> dict[str, Any]:
        with self._lock:
            batch = self._batch_by_id(batch_id)
            if batch is None:
                return {"ok": False, "effective": False, "message": f"纠正批次 {batch_id} 不存在"}

            corrections = [
                item
                for item in (self._correction_by_id(int(item_id)) for item_id in batch["items"])
                if item is not None
            ]
            pending = [item for item in corrections if item["disposition"] != STATUS_LANDED]
            if not pending:
                return {
                    "ok": True,
                    "effective": False,
                    "duplicate": True,
                    "message": "该批次的纠正结果已全部入湖，本次整批确认未重复生效",
                    "batch": self._batch_view(batch),
                }

            unfixed = [item for item in pending if not self._correction_fixed(item)]
            if unfixed:
                batch["batch_confirm_attempts"] += 1
                batch["last_conflict_at"] = _now()
                waiting = [self._waiting_item(item) for item in unfixed]
                labels = "、".join(item["offset_key"] for item in waiting)
                return {
                    "ok": False,
                    "effective": False,
                    "conflict": True,
                    "message": (
                        f"整批确认要求所有行都补齐后再一起放行，{labels} 仍缺少字段；"
                        "本次未产生确认边界，可改用逐行确认先放行已补齐的行"
                    ),
                    "waiting_for": waiting,
                    "batch": self._batch_view(batch),
                }

            landed: list[dict[str, Any]] = []
            for correction in pending:
                landed.append(self._land_locked(correction, "整批确认"))
            boundary = self._append_boundary(
                batch,
                "整批确认",
                pending,
                f"整批确认 {len(pending)} 行一起放行",
            )
            self._todos_refreshed_at = _now()
            return {
                "ok": True,
                "effective": True,
                "duplicate": False,
                "message": f"整批确认生效，{len(pending)} 行一起入湖",
                "items": landed,
                "boundary": boundary,
                "batch": self._batch_view(batch),
                "checkpoint": self.checkpoint(),
                "version": self._version_summary(),
            }

    # ------------------------------------------------------------------
    # 同业务键新版本
    # ------------------------------------------------------------------

    def add_new_version(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            partition = str(payload.get("partition") or "").strip()
            offset = payload.get("offset")
            if not partition or _blank(offset):
                return {"ok": False, "message": "请指定新版本的分区和偏移"}
            try:
                offset = int(offset)
            except (TypeError, ValueError):
                return {"ok": False, "message": "偏移必须是整数"}

            reject_id = payload.get("reject_id")
            reference = self._reject_by_id(int(reject_id)) if reject_id is not None else None
            business_key = str(payload.get("business_key") or "").strip()
            if not business_key and reference is not None:
                business_key = reference["business_key"]
            if not business_key:
                return {"ok": False, "message": "请指定同业务键"}

            existing = self._reject_by_offset(partition, offset)
            if existing is not None:
                return {
                    "ok": True,
                    "created": False,
                    "replaced": False,
                    "message": f"偏移 {_offset_key(partition, offset)} 已登记，本次未重复创建",
                    "reject": self._reject_view(existing),
                    "original": self._reject_view(reference) if reference else None,
                }

            original = reference
            if original is None:
                candidates = [
                    row
                    for row in self._rejects
                    if row["business_key"] == business_key
                    and _offset_key(row["partition"], row["offset"]) != _offset_key(partition, offset)
                ]
                candidates.sort(key=lambda row: (row["kind"] != "原始拒收", row["id"]))
                original = candidates[0] if candidates else None

            raw_values = payload.get("values") or payload.get("payload") or {}
            if not isinstance(raw_values, dict):
                raw_values = {}
            row_payload = deepcopy(raw_values)
            row_payload.setdefault("order_id", business_key)
            row_payload.setdefault("business_key", business_key)

            source_version = str(payload.get("source_version") or "v8")
            reject = self._seed_reject(
                partition=partition,
                offset=offset,
                business_key=business_key,
                payload=row_payload,
                source_version=source_version,
                kind="同业务键新版本",
                same_key_of=original["id"] if original else None,
            )
            reject["received_at"] = _now()
            self._todos_refreshed_at = _now()

            original_correction = self._active_correction(original) if original else None
            original_view = self._reject_view(original) if original else None
            message = (
                f"同业务键新版本已按 {_offset_key(partition, offset)} 单独登记，"
                "原拒绝偏移的纠正结果保持有效，业务键相同也不能互相替代"
            )
            return {
                "ok": True,
                "created": True,
                "replaced": False,
                "message": message,
                "reject": self._reject_view(reject),
                "original": original_view,
                "original_correction": (
                    self._correction_view(original_correction)
                    if original_correction
                    else None
                ),
                "original_still_valid": (
                    bool(original_correction and original_correction["valid"])
                    if original
                    else None
                ),
            }

    # ------------------------------------------------------------------
    # 质量纠正待办
    # ------------------------------------------------------------------

    def todos(
        self,
        *,
        status: str | None = None,
        keyword: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            rows = [self._todo_view(row) for row in self._rejects]
            if status:
                rows = [row for row in rows if row["status"] == status]
            if keyword:
                needle = keyword.casefold()
                rows = [
                    row
                    for row in rows
                    if needle
                    in " ".join(
                        [
                            row["business_key"],
                            row["partition"],
                            str(row["offset"]),
                            row["todo_no"],
                        ]
                    ).casefold()
                ]
            rows.sort(key=lambda row: (row["partition"], row["offset"]))
            counts = {
                STATUS_PENDING: 0,
                STATUS_WAITING: 0,
                STATUS_CORRECTED: 0,
            }
            for row in rows:
                counts[row["status"]] = counts.get(row["status"], 0) + 1
            return {
                "ok": True,
                "items": rows,
                "total": len(rows),
                "counts": counts,
                "pipeline": PIPELINE["name"],
                "table": VERSION["table"],
                "version_id": self.VERSION_ID,
                "refreshed_at": self._todos_refreshed_at,
            }

    def refresh_todos(self) -> dict[str, Any]:
        with self._lock:
            self._todos_refreshed_at = _now()
            snapshot = self.todos()
            snapshot["message"] = "质量纠正待办已按最新拒收与纠正结果刷新"
            return snapshot

    # ------------------------------------------------------------------
    # 版本详情与检查点恢复
    # ------------------------------------------------------------------

    def _version_summary(self) -> dict[str, Any]:
        accepted = [row for row in self._rejects if row["landed"]]
        rejected = [row for row in self._rejects if not row["landed"]]
        return {
            "id": VERSION["id"],
            "label": VERSION["label"],
            "table": VERSION["table"],
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "accepted_offsets": [
                _offset_key(row["partition"], row["offset"]) for row in accepted
            ],
            "rejected_offsets": [
                _offset_key(row["partition"], row["offset"]) for row in rejected
            ],
            "duplicate_count": sum(row["duplicate_count"] for row in self._rejects),
        }

    def version_detail(self, version_id: int = 7) -> dict[str, Any]:
        with self._lock:
            if int(version_id) != self.VERSION_ID:
                return {
                    "ok": False,
                    "message": f"版本 {version_id} 不存在",
                    "version": None,
                }

            accepted: list[dict[str, Any]] = []
            rejected: list[dict[str, Any]] = []
            for row in sorted(
                self._rejects, key=lambda item: (item["partition"], item["offset"])
            ):
                correction = self._active_correction(row)
                merged = (
                    self._merged_values(correction)
                    if correction
                    else deepcopy(row["payload"])
                )
                base = {
                    "reject_id": row["id"],
                    "partition": row["partition"],
                    "offset": row["offset"],
                    "offset_key": _offset_key(row["partition"], row["offset"]),
                    "business_key": row["business_key"],
                    "kind": row["kind"],
                    "same_key_of": row["same_key_of"],
                    "source_version": row["source_version"],
                    "values": merged,
                    "missing_fields": [
                        FIELD_LABELS[key]
                        for key in self._missing_keys(merged)
                    ],
                    "duplicate_count": row["duplicate_count"],
                    "correction_id": correction["id"] if correction else None,
                    "batch_id": correction["batch_id"] if correction else None,
                    "reason": self._reject_reason(row),
                }
                if row["landed"]:
                    base.update({
                        "disposition": STATUS_LANDED,
                        "confirmation_mode": row["confirmation_mode"],
                        "confirmed_at": row["landed_at"],
                    })
                    accepted.append(base)
                else:
                    base.update({
                        "disposition": self._reject_status(row),
                        "confirmation_mode": None,
                        "confirmed_at": None,
                    })
                    rejected.append(base)

            boundaries: list[dict[str, Any]] = []
            batches: list[dict[str, Any]] = []
            for batch in self._batches:
                batch_corrections = [
                    item
                    for item in (
                        self._correction_by_id(int(item_id))
                        for item_id in batch["items"]
                    )
                    if item is not None
                ]
                if not batch_corrections:
                    continue
                batches.append(self._batch_view(batch))
                boundaries.extend(deepcopy(batch["boundaries"]))
            boundaries.sort(key=lambda item: int(item["seq"]))

            return {
                "ok": True,
                "version": deepcopy(VERSION),
                "pipeline": PIPELINE,
                "accepted": accepted,
                "rejected": rejected,
                "counts": {
                    "accepted": len(accepted),
                    "rejected": len(rejected),
                    "duplicates": sum(row["duplicate_count"] for row in self._rejects),
                    "corrections": len(self._corrections),
                },
                "boundaries": boundaries,
                "batches": batches,
                "checkpoint": self.checkpoint_locked(),
                "refreshed_at": _now(),
            }

    def checkpoint_locked(self) -> dict[str, Any]:
        partitions: list[dict[str, Any]] = []
        names = sorted({row["partition"] for row in self._rejects})
        for partition in names:
            rows = [row for row in self._rejects if row["partition"] == partition]
            landed = sorted(
                [row for row in rows if row["landed"]], key=lambda row: row["offset"]
            )
            pending = sorted(
                [row for row in rows if not row["landed"]], key=lambda row: row["offset"]
            )
            ordered = sorted(rows, key=lambda row: row["offset"])
            if pending:
                next_offset = pending[0]["offset"]
            elif landed:
                next_offset = landed[-1]["offset"] + 1
            else:
                next_offset = 0
            receipts = [
                {
                    "reject_id": row["id"],
                    "offset": row["offset"],
                    "offset_key": _offset_key(partition, row["offset"]),
                    "business_key": row["business_key"],
                    "status": "已确认",
                    "confirmation_mode": row["confirmation_mode"],
                    "confirmed_at": row["landed_at"],
                    "duplicate_count": row["duplicate_count"],
                }
                for row in landed
            ]
            partitions.append({
                "partition": partition,
                "last_confirmed_offset": landed[-1]["offset"] if landed else None,
                "next_offset": next_offset,
                "next_read_offset": next_offset,
                "ordered_offsets": [
                    {
                        "offset": row["offset"],
                        "offset_key": _offset_key(partition, row["offset"]),
                        "business_key": row["business_key"],
                        "kind": row["kind"],
                        "status": "已确认" if row["landed"] else "待读取",
                    }
                    for row in ordered
                ],
                "receipts": receipts,
                "pending_offsets": [row["offset"] for row in pending],
                "duplicate_count": sum(row["duplicate_count"] for row in rows),
            })
        return {
            "ok": True,
            "order_basis": "按逐行回执重排下一次读取",
            "partitions": partitions,
            "refreshed_at": _now(),
        }

    def checkpoint(self) -> dict[str, Any]:
        with self._lock:
            return self.checkpoint_locked()

    # ------------------------------------------------------------------
    # 场景演示
    # ------------------------------------------------------------------

    def run_scenario(
        self,
        *,
        mode: str = "逐行确认",
        new_version_offset: int = 206,
    ) -> dict[str, Any]:
        """按题面顺序跑一遍：p1 缺金额、p2 缺日期，修正 p1 后同业务键新版本先到。"""
        with self._lock:
            self._reset_locked()
            self._scenario_runs += 1
            trace: list[dict[str, Any]] = []

            def record(step: str, result: dict[str, Any], **details: Any) -> None:
                trace.append({
                    "step": step,
                    "at": _now(),
                    "ok": bool(result.get("ok")),
                    "effective": bool(result.get("effective", result.get("ok"))),
                    "message": result.get("message", ""),
                    **details,
                })

            first = self.replay({
                "idempotency_key": "scenario-batch",
                "reads": [
                    {"partition": "p1", "offsets": [101]},
                    {"partition": "p2", "offsets": [205]},
                ],
            })
            record(
                "批量读取并重放 p1、p2",
                first,
                batch_id=(first.get("batch") or {}).get("id"),
                items=[
                    {
                        "offset_key": item["offset_key"],
                        "missing_fields": item["missing_fields"],
                        "correction_id": item["id"],
                    }
                    for item in (first.get("batch") or {}).get("items", [])
                ],
            )
            batch_id = int(first["batch"]["id"])

            concurrent = self.replay({
                "idempotency_key": "scenario-batch",
                "reads": [
                    {"partition": "p1", "offsets": [101]},
                    {"partition": "p2", "offsets": [205]},
                ],
            })
            record(
                "并发提交同一纠正批次",
                concurrent,
                effective_submissions=(concurrent.get("batch") or {}).get("effective_submissions"),
                duplicate_submissions=(concurrent.get("batch") or {}).get("duplicate_submissions"),
            )

            repeated = self.replay({
                "idempotency_key": "scenario-retry",
                "reads": [
                    {"partition": "p1", "offsets": [101]},
                    {"partition": "p2", "offsets": [205]},
                ],
            })
            record(
                "重复读取同一批偏移",
                repeated,
                duplicate_corrections=repeated.get("duplicate_corrections"),
            )

            fix_p1 = self.fix_correction(batch_id, {
                "partition": "p1",
                "offset": 101,
                "values": {"amount": 199.0},
            })
            record("修正 p1 缺少的金额", fix_p1, item=fix_p1.get("item"))

            new_version = self.add_new_version({
                "partition": "p2",
                "offset": new_version_offset,
                "business_key": "ORD-1001",
                "reject_id": 1,
                "source_version": "v8",
                "values": {
                    "order_id": "ORD-1001",
                    "amount": 199.0,
                    "order_date": "2026-11-22",
                },
            })
            record(
                "p2 同业务键新版本先到",
                new_version,
                reject=new_version.get("reject"),
                original=new_version.get("original"),
                original_still_valid=new_version.get("original_still_valid"),
                replaced=new_version.get("replaced"),
            )

            conflict = self.confirm_all(batch_id)
            record(
                "整批确认冲突尝试",
                conflict,
                waiting_for=conflict.get("waiting_for"),
                conflict=conflict.get("conflict"),
            )

            if mode == "整批确认":
                fix_p2 = self.fix_correction(batch_id, {
                    "partition": "p2",
                    "offset": 205,
                    "values": {"order_date": "2026-11-21"},
                })
                record("补齐 p2 缺少的日期", fix_p2, item=fix_p2.get("item"))

                confirmed = self.confirm_all(batch_id)
                record(
                    "整批确认两条一起放行",
                    confirmed,
                    boundary=confirmed.get("boundary"),
                )
                strategy = "整批确认"
            else:
                row_p1 = self.confirm_row(batch_id, {"partition": "p1", "offset": 101})
                record(
                    "逐行确认先放行 p1",
                    row_p1,
                    boundary=row_p1.get("boundary"),
                )

                fix_p2 = self.fix_correction(batch_id, {
                    "partition": "p2",
                    "offset": 205,
                    "values": {"order_date": "2026-11-21"},
                })
                record("补齐 p2 缺少的日期", fix_p2, item=fix_p2.get("item"))

                row_p2 = self.confirm_row(batch_id, {"partition": "p2", "offset": 205})
                record(
                    "逐行确认放行 p2",
                    row_p2,
                    boundary=row_p2.get("boundary"),
                )
                strategy = "逐行确认"

            todos = self.refresh_todos()
            record(
                "刷新质量纠正待办",
                todos,
                counts=todos.get("counts"),
            )

            detail = self.version_detail(self.VERSION_ID)
            checkpoint = self.checkpoint()
            partition_state = {
                item["partition"]: item for item in checkpoint["partitions"]
            }
            rows_verification = []
            for entry in detail["accepted"] + detail["rejected"]:
                if entry["kind"] == "同业务键新版本":
                    continue
                state = partition_state.get(entry["partition"], {})
                rows_verification.append({
                    "partition": entry["partition"],
                    "offset": entry["offset"],
                    "offset_key": entry["offset_key"],
                    "business_key": entry["business_key"],
                    "landed": entry["disposition"] == STATUS_LANDED,
                    "disposition": entry["disposition"],
                    "confirmation_mode": entry["confirmation_mode"],
                    "checkpoint_next_offset": state.get("next_read_offset"),
                    "duplicate_count": entry["duplicate_count"],
                    "values": entry["values"],
                })
            notice = next(
                (
                    item
                    for item in detail["rejected"]
                    if item["kind"] == "同业务键新版本"
                ),
                None,
            )
            return {
                "ok": True,
                "mode": strategy,
                "message": f"场景已按{strategy}跑完，两条原始拒收行均已入湖，同业务键新版本仍按新偏移单独保留",
                "trace": trace,
                "batch": self._batch_view(self._batch_by_id(batch_id)),
                "version": detail,
                "checkpoint": checkpoint,
                "todos": todos,
                "verification": {
                    "accepted_count": detail["counts"]["accepted"],
                    "rejected_count": detail["counts"]["rejected"],
                    "rows": rows_verification,
                    "new_version": (
                        {
                            "partition": notice["partition"],
                            "offset": notice["offset"],
                            "offset_key": notice["offset_key"],
                            "business_key": notice["business_key"],
                            "same_key_of": notice["same_key_of"],
                            "disposition": notice["disposition"],
                            "replaced_original": False,
                        }
                        if notice
                        else None
                    ),
                    "boundaries": detail["boundaries"],
                    "confirmation_strategy": strategy,
                    "batch_effective_submissions": (self._batch_by_id(batch_id) or {}).get(
                        "effective_submissions"
                    ),
                    "duplicate_count": detail["counts"]["duplicates"],
                },
            }


correction_service = IngestCorrectionService()
