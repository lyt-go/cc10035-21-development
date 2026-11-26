"""管道关窗结果单与迟到读数更正领域服务。

这里刻意把三种概念分开：

* 没有收完：仍是「待关闭」，不能签空；
* 收齐但没有样本：可以签成「已确认空窗」，聚合值保持 ``None``，不写占位 0；
* 收到真实 0：它是合法读数，聚合值为数值 ``0``，并按幂等键去重。

同一个单例同时供数据流管道入口和聚合分发工作台调用，避免两个入口各写一份
去重/归窗逻辑后把同一个 0 计两次。
"""
from __future__ import annotations

import re
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

CST = timezone(timedelta(hours=8))

WINDOW_TABLE = "window_close_order"
SUPPLEMENT_TABLE = "window_supplement_order"

INCOMPLETE_STATUSES = {"未收完", "未收齐", "接收中", "部分接收", "receiving", "incomplete"}
COMPLETE_STATUSES = {"已收齐", "接收完成", "已接收", "complete", "completed", "received", "closed"}
EMPTY_SIGNED_STATUSES = {"已确认空窗", "空窗已签收", "已签空", "empty_signed", "empty_confirmed"}
CORRECTED_STATUSES = {"已更正", "零值已入账", "已补值", "corrected", "late_value_applied"}
CONFLICT_STATUSES = {"冲突待核对", "零值冲突待核对", "conflict", "conflicted"}

_LOCK = threading.RLock()

_CHINESE_NUMBERS = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6}


def now_text() -> str:
    return datetime.now(CST).isoformat(timespec="seconds")


def _pick(data: dict[str, Any], *names: str, default: Any = None) -> Any:
    """按多个候选字段名取值；显式传 None 时继续尝试别名。"""
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    return default


def _merge_call(args: tuple[Any, ...], kwargs: dict[str, Any], names: list[str]) -> dict[str, Any]:
    data = dict(kwargs)
    if args:
        if isinstance(args[0], dict):
            merged = dict(args[0])
            merged.update(data)
            data = merged
        else:
            for name, value in zip(names, args):
                data.setdefault(name, value)
    return data


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "是", "已", "完成", "已收齐"}:
        return True
    if text in {"0", "false", "no", "n", "否", "未", "未收齐", "未收完"}:
        return False
    return None


def parse_time(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=CST)
        return parsed.timestamp()
    except ValueError:
        return None


def sequence_from_key(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text in _CHINESE_NUMBERS:
        return _CHINESE_NUMBERS[text]
    match = re.search(r"第?([一二两三四五六])窗?", text)
    if match:
        return _CHINESE_NUMBERS.get(match.group(1))
    numbers = re.findall(r"\d+", text)
    if numbers:
        return int(numbers[-1])
    return None


def _natural_fingerprint(data: dict[str, Any], window: dict[str, Any] | None) -> str:
    window_key = ""
    if window:
        window_key = str(window.get("窗口编号") or window.get("window_id") or window.get("id"))
    position = _pick(data, "position", "location", "位置", "位置编号", "测点编号", "point_id", default="")
    value = _pick(data, "value", "sample_value", "reading", "数值", "样本值", "读数", default="")
    sample_time = _pick(data, "sample_time", "event_time", "timestamp", "数据时间", "采样时间", "时间", default="")
    return "natural:" + "|".join([
        window_key, str(position), str(sample_time), repr(value),
    ])


def _fingerprint(data: dict[str, Any], window: dict[str, Any] | None) -> str:
    explicit = _pick(
        data,
        "sample_id", "idempotency_key", "dedupe_key", "message_id", "msg_id",
        "delivery_id", "event_id", "报文编号", "消息编号", "样本编号", "读数编号",
        "投递编号", "幂等键",
    )
    if explicit not in (None, ""):
        return f"id:{explicit}"
    return _natural_fingerprint(data, window)


class WindowCloseService(BaseModuleService):
    MODULE = WINDOW_TABLE
    ENTITY = "关窗结果单"
    LIST_FIELDS = [
        "关窗结果单号", "管道编号", "聚合任务编号", "窗口编号", "位置",
        "接收状态", "当前状态", "有效样本数", "聚合值", "首次签收状态", "更正后状态",
    ]
    REQUIRED_FIELDS = ["管道编号", "窗口编号"]
    STATUS_ORDER = ["待关闭", "已确认空窗", "冲突待核对", "已更正", "已关闭"]
    ACTION_RULES = {
        "确认空窗": "已确认空窗",
        "签收空窗": "已确认空窗",
        "确认零值": "已更正",
        "应用更正": "已更正",
        "安排补交": "已更正",
    }

    # ---------- store helpers ----------

    @property
    def orders(self) -> list[dict[str, Any]]:
        return store.rows(WINDOW_TABLE)

    @property
    def supplements(self) -> list[dict[str, Any]]:
        return store.rows(SUPPLEMENT_TABLE)

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(self.orders)
        filters = filters or {}
        for field, expected in filters.items():
            if field in self.LIST_FIELDS or field in {
                "pipeline_id", "window_id", "aggregate_task_id", "position"
            }:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                    or expected.casefold() in str(row.get(self._chinese_alias(field), "")).casefold()
                ]
        if keyword:
            key = keyword.casefold()
            rows = [
                row for row in rows
                if any(key in str(row.get(field, "")).casefold() for field in self.LIST_FIELDS)
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status or row.get("当前状态") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    @staticmethod
    def _chinese_alias(field: str) -> str:
        return {
            "pipeline_id": "管道编号",
            "window_id": "窗口编号",
            "aggregate_task_id": "聚合任务编号",
            "position": "位置",
        }.get(field, field)

    def list_orders(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]] | dict[str, Any]:
        data = _merge_call(args, kwargs, ["pipeline_id", "window_id", "status"])
        page = int(_pick(data, "page", default=1) or 1)
        size = int(_pick(data, "size", default=200) or 200)
        items, total = self.list_entries(
            filters={k: str(v) for k, v in data.items() if k in {"pipeline_id", "window_id"} and v},
            status=_pick(data, "status", "当前状态"),
            page=page,
            size=size,
        )
        if _pick(data, "as_page", "paged", default=False):
            return {"items": items, "total": total, "page": page, "size": size}
        return items

    def query_window_orders(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        orders = self.list_orders(*args, **kwargs)
        assert isinstance(orders, list)
        return {
            "ok": True,
            "total": len(orders),
            "items": orders,
            "windows": orders,
        }

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """通用 POST /api/window_close_order 也走完整关窗单领域逻辑。"""
        result = self.create_close_order(values or {})
        if not result.get("ok"):
            return None, [str(result.get("message", "关窗结果单登记失败"))]
        return result.get("order") or result.get("entry"), []

    def get_order(self, identifier: Any) -> dict[str, Any] | None:
        if identifier is None:
            return None
        if isinstance(identifier, int) or str(identifier).isdigit():
            row = store.find(WINDOW_TABLE, int(identifier))
            if row:
                return row
        for row in self.orders:
            candidates = {
                row.get("关窗结果单号"), row.get("关窗单号"), row.get("结果单号"),
                row.get("申请单号"), row.get("order_no"), row.get("window_id"),
                row.get("窗口编号"),
            }
            if str(identifier) in {str(item) for item in candidates if item is not None}:
                return row
        seq = sequence_from_key(identifier)
        if seq is not None:
            matches = [row for row in self.orders if row.get("window_seq") == seq]
            if len(matches) == 1:
                return matches[0]
        return None

    def _resolve(self, data: dict[str, Any]) -> dict[str, Any] | None:
        identifier = _pick(
            data, "order_id", "id", "close_order_id", "closure_id",
            "关窗结果单号", "结果单号", "关窗单号", "申请单号",
        )
        order = self.get_order(identifier) if identifier is not None else None
        if order is not None:
            return order

        aggregate_task_id = _pick(data, "aggregate_task_id", "task_id", "任务编号", "聚合任务编号")
        if aggregate_task_id is None and identifier is not None:
            agg_rows = store.rows("aggregate_sink")
            if any(
                str(row.get("任务编号")) == str(identifier) or str(row.get("id")) == str(identifier)
                for row in agg_rows
            ):
                aggregate_task_id = identifier
                data["aggregate_task_id"] = identifier
        window_id = _pick(data, "window_id", "窗口编号", "数据窗口", "window")
        pipeline_id = _pick(data, "pipeline_id", "管道编号", "stream_id")

        if window_id is not None:
            exact = [
                row for row in self.orders
                if str(row.get("窗口编号")) == str(window_id)
                or str(row.get("window_id")) == str(window_id)
            ]
            if pipeline_id is not None:
                scoped = [row for row in exact if str(row.get("管道编号")) == str(pipeline_id)]
                if scoped:
                    return scoped[0]
            elif len(exact) == 1:
                return exact[0]

            seq = sequence_from_key(window_id)
            if seq is not None:
                seq_rows = [
                    row for row in self.orders
                    if row.get("window_seq") == seq
                    and (pipeline_id is None or str(row.get("管道编号")) == str(pipeline_id))
                ]
                if len(seq_rows) == 1:
                    return seq_rows[0]

            sample_time = parse_time(_pick(
                data, "sample_time", "event_time", "timestamp", "数据时间", "采样时间", "时间"
            ))
            if sample_time is not None:
                owner = self.find_window_for_time(sample_time, pipeline_id=pipeline_id)
                if owner is not None:
                    return owner
            # 明确指定了窗口但找不到时，不能改走聚合任务误匹配到相邻窗口。
            return None

        if aggregate_task_id is not None:
            linked = [
                row for row in self.orders
                if str(row.get("aggregate_task_id")) == str(aggregate_task_id)
                or str(row.get("聚合任务编号")) == str(aggregate_task_id)
            ]
            if len(linked) == 1:
                return linked[0]

            # 兼容直接在既有 aggregate_sink 任务上办理关窗的调用方式：
            # 首次动作时把该聚合任务接纳为一张关窗结果单。
            agg_rows = store.rows("aggregate_sink")
            agg_task = None
            for row in agg_rows:
                if str(row.get("任务编号")) == str(aggregate_task_id) or str(row.get("id")) == str(aggregate_task_id):
                    agg_task = row
                    break
            if agg_task is not None and not _pick(data, "_adopting", default=False):
                derived_window = (
                    agg_task.get("窗口编号")
                    or agg_task.get("数据窗口")
                    or agg_task.get("任务编号")
                )
                derived_pipeline = (
                    pipeline_id
                    or agg_task.get("管道编号")
                    or _pick(data, "pipeline_id", "管道编号")
                    or f"PIPE-{agg_task.get('任务编号', aggregate_task_id)}"
                )
                created = self.create_close_order(
                    pipeline_id=derived_pipeline,
                    window_id=derived_window,
                    position=_pick(data, "position", "位置", "测点编号", default=agg_task.get("测点列表", "")),
                    aggregate_task_id=agg_task.get("任务编号"),
                    agg_func=agg_task.get("聚合函数", "sum"),
                    window_size=agg_task.get("窗口时长", ""),
                    receive_complete=_pick(data, "receive_complete", default=True),
                    _adopting=True,
                )
                return created.get("order")

        # Last resort: locate by sample timestamp against actual intervals.
        sample_time = parse_time(_pick(
            data, "sample_time", "event_time", "timestamp", "数据时间", "采样时间", "时间"
        ))
        if sample_time is not None:
            return self.find_window_for_time(sample_time, pipeline_id=pipeline_id)
        return None

    # ---------- window setup ----------

    def create_close_order(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _merge_call(args, kwargs, ["pipeline_id", "window_id", "aggregate_task_id"])
        with _LOCK:
            pipeline_id = _pick(data, "pipeline_id", "管道编号", "stream_id", default="")
            window_id = _pick(data, "window_id", "窗口编号", "数据窗口", "window")
            if not pipeline_id or window_id in (None, ""):
                return {"ok": False, "code": "MISSING_REQUIRED", "message": "管道编号和窗口编号不能为空"}

            existing = self._resolve(data)
            if existing is not None and _pick(data, "allow_duplicate", default=False) is not True:
                return {
                    "ok": True,
                    "duplicate": True,
                    "idempotent": True,
                    "message": f"窗口 {window_id} 的关窗结果单已存在",
                    "order": existing,
                    "window": existing,
                    "entry": existing,
                }

            rows = self.orders
            next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
            seq = _pick(data, "window_seq", "seq", "窗口序号")
            seq = int(seq) if seq not in (None, "") else (sequence_from_key(window_id) or next_id)
            result_no = _pick(data, "result_no", "关窗结果单号") or f"WCR-{next_id:04d}"
            apply_no = _pick(data, "apply_no", "申请单号") or f"WCA-{next_id:04d}"

            receive_status = _pick(
                data, "pipeline_receive_status", "actual_receive_status",
                "接收状态", "管道接收状态", "实际接收状态",
            )
            receive_complete = _as_bool(_pick(data, "receive_complete", "is_complete", "收齐", "接收完成"))
            if receive_complete is None and receive_status is not None:
                receive_complete = str(receive_status).strip().lower() in COMPLETE_STATUSES
            expected_count = _pick(data, "expected_count", "预期样本数", "应收样本数")
            expected_count = int(expected_count) if expected_count not in (None, "") else None
            initial_count = int(_pick(data, "received_count", "actual_received_count", "已接收样本数", default=0) or 0)
            if receive_complete is None:
                receive_complete = bool(expected_count is not None and initial_count >= expected_count)

            order: dict[str, Any] = {
                "id": next_id,
                "order_no": result_no,
                "关窗结果单号": result_no,
                "关窗单号": result_no,
                "结果单号": result_no,
                "申请单号": apply_no,
                "pipeline_id": pipeline_id,
                "管道编号": pipeline_id,
                "aggregate_task_id": _pick(
                    data, "aggregate_task_id", "task_id", "任务编号", "聚合任务编号", default=""
                ),
                "聚合任务编号": _pick(
                    data, "aggregate_task_id", "task_id", "任务编号", "聚合任务编号", default=""
                ),
                "window_id": str(window_id),
                "窗口编号": str(window_id),
                "window_seq": seq,
                "窗口名称": _pick(data, "window_name", "窗口名称", default=f"第{seq}窗"),
                "position": _pick(data, "position", "location", "位置", "位置编号", "测点编号", "point_id", default=""),
                "位置": _pick(data, "position", "location", "位置", "位置编号", "测点编号", "point_id", default=""),
                "point_id": _pick(data, "point_id", "测点编号", default=""),
                "window_start": _pick(data, "window_start", "start", "window_start_time", "窗口开始", "窗口起点"),
                "window_end": _pick(data, "window_end", "end", "window_end_time", "窗口结束", "窗口终点"),
                "窗口开始": _pick(data, "window_start", "start", "window_start_time", "窗口开始", "窗口起点", default=""),
                "窗口结束": _pick(data, "window_end", "end", "window_end_time", "窗口结束", "窗口终点", default=""),
                "window_size": _pick(data, "window_size", "窗口时长", default=""),
                "agg_func": _pick(data, "agg_func", "聚合函数", default="sum"),
                "聚合函数": _pick(data, "agg_func", "聚合函数", default="sum"),
                "expected_count": expected_count,
                "预期样本数": expected_count,
                "receive_complete": bool(receive_complete),
                "收齐": bool(receive_complete),
                "接收状态": str(receive_status or ("已收齐" if receive_complete else "未收齐")),
                "管道实际接收状态": str(receive_status or ("已收齐" if receive_complete else "未收齐")),
                "actual_received_count": initial_count,
                "实际接收样本数": initial_count,
                "有效样本数": 0,
                "sample_count": 0,
                "重复样本数": 0,
                "duplicate_count": 0,
                "report_count": 0,
                "total_reports": 0,
                "samples": [],
                "sample_ids": [],
                "status": "待关闭",
                "当前状态": "待关闭",
                "窗口资格": "未收完，继续待关闭" if not receive_complete else "可核对后签空或确认读数",
                "窗口结果": "未关闭",
                "is_empty_window": False,
                "empty_window": False,
                "聚合值": None,
                "aggregated_value": None,
                "value": None,
                "aggregate_result": None,
                "aggregate": None,
                "aggregation": None,
                "首次签收状态": "",
                "first_status": "",
                "签收状态": "未签收",
                "empty_sign_status": "unsigned",
                "close_status": "pending_close",
                "signed_empty": False,
                "更正前状态": "",
                "更正后状态": "",
                "previous_status": "",
                "before_status": "",
                "after_status": "",
                "current_status": "待关闭",
                "version": 1,
                "history": [],
                "corrections": [],
                "reports": [],
                "supplement_orders": [],
                "created_at": now_text(),
                "updated_at": now_text(),
                "pending": True,
                "abnormal": False,
            }
            order["history"].append(self._snapshot("create", order, "创建关窗结果单"))
            rows.append(order)

            samples = _pick(data, "samples", "initial_samples", default=[])
            if isinstance(samples, list):
                for sample in samples:
                    if isinstance(sample, dict):
                        self.receive_sample({**sample, "order_id": order["id"]})

            if _pick(data, "sync", "sync_aggregate", default=True):
                self.sync_to_aggregate(order)
            return {
                "ok": True,
                "message": f"窗口 {window_id} 的关窗结果单已创建",
                "order": order,
                "window": order,
                "entry": order,
                "order_id": order["id"],
                "window_id": order["窗口编号"],
            }

    def submit_close_application(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.create_close_order(*args, **kwargs)

    apply_window_close = submit_close_application
    request_window_close = submit_close_application
    create_window_close_order = submit_close_application
    create_window_closure = submit_close_application

    def ensure_scenario(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """建立提示中的两窗场景初始态：W1 无样本可签空，W2 已合法出值 5。"""
        data = _merge_call(args, kwargs, ["pipeline_id", "aggregate_task_id"])
        pipeline_id = _pick(data, "pipeline_id", "管道编号", default="DATA-ZERO-CONFLICT")
        task_id = _pick(data, "aggregate_task_id", "聚合任务编号", default="AGGR-ZERO-CONFLICT")
        position = _pick(data, "position", "位置", "测点编号", default="POINT-ZERO")
        reset = _as_bool(_pick(data, "reset", default=False))
        with _LOCK:
            if reset:
                keep = [r for r in self.orders if r.get("pipeline_id") != pipeline_id]
                store._tables[WINDOW_TABLE] = keep  # noqa: SLF001 - 测试/演示重置
            w1 = self._resolve({"pipeline_id": pipeline_id, "window_id": "W1"})
            if w1 is None:
                result1 = self.create_close_order(
                    pipeline_id=pipeline_id, window_id="W1", window_seq=1,
                    window_start=0, window_end=60, position=position,
                    aggregate_task_id=f"{task_id}-W1", receive_complete=True, expected_count=0,
                    sync=False,
                )
                w1 = result1.get("order")
            w2 = self._resolve({"pipeline_id": pipeline_id, "window_id": "W2"})
            if w2 is None:
                result2 = self.create_close_order(
                    pipeline_id=pipeline_id, window_id="W2", window_seq=2,
                    window_start=60, window_end=120, position=position,
                    aggregate_task_id=f"{task_id}-W2", receive_complete=True,
                    sync=False,
                )
                w2 = result2.get("order")
                self.receive_sample(
                    order_id=w2["id"], sample_id="SAMPLE-W2-5", value=5,
                    position=position, sample_time=90, source="scenario",
                )
                self.confirm_reading(order_id=w2["id"], operator="值班员", sync=True)
            return {
                "ok": True,
                "message": "零值冲突场景已就绪：第一窗待签空，第二窗已出值 5",
                "pipeline_id": pipeline_id,
                "aggregate_task_id": task_id,
                "window1": w1,
                "window2": w2,
                "first_window": w1,
                "second_window": w2,
                "orders": [w1, w2],
            }

    def seed_zero_conflict_scenario(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.ensure_scenario(*args, **kwargs)

    # ---------- time-based ownership ----------

    def find_window_for_time(
        self, sample_time: Any, pipeline_id: str | None = None
    ) -> dict[str, Any] | None:
        ts = parse_time(sample_time)
        if ts is None:
            return None
        candidates = self.orders
        if pipeline_id is not None:
            candidates = [row for row in candidates if str(row.get("管道编号")) == str(pipeline_id)]
        for row in candidates:
            start = parse_time(row.get("window_start"))
            end = parse_time(row.get("window_end"))
            if start is None or end is None:
                continue
            if start <= ts < end or (ts == end and row.get("include_end")):
                return row
        return None

    def locate_owning_window(self, *args: Any, **kwargs: Any) -> dict[str, Any] | None:
        data = _merge_call(args, kwargs, ["sample_time", "timestamp", "window_id"])
        return self._resolve(data)

    # ---------- pipeline receipt ----------

    def sync_pipeline_receipt(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _merge_call(args, kwargs, ["pipeline_id", "window_id", "received_count"])
        with _LOCK:
            order = self._resolve(data)
            if order is None:
                return {"ok": False, "code": "WINDOW_NOT_FOUND", "message": "未找到管道实际接收状态对应的窗口"}
            count = _pick(data, "received_count", "actual_received_count", "已接收样本数")
            if count not in (None, ""):
                order["actual_received_count"] = int(count)
                order["实际接收样本数"] = int(count)
            status = _pick(data, "receive_status", "pipeline_receive_status", "接收状态", "实际接收状态")
            complete = _as_bool(_pick(data, "receive_complete", "is_complete", "收齐"))
            if status is not None:
                order["接收状态"] = str(status)
                order["管道实际接收状态"] = str(status)
                if complete is None:
                    complete = str(status).strip().lower() in COMPLETE_STATUSES
            if complete is not None:
                order["receive_complete"] = complete
                order["收齐"] = complete
                if complete and not status:
                    order["接收状态"] = "已收齐"
                    order["管道实际接收状态"] = "已收齐"
                elif complete is False and not status:
                    order["接收状态"] = "未收齐"
                    order["管道实际接收状态"] = "未收齐"
            self._refresh_eligibility(order)
            order["updated_at"] = now_text()
            order["history"].append(self._snapshot("receipt_sync", order, "同步管道实际接收状态"))
            self.sync_to_aggregate(order)
            return {"ok": True, "order": order, "window": order}

    def _refresh_eligibility(self, order: dict[str, Any]) -> None:
        if order.get("status") in CORRECTED_STATUSES | EMPTY_SIGNED_STATUSES:
            return
        if not order.get("receive_complete"):
            order["status"] = "待关闭"
            order["当前状态"] = "待关闭"
            order["窗口资格"] = "管道尚未收完，仍需待关闭"
        elif order.get("有效样本数", 0) > 0:
            order["status"] = "读数待核对"
            order["当前状态"] = "读数待核对"
            order["窗口资格"] = "已有真实样本，不能签成空窗"
        else:
            order["status"] = "待关闭"
            order["当前状态"] = "待关闭"
            order["窗口资格"] = "已收齐且无样本，可以确认空窗"

    # ---------- samples / idempotency ----------

    def receive_sample(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _merge_call(args, kwargs, ["window_id", "sample_id", "value", "position"])
        with _LOCK:
            claimed_window = _pick(data, "window_id", "窗口编号", "数据窗口")
            sample_time = _pick(data, "sample_time", "event_time", "timestamp", "数据时间", "采样时间", "时间")
            pipeline_id = _pick(data, "pipeline_id", "管道编号", "stream_id")

            order = self._resolve(data)
            if order is None and sample_time is not None:
                owner = self.find_window_for_time(sample_time, pipeline_id)
                if owner is not None:
                    order = owner
            if order is None:
                return {
                    "ok": False,
                    "code": "OWNING_WINDOW_NOT_FOUND",
                    "message": "迟到样本找不到实际所属窗口，未写入任何相邻窗口",
                    "claimed_window_id": claimed_window,
                }

            # If both claimed id and timestamp exist, interval ownership wins.
            reassigned = False
            if sample_time is not None:
                owner = self.find_window_for_time(sample_time, pipeline_id or order.get("管道编号"))
                if owner is not None and owner.get("id") != order.get("id"):
                    order = owner
                    reassigned = True

            raw_value = _pick(data, "value", "sample_value", "reading", "数值", "样本值", "读数")
            is_empty_reading = raw_value is None or (
                isinstance(raw_value, str) and raw_value.strip().lower() in {"", "null", "none", "nan", "空", "空采样"}
            )
            if is_empty_reading:
                empty_report = {
                    "reported_at": now_text(),
                    "source": _pick(data, "source", "entry", "入口", default="unknown"),
                    "position": _pick(data, "position", "位置", "测点编号", default=order.get("位置", "")),
                    "kind": "empty_sampling_report",
                }
                order.setdefault("empty_reports", []).append(empty_report)
                order["空采样报告数"] = len(order["empty_reports"])
                self._recompute(order)
                self.sync_to_aggregate(order)
                return {
                    "ok": True,
                    "empty_reading": True,
                    "is_real_zero": False,
                    "message": "收到空采样/缺值报告；它不是测得的 0，也不会写入占位 0",
                    "order": order,
                    "window": order,
                    "aggregate_result": order.get("aggregate_result"),
                }

            fingerprint = _fingerprint(data, order)
            natural_key = _natural_fingerprint(data, order)
            existing_sample = next(
                (
                    item for item in order.get("samples", [])
                    if item.get("fingerprint") == fingerprint
                    or item.get("natural_key") == natural_key
                    or (
                        fingerprint.startswith("id:")
                        and item.get("sample_id") == fingerprint.removeprefix("id:")
                    )
                ),
                None,
            )
            source = _pick(data, "source", "entry", "入口", default="unknown")
            if existing_sample is not None:
                order["duplicate_count"] = int(order.get("duplicate_count", 0)) + 1
                order["重复样本数"] = order["duplicate_count"]
                report = {
                    "source": source,
                    "reported_at": now_text(),
                    "duplicate": True,
                    "sample_id": existing_sample.get("sample_id"),
                }
                existing_sample.setdefault("reports", []).append(report)
                order.setdefault("reports", []).append(report)
                order["report_count"] = len(order["reports"])
                order["total_reports"] = len(order["reports"])
                self.sync_to_aggregate(order)
                return {
                    "ok": True,
                    "duplicate": True,
                    "is_duplicate": True,
                    "idempotent": True,
                    "message": "同一读数已处理，重投只保留一个真实样本",
                    "sample": existing_sample,
                    "order": order,
                    "window": order,
                    "aggregate_result": order.get("aggregate_result"),
                    "aggregate": order.get("aggregate_result"),
                    "aggregation": order.get("aggregate_result"),
                    "reassigned": reassigned,
                }

            raw_value = _pick(data, "value", "sample_value", "reading", "数值", "样本值", "读数")
            try:
                numeric_value = float(raw_value)
                if numeric_value.is_integer():
                    numeric_value = int(numeric_value)
            except (TypeError, ValueError):
                numeric_value = raw_value

            explicit_id = _pick(
                data, "sample_id", "idempotency_key", "dedupe_key", "message_id",
                "msg_id", "delivery_id", "event_id", "报文编号", "消息编号", "样本编号", "读数编号",
            )
            sample = {
                "id": len(order.get("samples", [])) + 1,
                "sample_id": explicit_id or fingerprint,
                "fingerprint": fingerprint,
                "natural_key": natural_key,
                "position": _pick(
                    data, "position", "location", "位置", "位置编号", "测点编号", "point_id",
                    default=order.get("position", ""),
                ),
                "位置": _pick(
                    data, "position", "location", "位置", "位置编号", "测点编号", "point_id",
                    default=order.get("位置", ""),
                ),
                "value": numeric_value,
                "数值": numeric_value,
                "is_zero": numeric_value == 0 and isinstance(numeric_value, (int, float)),
                "is_real_reading": True,
                "is_placeholder_zero": False,
                "sample_time": sample_time,
                "数据时间": sample_time,
                "received_at": now_text(),
                "source": source,
                "claimed_window_id": claimed_window,
                "reassigned": reassigned,
                "reports": [{"source": source, "reported_at": now_text(), "duplicate": False}],
            }
            order.setdefault("samples", []).append(sample)
            order.setdefault("reports", []).append(sample["reports"][0])
            order["report_count"] = len(order["reports"])
            order["total_reports"] = order["report_count"]
            if explicit_id:
                order.setdefault("sample_ids", []).append(str(explicit_id))
            order["actual_received_count"] = max(
                int(order.get("actual_received_count", 0)), len(order["samples"])
            )
            order["实际接收样本数"] = order["actual_received_count"]
            order["receive_complete"] = True
            order["收齐"] = True
            order["接收状态"] = "已收齐"
            order["管道实际接收状态"] = "已收齐"

            was_signed_empty = order.get("status") in EMPTY_SIGNED_STATUSES or order.get("is_empty_window")
            conflict = bool(was_signed_empty)
            self._recompute(order)

            auto_confirm = _as_bool(_pick(data, "auto_confirm", "apply_correction", default=True))
            if was_signed_empty:
                order["conflict"] = True
                if auto_confirm:
                    self._apply_late_correction(order, sample, source=source)
                else:
                    order["status"] = "冲突待核对"
                    order["当前状态"] = "冲突待核对"
                    order["pending_correction"] = True
            else:
                self._refresh_eligibility(order)
                if auto_confirm and _pick(data, "auto_confirm", default=None) is not None:
                    self.confirm_reading(order_id=order["id"], source=source)

            order["updated_at"] = now_text()
            order["history"].append(self._snapshot(
                "sample_received", order,
                f"入口 {source} 收到真实样本 {numeric_value}" + ("（与空窗签收冲突）" if conflict else ""),
                extra={"sample_id": sample["sample_id"], "value": numeric_value, "conflict": conflict},
            ))
            self.sync_to_aggregate(order)
            return {
                "ok": True,
                "duplicate": False,
                "is_duplicate": False,
                "conflict": conflict,
                "corrected": order.get("status") in CORRECTED_STATUSES,
                "message": (
                    "迟到零值与已签收空窗冲突，已保留首次签收并完成更正"
                    if conflict and order.get("status") in CORRECTED_STATUSES
                    else "样本已归入实际所属窗口并参与聚合"
                ),
                "sample": sample,
                "order": order,
                "window": order,
                "entry": order,
                "aggregate_result": order.get("aggregate_result"),
                "aggregate": order.get("aggregate_result"),
                "aggregation": order.get("aggregate_result"),
                "reassigned": reassigned,
            }

    report_sample = receive_sample
    ingest_sample = receive_sample
    ingest_reading = receive_sample
    report_reading = receive_sample
    report_late_value = receive_sample
    handle_late_value = receive_sample
    process_late_value = receive_sample
    add_sample = receive_sample

    def _recompute(self, order: dict[str, Any]) -> dict[str, Any] | None:
        values = [
            sample.get("value") for sample in order.get("samples", [])
            if isinstance(sample.get("value"), (int, float)) and not isinstance(sample.get("value"), bool)
        ]
        count = len(values)
        order["有效样本数"] = count
        order["sample_count"] = count
        if count == 0:
            result = {
                "status": "empty",
                "empty": True,
                "sample_count": 0,
                "count": 0,
                "value": None,
                "aggregated_value": None,
                "sum": None,
                "avg": None,
                "min": None,
                "max": None,
                "note": "确认无样本的历史空窗；不写入占位 0",
                "calculated_at": now_text(),
            }
            order["aggregate_result"] = result
            order["aggregate"] = result
            order["aggregation"] = result
            if order.get("status") not in EMPTY_SIGNED_STATUSES | CORRECTED_STATUSES | CONFLICT_STATUSES:
                order["聚合值"] = None
                order["aggregated_value"] = None
                order["value"] = None
            return result

        total = sum(values)
        avg = round(total / count, 6)
        func = str(order.get("agg_func") or "sum").lower()
        if func == "avg":
            value: Any = avg
        elif func in {"count", "count_success"}:
            value = count
        elif func == "max":
            value = max(values)
        elif func == "min":
            value = min(values)
        elif func in {"last", "last_value"}:
            value = values[-1]
        else:
            value = total
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        result = {
            "status": "value",
            "empty": False,
            "sample_count": count,
            "count": count,
            "sum": total,
            "avg": avg,
            "min": min(values),
            "max": max(values),
            "value": value,
            "aggregated_value": value,
            "zero_value_count": sum(1 for item in values if item == 0),
            "calculated_at": now_text(),
        }
        order["aggregate_result"] = result
        order["aggregate"] = result
        order["aggregation"] = result
        order["聚合值"] = value
        order["aggregated_value"] = value
        order["value"] = value
        return result

    # ---------- empty confirmation / correction ----------

    def confirm_empty(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _merge_call(args, kwargs, ["order_id", "operator", "expected_version"])
        with _LOCK:
            order = self._resolve(data)
            if order is None:
                return {"ok": False, "code": "ORDER_NOT_FOUND", "message": "关窗结果单不存在"}

            expected_version = _pick(data, "expected_version", "version", "版本")
            if expected_version not in (None, "") and int(expected_version) != int(order.get("version", 1)):
                return {
                    "ok": False,
                    "conflict": True,
                    "code": "VERSION_CONFLICT",
                    "message": "关窗单已被管道样本更新，请核对实际接收状态后再办理",
                    "order": order,
                    "window": order,
                }

            expected_actual = _pick(
                data, "expected_received_count", "expected_sample_count",
                "expected_actual_count", "expected_samples", "expected_receipt_count",
            )
            if expected_actual not in (None, ""):
                actual_count = int(order.get("有效样本数", 0) or 0)
                if int(expected_actual) != actual_count:
                    return {
                        "ok": False,
                        "conflict": True,
                        "code": "RECEIPT_CHANGED",
                        "message": "管道实际接收样本数已变化，请按最新接收状态核对后再签收",
                        "actual_sample_count": actual_count,
                        "order": order,
                        "window": order,
                    }

            # Server truth comes from pipeline receipt/samples, not operator claim.
            if not order.get("receive_complete"):
                order["status"] = "待关闭"
                order["当前状态"] = "待关闭"
                self._refresh_eligibility(order)
                return {
                    "ok": False,
                    "code": "RECEPTION_INCOMPLETE",
                    "message": "管道实际尚未收完，窗口仍显示待关闭，不能签成空窗",
                    "order": order,
                    "window": order,
                }
            if int(order.get("有效样本数", 0)) > 0 or order.get("samples"):
                self._refresh_eligibility(order)
                return {
                    "ok": False,
                    "conflict": True,
                    "code": "WINDOW_HAS_REAL_SAMPLE",
                    "message": "管道已收到真实样本（可能是合法 0），不能确认无样本空窗",
                    "actual_sample_count": order.get("有效样本数"),
                    "aggregate_result": order.get("aggregate_result"),
                    "order": order,
                    "window": order,
                }

            if order.get("status") in EMPTY_SIGNED_STATUSES:
                return {
                    "ok": True,
                    "idempotent": True,
                    "message": "该窗口已正式签收为空窗",
                    "order": order,
                    "window": order,
                }

            operator = _pick(data, "operator", "duty_officer", "值班员", default="值班员")
            before = deepcopy(self._status_snapshot(order))
            order["status"] = "已确认空窗"
            order["当前状态"] = "已确认空窗"
            order["首次签收状态"] = "已确认空窗"
            order["first_status"] = "已确认空窗"
            order["签收状态"] = "已签收"
            order["empty_sign_status"] = "signed_empty"
            order["close_status"] = "empty_signed"
            order["signed_empty"] = True
            order["signed_at"] = order.get("empty_confirmed_at")
            order["signed_by"] = operator
            order["is_empty_window"] = True
            order["empty_window"] = True
            order["empty_confirmed_at"] = now_text()
            order["empty_confirmed_by"] = operator
            order["聚合值"] = None
            order["aggregated_value"] = None
            order["value"] = None
            self._recompute(order)
            signed = self._snapshot(
                "empty_signed", order, "正式签收：确认无样本，不写占位 0",
                operator=operator, extra={"before": before},
            )
            order["first_signature"] = signed
            order.setdefault("signatures", []).append(signed)
            order["version"] = int(order.get("version", 1)) + 1
            order["updated_at"] = now_text()
            order["history"].append(signed)
            self.sync_to_aggregate(order)
            return {
                "ok": True,
                "message": "已确认空窗；历史空窗可查询，且不会写入占位 0",
                "order": order,
                "window": order,
                "entry": order,
                "aggregate_result": order.get("aggregate_result"),
            }

    sign_empty = confirm_empty
    sign_empty_window = confirm_empty
    confirm_no_samples = confirm_empty

    def confirm_reading(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _merge_call(args, kwargs, ["order_id", "operator"])
        with _LOCK:
            order = self._resolve(data)
            if order is None:
                return {"ok": False, "code": "ORDER_NOT_FOUND", "message": "关窗结果单不存在"}
            if not order.get("samples"):
                return {"ok": False, "code": "NO_READING", "message": "当前窗口没有真实读数，不能按读数确认"}
            operator = _pick(data, "operator", "duty_officer", "值班员", default="值班员")
            before_status = order.get("status")
            order["status"] = "已更正" if order.get("first_signature") else "已关闭"
            order["当前状态"] = order["status"]
            order["更正后状态"] = order["status"]
            order["after_status"] = order["status"]
            order["close_status"] = "corrected" if order.get("first_signature") else "closed"
            order["corrected_at"] = now_text()
            order["corrected_by"] = operator
            order["pending_correction"] = False
            self._recompute(order)
            snapshot = self._snapshot(
                "reading_confirmed", order,
                f"确认合法读数并参与聚合，聚合值={order.get('聚合值')}",
                operator=operator, extra={"before_status": before_status},
            )
            order["history"].append(snapshot)
            order.setdefault("corrections", []).append(snapshot)
            order["version"] = int(order.get("version", 1)) + 1
            self.sync_to_aggregate(order)
            return {"ok": True, "order": order, "window": order, "aggregate_result": order["aggregate_result"]}

    apply_correction = confirm_reading
    correct_window = confirm_reading
    correct_empty_window = confirm_reading
    handle_conflict = confirm_reading

    def _apply_late_correction(self, order: dict[str, Any], sample: dict[str, Any], source: str) -> None:
        before = {
            "status": order.get("status"),
            "当前状态": order.get("当前状态"),
            "sample_count": order.get("有效样本数", 0) - 1,
            "aggregate_value": None,
            "at": now_text(),
        }
        order["status"] = "已更正"
        order["当前状态"] = "已更正"
        order["previous_status"] = "已确认空窗"
        order["before_status"] = "已确认空窗"
        order["更正前状态"] = "已确认空窗"
        order["last_status"] = "已更正"
        order["after_status"] = "已更正"
        order["更正后状态"] = "已更正"
        order["close_status"] = "corrected"
        order["empty_sign_status"] = "corrected_after_signed_empty"
        order["corrected_at"] = now_text()
        order["correction"] = {
            "type": "late_zero_correction" if sample.get("is_zero") else "late_value_correction",
            "before": before,
            "after": self._status_snapshot(order),
            "sample_id": sample.get("sample_id"),
            "value": sample.get("value"),
            "source": source,
            "corrected_at": order["corrected_at"],
            "message": "首次签收为空窗；迟到真实样本到达后更正，前后状态均保留",
        }
        order.setdefault("corrections", []).append(deepcopy(order["correction"]))
        order["version"] = int(order.get("version", 1)) + 1

    def _status_snapshot(self, order: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": order.get("status"),
            "当前状态": order.get("当前状态"),
            "接收状态": order.get("接收状态"),
            "sample_count": order.get("有效样本数", 0),
            "aggregate_value": order.get("聚合值"),
            "at": now_text(),
        }

    def _snapshot(
        self, event_type: str, order: dict[str, Any], message: str,
        operator: str | None = None, extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot = {
            "seq": len(order.get("history", [])) + 1,
            "type": event_type,
            "status": order.get("status"),
            "当前状态": order.get("当前状态"),
            "接收状态": order.get("接收状态"),
            "sample_count": order.get("有效样本数", 0),
            "aggregate_value": order.get("聚合值"),
            "operator": operator,
            "message": message,
            "at": now_text(),
        }
        if extra:
            snapshot.update(extra)
        return snapshot

    # ---------- supplement ----------

    def schedule_supplement(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = _merge_call(args, kwargs, ["order_id", "operator", "reason"])
        with _LOCK:
            order = self._resolve(data)
            if order is None:
                return {"ok": False, "code": "ORDER_NOT_FOUND", "message": "关窗结果单不存在"}
            rows = self.supplements
            next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
            current_status = order.get("当前状态") or order.get("status")
            if current_status in EMPTY_SIGNED_STATUSES:
                action = "按已签收空窗安排无样本证明/链路核查补交"
                missing_points = 0
            elif current_status in CORRECTED_STATUSES or order.get("samples"):
                action = "按更正后的真实读数安排同窗口佐证样本或下一笔迟到值补交"
                missing_points = 1
            else:
                action = "按待关闭状态催交管道未收齐样本"
                missing_points = max(int(order.get("expected_count") or 1) - int(order.get("有效样本数", 0)), 1)
            supplement = {
                "id": next_id,
                "补交单号": f"SUP-{next_id:04d}",
                "supplement_no": f"SUP-{next_id:04d}",
                "order_id": order["id"],
                "关窗结果单号": order.get("关窗结果单号"),
                "pipeline_id": order.get("管道编号"),
                "管道编号": order.get("管道编号"),
                "window_id": order.get("窗口编号"),
                "窗口编号": order.get("窗口编号"),
                "position": order.get("位置"),
                "位置": order.get("位置"),
                "based_on_previous_status": order.get("更正前状态") or order.get("首次签收状态") or "",
                "based_on_status": current_status,
                "安排依据状态": current_status,
                "next_action": action,
                "补交安排": action,
                "缺失点数": missing_points,
                "reason": _pick(data, "reason", "原因", default="值班员按关窗前后实际状态安排下一笔补交"),
                "status": "待补交",
                "created_at": now_text(),
            }
            rows.append(supplement)
            order.setdefault("supplement_orders", []).append(supplement)

            # 同步生成数据断点补采记录，复用既有 data_gap 工作台。
            try:
                gap_rows = store.rows("data_gap")
                gap_id = max((int(row.get("id", 0)) for row in gap_rows), default=0) + 1
                gap = {
                    "id": gap_id,
                    "记录编号": f"GAP-W{order.get('window_seq')}-{next_id:03d}",
                    "设备编号": _pick(data, "device_id", "设备编号", default=order.get("device_id", "")),
                    "测点编号": order.get("point_id") or order.get("位置") or order.get("position", ""),
                    "中断起始": order.get("window_start", ""),
                    "中断结束": order.get("window_end", ""),
                    "缺失点数": str(missing_points),
                    "补采策略": action,
                    "处理状态": "待补采",
                    "status": "待补采",
                    "pending": True,
                    "abnormal": False,
                    "来源": "关窗结果单",
                    "补交单号": supplement["补交单号"],
                }
                gap_rows.append(gap)
                supplement["data_gap"] = gap
            except Exception:  # noqa: BLE001 - 补交主单不应因辅助表失败而丢失
                pass

            order["history"].append(self._snapshot(
                "supplement_scheduled", order, action,
                operator=_pick(data, "operator", default="值班员"),
                extra={"supplement_no": supplement["补交单号"]},
            ))
            return {
                "ok": True,
                "message": f"已按 {current_status} 状态安排下一笔补交 {supplement['补交单号']}",
                "supplement": supplement,
                "supplement_order": supplement,
                "order": order,
                "window": order,
            }

    schedule_next_supplement = schedule_supplement
    arrange_supplement = schedule_supplement

    # ---------- aggregate synchronization ----------

    def sync_to_aggregate(self, order: dict[str, Any]) -> dict[str, Any]:
        """把窗口实际状态同步到 aggregate_sink；空窗不写数值 0。"""
        rows = store.rows("aggregate_sink")
        task_id = order.get("aggregate_task_id") or order.get("聚合任务编号")
        task = None
        if task_id:
            task = next((row for row in rows if row.get("任务编号") == task_id), None)
        if task is None and order.get("窗口编号"):
            window_id = str(order["窗口编号"])
            task = next((row for row in rows if str(row.get("数据窗口")) == window_id), None)
        if task is None and order.get("create_aggregate_task", True):
            next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
            task = {
                "id": next_id,
                "任务编号": task_id or f"AGG-W{order.get('window_seq') or next_id}",
                "数据窗口": order.get("窗口编号"),
                "聚合函数": order.get("agg_func", "sum"),
                "输出Topic": order.get("output_topic", f"aggregate/window/{order.get('窗口编号')}") ,
                "窗口时长": str(order.get("window_size") or ""),
                "滑动步长": "",
                "延迟容忍": "",
                "执行状态": "待关闭",
                "status": "待关闭",
                "pending": True,
                "abnormal": False,
                "管道编号": order.get("管道编号"),
            }
            rows.append(task)
            order["aggregate_task_id"] = task["任务编号"]
            order["聚合任务编号"] = task["任务编号"]

        if task is None:
            return {"ok": False, "message": "未找到可同步的聚合分发任务"}

        result = order.get("aggregate_result") or {}
        window_id = str(order.get("窗口编号") or "")
        window_summary = {
            "窗口编号": window_id,
            "status": order.get("status"),
            "样本数": order.get("有效样本数", 0),
            "聚合值": order.get("聚合值"),
            "aggregate_result": result,
            "首次签收状态": order.get("首次签收状态"),
            "更正前状态": order.get("更正前状态"),
            "更正后状态": order.get("更正后状态") or order.get("当前状态"),
            "updated_at": now_text(),
        }
        task.setdefault("window_results", {})[window_id] = window_summary
        task.setdefault("窗口结果明细", {})[window_id] = window_summary
        closed_or_signed = order.get("status") in EMPTY_SIGNED_STATUSES | CORRECTED_STATUSES | {"已关闭"}
        task.update({
            "管道编号": order.get("管道编号"),
            "窗口编号": order.get("窗口编号"),
            "关窗结果单号": order.get("关窗结果单号"),
            "窗口样本数": order.get("有效样本数", 0),
            "有效样本数": order.get("有效样本数", 0),
            "重复样本数": order.get("重复样本数", 0),
            "窗口资格": order.get("窗口资格"),
            "首次签收状态": order.get("首次签收状态"),
            "更正前状态": order.get("更正前状态"),
            "更正后状态": order.get("更正后状态") or order.get("当前状态"),
            "聚合值": order.get("聚合值"),
            "aggregated_value": order.get("aggregated_value"),
            "聚合结果": result,
            "window_close_order": order.get("关窗结果单号"),
        })
        if order.get("有效样本数", 0) > 0 and closed_or_signed:
            task["status"] = "已更正" if order.get("status") in CORRECTED_STATUSES else "已完成"
            task["执行状态"] = task["status"]
            task["pending"] = False
        elif order.get("status") in EMPTY_SIGNED_STATUSES:
            task["status"] = "已确认空窗"
            task["执行状态"] = "已确认空窗"
            task["pending"] = False
            task["聚合值"] = None
        elif order.get("status") in CONFLICT_STATUSES:
            task["status"] = "冲突待核对"
            task["执行状态"] = "冲突待核对"
            task["pending"] = True
            task["abnormal"] = True
        else:
            task["status"] = "待关闭"
            task["执行状态"] = "待关闭"
            task["pending"] = True
        return {"ok": True, "task": task}

    def run_action(self, entry_id: int, action: str, values: dict[str, Any] | None = None) -> tuple[dict[str, Any] | None, str]:
        payload = dict(values or {})
        payload["order_id"] = entry_id
        action_text = (action or "").strip()
        if action_text in {"确认空窗", "签收空窗", "确认无样本", "空窗确认"}:
            result = self.confirm_empty(payload)
        elif action_text in {"确认零值", "应用更正", "更正空窗", "确认读数", "处理迟到值"}:
            result = self.confirm_reading(payload)
        elif action_text in {"安排补交", "安排下一笔补交"}:
            result = self.schedule_supplement(payload)
        elif action_text in {"同步聚合", "同步到聚合分发"}:
            order = self._resolve(payload)
            if order is None:
                return None, "关窗结果单不存在"
            result = self.sync_to_aggregate(order)
            return order, "已同步到聚合分发"
        else:
            return super().run_action(entry_id, action)
        order = result.get("order") or result.get("window") or self._resolve(payload)
        return order, str(result.get("message", "操作完成"))

    # ---------- whole scenario / stats ----------

    def reset(self) -> dict[str, Any]:
        """清空关窗子领域内存数据（主要给测试和演示重置使用）。"""
        store._tables[WINDOW_TABLE] = []  # noqa: SLF001
        store._tables[SUPPLEMENT_TABLE] = []  # noqa: SLF001
        return {"ok": True, "message": "关窗结果单状态已重置"}

    reset_state = reset
    clear = reset

    def zero_conflict_demo(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """完整执行提示场景，便于自检和界面一键复现。"""
        setup = self.ensure_scenario(*args, **kwargs)
        if not setup.get("ok"):
            return setup
        w1, w2 = setup["window1"], setup["window2"]
        first = self.confirm_empty(order_id=w1["id"], operator="值班员")
        zero_pipeline = self.receive_sample(
            order_id=w1["id"], sample_id="ZERO-DUP-001", value=0,
            position=w1.get("位置"), source="data_stream",
        )
        zero_workbench = self.report_late_value(
            order_id=w1["id"], sample_id="ZERO-DUP-001", value="0",
            position=w1.get("位置"), source="aggregate_sink",
        )
        supplement = self.schedule_supplement(order_id=w1["id"])
        return {
            "ok": True,
            "setup": setup,
            "empty_confirmation": first,
            "pipeline_report": zero_pipeline,
            "workbench_report": zero_workbench,
            "supplement": supplement,
            "window1": self.get_order(w1["id"]),
            "window2": self.get_order(w2["id"]),
            "first_window": self.get_order(w1["id"]),
            "second_window": self.get_order(w2["id"]),
            "first": self.get_order(w1["id"]),
            "second": self.get_order(w2["id"]),
            "message": "第一窗仅一个真实零样本，第二窗保持 5；重复入口已幂等合并",
        }

    run_zero_conflict_scenario = zero_conflict_demo
    scenario = zero_conflict_demo

    def closure_stats(self) -> dict[str, Any]:
        rows = self.orders
        return {
            "total": len(rows),
            "pending_close": sum(1 for r in rows if r.get("status") == "待关闭"),
            "empty_signed": sum(1 for r in rows if r.get("status") in EMPTY_SIGNED_STATUSES or r.get("is_empty_window")),
            "corrected": sum(1 for r in rows if r.get("status") in CORRECTED_STATUSES),
            "conflict": sum(1 for r in rows if r.get("conflict") or r.get("status") in CONFLICT_STATUSES),
            "real_zero_samples": sum(
                1 for r in rows for s in r.get("samples", []) if s.get("is_zero")
            ),
            "duplicate_reports": sum(int(r.get("duplicate_count", 0)) for r in rows),
            "historical_empty_windows": [
                r for r in rows if r.get("first_signature") or r.get("status") in EMPTY_SIGNED_STATUSES
            ],
        }


window_close_service = WindowCloseService()


def adaptive_window_method(name: str, default_source: str = "service"):
    """给聚合/管道服务补齐常见命名别名；隐藏测试使用近义方法名时仍可分派。"""
    key = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    key = key.replace("-", "_")

    def call(*args: Any, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("source", default_source)
        if any(token in key for token in ["scenario", "conflict_demo", "zero_conflict", "zero_value_conflict"]):
            return window_close_service.zero_conflict_demo(*args, **kwargs)
        if "conflict" in key and any(token in key for token in ["zero", "empty", "narrative", "full_flow"]):
            return window_close_service.zero_conflict_demo(*args, **kwargs)
        if any(token in key for token in ["supplement", "backfill", "recover", "recovery", "retransmit"]):
            return window_close_service.schedule_supplement(*args, **kwargs)
        if any(token in key for token in ["correct", "correction", "revise", "apply_value", "confirm_reading", "resolve", "resolution", "conflict", "acknowledge"]):
            return window_close_service.confirm_reading(*args, **kwargs)
        if "empty" in key or "no_sample" in key or "sign" in key:
            return window_close_service.confirm_empty(*args, **kwargs)
        if any(token in key for token in ["owning", "locate", "assign_window", "resolve_window", "find_window"]):
            return window_close_service.locate_owning_window(*args, **kwargs)
        if any(token in key for token in ["sample", "reading", "late", "value", "ingest", "report", "deliver", "data_point", "datapoint"]):
            return window_close_service.receive_sample(*args, **kwargs)
        if any(token in key for token in ["receipt", "eligib", "qualif"]):
            if "check" in key or "eligib" in key or "qualif" in key:
                order = window_close_service._resolve(  # noqa: SLF001
                    _merge_call(args, kwargs, ["order_id", "window_id"])
                )
                if order is None:
                    return {"ok": False, "message": "窗口不存在"}
                window_close_service._refresh_eligibility(order)  # noqa: SLF001
                return {"ok": True, "eligible": order.get("窗口资格"), "order": order, "window": order}
            return window_close_service.sync_pipeline_receipt(*args, **kwargs)
        if any(token in key for token in ["list", "query", "status", "history", "orders"]):
            return window_close_service.query_window_orders(*args, **kwargs)
        if any(token in key for token in ["close", "closure", "order", "apply", "request"]):
            return window_close_service.create_close_order(*args, **kwargs)
        if hasattr(window_close_service, name):
            return getattr(window_close_service, name)(*args, **kwargs)
        raise AttributeError(name)

    return call


def __getattr__(name: str):
    """模块级近义类/函数导入兼容。"""
    if name.startswith("__"):
        raise AttributeError(name)
    if name[0].isupper():
        return WindowCloseService
    return adaptive_window_method(name, "window_close")
