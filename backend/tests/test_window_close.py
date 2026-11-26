"""关窗结果单、空窗/零值区分和迟到样本归窗的回归测试。"""
from __future__ import annotations

from copy import deepcopy

import pytest

from app.services.aggregate_sink import AggregateSinkService
from app.services.data_stream import DataStreamService
from app.services.window_close import WindowCloseService
from app.store import store


@pytest.fixture(autouse=True)
def restore_global_store():
    """本文件的场景会创建聚合/断点辅助行；用例后完整还原全局内存仓库。"""
    snapshot = deepcopy(store._tables)
    yield
    store._tables = snapshot


@pytest.fixture
def windows():
    service = WindowCloseService()
    pipeline = "P-WINDOW-TEST"
    # 每个用例使用独立管道；清理该管道产生的窗口单，避免全局内存仓库串扰。
    yield service, pipeline
    table = store.rows("window_close_order")
    store._tables["window_close_order"] = [
        row for row in table if row.get("pipeline_id") != pipeline
    ]


def _two_windows(service: WindowCloseService, pipeline: str):
    w1 = service.create_close_order(
        pipeline_id=pipeline, window_id="W1", window_start=0, window_end=60,
        position="P1", receive_complete=True,
    )["order"]
    w2 = service.create_close_order(
        pipeline_id=pipeline, window_id="W2", window_start=60, window_end=120,
        position="P1", receive_complete=True,
    )["order"]
    service.receive_sample(order_id=w2["id"], sample_id="w2-5", value=5, sample_time=90)
    service.confirm_reading(order_id=w2["id"])
    return service.get_order(w1["id"]), service.get_order(w2["id"])


def test_incomplete_window_stays_pending_and_cannot_sign_empty(windows):
    service, pipeline = windows
    order = service.create_close_order(
        pipeline_id=pipeline, window_id="W1", receive_complete=False
    )["order"]
    result = service.confirm_empty(order_id=order["id"])
    assert result["ok"] is False
    assert result["code"] == "RECEPTION_INCOMPLETE"
    assert service.get_order(order["id"])["status"] == "待关闭"


def test_confirmed_empty_has_no_placeholder_zero_but_remains_queryable(windows):
    service, pipeline = windows
    order = service.create_close_order(
        pipeline_id=pipeline, window_id="W1", receive_complete=True
    )["order"]
    result = service.confirm_empty(order_id=order["id"])
    assert result["ok"] is True
    stored = service.get_order(order["id"])
    assert stored["status"] == "已确认空窗"
    assert stored["有效样本数"] == 0
    assert stored["聚合值"] is None
    assert stored["aggregate_result"]["status"] == "empty"
    assert any(item["type"] == "empty_signed" for item in stored["history"])
    assert any(row["关窗结果单号"] == stored["关窗结果单号"] for row in service.orders)


def test_real_zero_is_legal_and_duplicate_from_two_entries_counts_once(windows):
    service, pipeline = windows
    w1, w2 = _two_windows(service, pipeline)
    data_stream = DataStreamService()
    aggregate = AggregateSinkService()

    first = data_stream.receive_sample(
        pipeline_id=pipeline, window_id="W1", sample_id="zero-1", value=0,
        position="P1", source="data_stream",
    )
    second = aggregate.handle_late_value(
        pipeline_id=pipeline, window_id="W1", sample_id="zero-1", value="0",
        position="P1", source="aggregate_sink",
    )

    assert first["ok"] is True
    assert second["ok"] is True
    assert second["duplicate"] is True
    stored_w1 = service.get_order(w1["id"])
    stored_w2 = service.get_order(w2["id"])
    assert stored_w1["有效样本数"] == 1
    assert stored_w1["聚合值"] == 0
    assert stored_w1["aggregate_result"]["empty"] is False
    assert stored_w2["聚合值"] == 5
    assert stored_w2["有效样本数"] == 1


def test_late_sample_uses_actual_owner_and_does_not_change_other_windows(windows):
    service, pipeline = windows
    w1, w2 = _two_windows(service, pipeline)

    result = service.receive_sample(
        pipeline_id=pipeline, window_id="W2", sample_time=12,
        sample_id="late-zero", value=0,
    )
    assert result["ok"] is True
    assert result["reassigned"] is True
    assert result["window"]["窗口编号"] == "W1"
    assert service.get_order(w1["id"])["聚合值"] == 0
    assert service.get_order(w2["id"])["聚合值"] == 5


def test_formerly_signed_empty_keeps_before_and_after_states_and_supplement(windows):
    service, pipeline = windows
    order = service.create_close_order(
        pipeline_id=pipeline, window_id="W1", receive_complete=True
    )["order"]
    service.confirm_empty(order_id=order["id"], operator="值班员")
    conflict = service.receive_sample(
        order_id=order["id"], sample_id="late-zero", value=0, source="data_stream"
    )
    assert conflict["conflict"] is True
    stored = service.get_order(order["id"])
    assert stored["status"] == "已更正"
    assert stored["首次签收状态"] == "已确认空窗"
    assert stored["更正前状态"] == "已确认空窗"
    assert stored["更正后状态"] == "已更正"
    assert stored["correction"]["before"]["status"] == "已确认空窗"
    assert stored["correction"]["after"]["status"] == "已更正"

    supplement = service.schedule_supplement(order_id=order["id"])
    assert supplement["ok"] is True
    assert supplement["supplement"]["based_on_status"] == "已更正"
    assert stored["supplement_orders"]


def test_complete_narrative_scenario():
    result = WindowCloseService().zero_conflict_demo(reset=True)
    assert result["ok"] is True
    w1 = result["window1"]
    w2 = result["window2"]
    assert w1["有效样本数"] == 1
    assert w1["聚合值"] == 0
    assert w1["重复样本数"] == 1
    assert w1["first_signature"]["status"] == "已确认空窗"
    assert w1["correction"]["before"]["status"] == "已确认空窗"
    assert w2["聚合值"] == 5
    assert w2["有效样本数"] == 1
