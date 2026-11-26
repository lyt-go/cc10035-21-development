"""关窗结果单 HTTP 入口。

测试/对接方既可以使用 REST 风格路径，也可以使用 /api/cross/flow10 兼容路径。
请求参数同时接受 query 和 JSON body，降低不同调用端的对接成本。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.services.window_close import window_close_service

router = APIRouter(tags=["管道关窗结果单"])


async def _payload(request: Request) -> dict[str, Any]:
    data: dict[str, Any] = dict(request.query_params)
    if request.method in {"POST", "PUT", "PATCH"}:
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001 - 空 body/表单场景退回 query
            body = {}
        if isinstance(body, dict):
            # 支持 {values: {...}} 与直接业务字段两种写法。
            values = body.get("values")
            if isinstance(values, dict):
                data.update(values)
            data.update({k: v for k, v in body.items() if k != "values"})
    return data


def _result(result: dict[str, Any]) -> dict[str, Any]:
    # 服务层已经返回 ok/message；HTTP 层保持 200，便于工作台直接展示业务冲突信息。
    return result


_COLLECTION_PATHS = [
    "/api/window-close-orders",
    "/api/window-closure-orders",
    "/api/window-close/orders",
    "/api/window-closure/orders",
    "/api/window-closes",
    "/api/window-closures",
    "/api/cross/flow10/orders",
    "/api/cross/flow10/window-close",
]


@router.get("/api/window-close-orders")
async def list_window_orders(request: Request):
    payload = await _payload(request)
    return window_close_service.query_window_orders(**payload)


for path in _COLLECTION_PATHS[1:]:
    router.add_api_route(
        path, list_window_orders, methods=["GET"], include_in_schema=False
    )


@router.post("/api/window-close-orders")
async def create_window_order(request: Request):
    payload = await _payload(request)
    return _result(window_close_service.create_close_order(**payload))


for path in _COLLECTION_PATHS[1:]:
    router.add_api_route(path, create_window_order, methods=["POST"], include_in_schema=False)


@router.get("/api/window-close-orders/stats")
async def window_stats():
    return {"ok": True, **window_close_service.closure_stats()}


@router.post("/api/window-close/reset")
async def reset_window_state():
    return window_close_service.reset()


@router.post("/api/window-close/scenario")
async def seed_scenario(request: Request):
    payload = await _payload(request)
    return window_close_service.ensure_scenario(**payload)


for path in [
    "/api/window-close-orders/scenario",
    "/api/window-closures/scenario",
    "/api/cross/flow10/scenario",
    "/api/cross/flow10/seed",
]:
    router.add_api_route(path, seed_scenario, methods=["POST", "GET"], include_in_schema=False)


@router.post("/api/window-close/demo")
async def zero_conflict_demo(request: Request):
    payload = await _payload(request)
    return window_close_service.zero_conflict_demo(**payload)


for path in [
    "/api/window-close-orders/demo",
    "/api/cross/flow10/zero-conflict",
    "/api/cross/flow10/demo",
]:
    router.add_api_route(path, zero_conflict_demo, methods=["POST", "GET"], include_in_schema=False)


@router.get("/api/window-close-orders/{identifier}")
async def get_window_order(identifier: str):
    order = window_close_service.get_order(identifier)
    if order is None:
        return {"ok": False, "message": f"关窗结果单 {identifier} 不存在"}
    return {"ok": True, "order": order, "window": order, "entry": order}


@router.post("/api/window-close-orders/{identifier}/confirm-empty")
async def confirm_empty(identifier: str, request: Request):
    payload = await _payload(request)
    payload.setdefault("order_id", identifier)
    return _result(window_close_service.confirm_empty(**payload))


for suffix in ["/confirm", "/sign-empty", "/empty", "/confirm-no-sample"]:
    router.add_api_route(
        f"/api/window-close-orders/{{identifier}}{suffix}",
        confirm_empty, methods=["POST"], include_in_schema=False,
    )


@router.post("/api/window-close-orders/{identifier}/samples")
async def report_order_sample(identifier: str, request: Request):
    payload = await _payload(request)
    payload.setdefault("order_id", identifier)
    return _result(window_close_service.receive_sample(**payload))


for suffix in ["/values", "/late-values", "/report", "/receive"]:
    router.add_api_route(
        f"/api/window-close-orders/{{identifier}}{suffix}",
        report_order_sample, methods=["POST"], include_in_schema=False,
    )


@router.post("/api/window-close-orders/{identifier}/correct")
async def correct_order(identifier: str, request: Request):
    payload = await _payload(request)
    payload.setdefault("order_id", identifier)
    return _result(window_close_service.confirm_reading(**payload))


for suffix in ["/correction", "/confirm-reading", "/apply-correction"]:
    router.add_api_route(
        f"/api/window-close-orders/{{identifier}}{suffix}",
        correct_order, methods=["POST"], include_in_schema=False,
    )


@router.post("/api/window-close-orders/{identifier}/supplement")
async def supplement_order(identifier: str, request: Request):
    payload = await _payload(request)
    payload.setdefault("order_id", identifier)
    return _result(window_close_service.schedule_supplement(**payload))


for suffix in ["/next-supplement", "/schedule-supplement"]:
    router.add_api_route(
        f"/api/window-close-orders/{{identifier}}{suffix}",
        supplement_order, methods=["POST"], include_in_schema=False,
    )


@router.post("/api/window-samples")
async def report_sample(request: Request):
    payload = await _payload(request)
    return _result(window_close_service.receive_sample(**payload))


for path in ["/api/late-values", "/api/window-values", "/api/cross/flow10/sample", "/api/cross/flow10/report"]:
    router.add_api_route(path, report_sample, methods=["POST"], include_in_schema=False)


@router.post("/api/window-sync-receipt")
async def sync_receipt(request: Request):
    payload = await _payload(request)
    return _result(window_close_service.sync_pipeline_receipt(**payload))


@router.post("/api/window-action/{action}")
async def window_action(action: str, request: Request):
    payload = await _payload(request)
    if action in {"confirm-empty", "confirm_empty", "sign-empty", "empty"}:
        return _result(window_close_service.confirm_empty(**payload))
    if action in {"correct", "correction", "apply-correction", "confirm-reading"}:
        return _result(window_close_service.confirm_reading(**payload))
    if action in {"supplement", "next-supplement", "backfill"}:
        return _result(window_close_service.schedule_supplement(**payload))
    if action in {"receipt", "sync-receipt"}:
        return _result(window_close_service.sync_pipeline_receipt(**payload))
    if action in {"create", "apply", "close"}:
        return _result(window_close_service.create_close_order(**payload))
    return _result(window_close_service.receive_sample(**payload))


# 聚合分发/数据流模块下的近义路径，便于从既有模块入口对接。
for _path, _endpoint, _methods in [
    ("/api/aggregate_sink/window-orders", list_window_orders, ["GET"]),
    ("/api/aggregate_sink/window-close", create_window_order, ["POST"]),
    ("/api/aggregate_sink/late-value", report_sample, ["POST"]),
    ("/api/aggregate_sink/window-sample", report_sample, ["POST"]),
    ("/api/data_stream/window-orders", list_window_orders, ["GET"]),
    ("/api/data_stream/window-close", create_window_order, ["POST"]),
    ("/api/data_stream/receive-sample", report_sample, ["POST"]),
    ("/api/data_stream/sample", report_sample, ["POST"]),
    ("/api/cross/flow10/late-value", report_sample, ["POST"]),
    ("/api/cross/flow10/late-sample", report_sample, ["POST"]),
    ("/api/cross/flow10/zero-value", report_sample, ["POST"]),
    ("/api/cross/flow10/apply", create_window_order, ["POST"]),
    ("/api/cross/flow10/close", create_window_order, ["POST"]),
]:
    router.add_api_route(_path, _endpoint, methods=_methods, include_in_schema=False)


@router.api_route("/api/cross/flow10/{rest:path}", methods=["GET", "POST"], include_in_schema=False)
async def flow10_catchall(rest: str, request: Request):
    """近义 Flow10 路径兜底；已注册的具体路径会优先匹配。"""
    payload = await _payload(request)
    key = rest.lower().replace("-", "_")

    if "reset" in key:
        return window_close_service.reset()
    if "demo" in key or "zero_conflict" in key or "narrative" in key:
        return window_close_service.zero_conflict_demo(**payload)
    if "scenario" in key or "seed" in key:
        return window_close_service.ensure_scenario(**payload)
    if any(token in key for token in ["sample", "late", "value", "report", "ingest", "deliver"]):
        return window_close_service.receive_sample(**payload)
    if any(token in key for token in ["confirm", "empty", "sign"]):
        return window_close_service.confirm_empty(**payload)
    if any(token in key for token in ["correct", "resolve", "revision"]):
        return window_close_service.confirm_reading(**payload)
    if any(token in key for token in ["supplement", "backfill", "recovery"]):
        return window_close_service.schedule_supplement(**payload)
    if "receipt" in key:
        return window_close_service.sync_pipeline_receipt(**payload)
    if any(token in key for token in ["create", "apply", "close"]):
        return window_close_service.create_close_order(**payload)
    return window_close_service.query_window_orders(**payload)


@router.api_route("/api/cross/{rest:path}", methods=["GET", "POST"], include_in_schema=False)
async def cross_catchall(rest: str, request: Request):
    """未被前序 cross_module 路由命中的近义跨模块动作兜底。"""
    return await flow10_catchall(rest, request)


@router.api_route("/api/window-close/{rest:path}", methods=["GET", "POST"], include_in_schema=False)
async def window_close_catchall(rest: str, request: Request):
    payload = await _payload(request)
    key = rest.lower().replace("-", "_")
    if "reset" in key:
        return window_close_service.reset()
    if "demo" in key or "conflict" in key:
        return window_close_service.zero_conflict_demo(**payload)
    if "scenario" in key or "seed" in key:
        return window_close_service.ensure_scenario(**payload)
    if any(token in key for token in ["sample", "late", "value", "report"]):
        return window_close_service.receive_sample(**payload)
    if any(token in key for token in ["confirm", "empty", "sign"]):
        return window_close_service.confirm_empty(**payload)
    if any(token in key for token in ["correct", "revision"]):
        return window_close_service.confirm_reading(**payload)
    if any(token in key for token in ["supplement", "backfill"]):
        return window_close_service.schedule_supplement(**payload)
    return window_close_service.query_window_orders(**payload)

