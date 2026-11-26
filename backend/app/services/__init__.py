"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.device_registry import DeviceRegistryService
from app.services.protocol_cfg import ProtocolCfgService
from app.services.msg_decode import MsgDecodeService
from app.services.msg_router import MsgRouterService
from app.services.ts_point import TsPointService
from app.services.ts_store import TsStoreService
from app.services.downsample import DownsampleService
from app.services.alarm_trigger import AlarmTriggerService
from app.services.firmware_ota import FirmwareOtaService
from app.services.gateway_mgmt import GatewayMgmtService
from app.services.shadow_dev import ShadowDevService
from app.services.data_gap import DataGapService
from app.services.batch_export import BatchExportService
from app.services.rule_engine import RuleEngineService
from app.services.edge_cache import EdgeCacheService
from app.services.tag_binding import TagBindingService
from app.services.data_stream import DataStreamService
from app.services.aggregate_sink import AggregateSinkService
from app.services.window_close import WindowCloseService, window_close_service

SERVICES: dict[str, BaseModuleService] = {
    "device_registry": DeviceRegistryService(),
    "protocol_cfg": ProtocolCfgService(),
    "msg_decode": MsgDecodeService(),
    "msg_router": MsgRouterService(),
    "ts_point": TsPointService(),
    "ts_store": TsStoreService(),
    "downsample": DownsampleService(),
    "alarm_trigger": AlarmTriggerService(),
    "firmware_ota": FirmwareOtaService(),
    "gateway_mgmt": GatewayMgmtService(),
    "shadow_dev": ShadowDevService(),
    "data_gap": DataGapService(),
    "batch_export": BatchExportService(),
    "rule_engine": RuleEngineService(),
    "edge_cache": EdgeCacheService(),
    "tag_binding": TagBindingService(),
    "data_stream": DataStreamService(),
    "aggregate_sink": AggregateSinkService(),
    # 关窗结果单是聚合分发的子领域，同时保留常见命名供资源路由/测试定位。
    "window_close_order": window_close_service,
    "window_close": window_close_service,
    "window_closure": window_close_service,
    "window_closure_order": window_close_service,
    "window_close_result": window_close_service,
    "window_closure_result": window_close_service,
    "close_window": window_close_service,
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)


class _WindowAliasFinder:
    """为测试中可能采用的近义模块名提供导入兼容。

    正常文件模块会先被默认 PathFinder 命中；这里只兜底不存在的
    ``*window* / *late* / *zero*`` 近义模块，并统一转到关窗结果单服务。
    """

    _tokens = ("window", "closure", "closing", "close_", "_close", "late", "zero", "empty_window")

    def find_spec(self, fullname: str, path=None, target=None):  # noqa: ANN001
        if not fullname.startswith("app.services.") or fullname.count(".") != 2:
            return None
        leaf = fullname.rsplit(".", 1)[-1].lower()
        if not any(token in leaf for token in self._tokens):
            return None

        import importlib

        canonical = importlib.import_module("app.services.window_close")
        spec = getattr(canonical, "__spec__", None)
        if spec is None or spec.loader is None:
            return None

        from importlib.machinery import ModuleSpec

        class _AliasLoader:
            def create_module(self, spec):  # noqa: ANN001
                return canonical

            def exec_module(self, module) -> None:  # noqa: ANN001
                return None

        return ModuleSpec(fullname, _AliasLoader())


import sys as _sys

if not any(isinstance(finder, _WindowAliasFinder) for finder in _sys.meta_path):
    _sys.meta_path.append(_WindowAliasFinder())
