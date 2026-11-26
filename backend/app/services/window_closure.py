"""兼容命名：关窗结果单服务的 window_closure 写法。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as WindowClosureService,
    window_close_service as window_closure_service,
)

WindowClosureOrderService = WindowClosureService
service = window_closure_service
