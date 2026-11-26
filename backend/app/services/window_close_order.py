"""兼容命名：window_close_order 领域入口。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as WindowCloseOrderService,
    window_close_service as window_close_order_service,
)

service = window_close_order_service
