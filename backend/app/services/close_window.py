"""兼容命名：close_window 领域入口。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as CloseWindowService,
    window_close_service as close_window_service,
)

CloseWindowOrderService = CloseWindowService
service = close_window_service
