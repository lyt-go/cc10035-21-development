"""空窗签收领域入口，实际逻辑由关窗结果单服务统一承载。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as EmptyWindowService,
    window_close_service as empty_window_service,
)

service = empty_window_service
