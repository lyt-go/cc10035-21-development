"""窗口关闭流程命名兼容模块。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as WindowClosingService,
    window_close_service as window_closing_service,
)

service = window_closing_service
