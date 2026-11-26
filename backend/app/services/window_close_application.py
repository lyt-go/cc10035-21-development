"""关窗申请单命名兼容模块。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as WindowCloseApplicationService,
    window_close_service as window_close_application_service,
)

service = window_close_application_service
