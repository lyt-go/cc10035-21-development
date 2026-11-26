"""关窗结果单命名兼容模块。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as WindowCloseResultService,
    window_close_service as window_close_result_service,
)

WindowCloseOrderResultService = WindowCloseResultService
service = window_close_result_service
