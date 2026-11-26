"""关窗结果单复数命名兼容模块。"""
from app.services.window_close import *  # noqa: F401,F403
from app.services.window_close import (  # noqa: F401
    WindowCloseService,
    window_close_service,
)

service = window_close_service
