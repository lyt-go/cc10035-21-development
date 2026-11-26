"""窗口领域服务的简短兼容导入路径。"""
from app.services.window_close import *  # noqa: F401,F403
from app.services.window_close import (  # noqa: F401
    WindowCloseService as WindowService,
    window_close_service as window_service,
)

service = window_service
