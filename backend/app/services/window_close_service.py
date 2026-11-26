"""兼容导入：window_close_service。"""
from app.services.window_close import *  # noqa: F401,F403
from app.services.window_close import (  # noqa: F401
    WindowCloseService,
    window_close_service,
)

service = window_close_service
