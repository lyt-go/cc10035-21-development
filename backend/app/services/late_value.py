"""迟到值处理领域入口，实际逻辑由关窗结果单服务统一承载。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as LateValueService,
    adaptive_window_method,
    window_close_service as late_value_service,
)

service = late_value_service
