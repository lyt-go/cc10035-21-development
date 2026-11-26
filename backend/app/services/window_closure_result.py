"""兼容命名：window_closure_result 领域入口。"""
from app.services.window_close import (  # noqa: F401
    WindowCloseService as WindowClosureResultService,
    window_close_service as window_closure_result_service,
)

service = window_closure_result_service
