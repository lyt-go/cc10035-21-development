"""路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import cross_module, resource, window_close

ROUTERS = [cross_module, window_close, resource]
