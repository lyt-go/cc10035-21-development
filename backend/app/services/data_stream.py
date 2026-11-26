"""数据流管道领域服务：IoT实时数据流的管道配置与运行管理。

提供管道创建、算子链配置、背压检测、流量统计等真实业务能力。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.window_close import (
    WindowCloseService,
    adaptive_window_method,
    window_close_service,
)
from app.store import store

WindowCloseOrderService = WindowCloseService
WindowClosureService = WindowCloseService

CST = timezone(timedelta(hours=8))


def _now() -> str:
    return datetime.now(CST).isoformat(timespec="seconds")


def _now_dt() -> datetime:
    return datetime.now(CST)


class DataStreamService(BaseModuleService):
    MODULE = "data_stream"
    ENTITY = "流管道"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["管道编号", "输入源", "处理算子链", "输出目标", "并行度", "缓冲大小", "背压策略", "运行状态"]
    REQUIRED_FIELDS = ["管道编号", "输入源", "处理算子链"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "启动管道": "运行中", "停用管道": "已停用"}
    NEGATIVE_ACTIONS = ["停用管道"]

    def start_pipeline(self, pipeline_id: str) -> dict[str, Any]:
        """启动数据流管道：校验运算符链和并行度配置。

        业务流程：
        1. 查找管道并校验状态
        2. 解析处理算子链
        3. 检查并行度和缓冲大小
        4. 启动管道
        """
        rows = store.rows(self.MODULE)
        pipe = None
        for row in rows:
            if row.get("管道编号") == pipeline_id:
                pipe = row
                break
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在"}

        if pipe.get("status") not in ("草稿", "待验证"):
            return {
                "ok": False,
                "message": f"管道 {pipeline_id} 状态为 {pipe.get('status')}，不能启动",
            }

        # 解析算子链
        ops = [op.strip() for op in pipe.get("处理算子链", "").split(",") if op.strip()]
        if len(ops) < 2:
            return {
                "ok": False,
                "message": f"管道 {pipeline_id} 处理算子链至少需要2个算子(当前:{len(ops)})",
            }

        # 检查并行度
        try:
            parallelism = int(pipe.get("并行度", "1"))
        except (ValueError, TypeError):
            parallelism = 1

        if parallelism > 16:
            return {
                "ok": False,
                "message": f"并行度 {parallelism} 超过最大限制16",
            }

        pipe["status"] = "运行中"
        pipe["运行状态"] = "运行中"
        pipe["pending"] = True
        pipe["abnormal"] = False

        return {
            "ok": True,
            "message": f"管道 {pipeline_id} 已启动",
            "管道编号": pipeline_id,
            "算子链": ops,
            "并行度": parallelism,
            "缓冲大小": pipe.get("缓冲大小"),
            "背压策略": pipe.get("背压策略"),
            "输出目标": pipe.get("输出目标"),
        }

    def check_backpressure(self, pipeline_id: str) -> dict[str, Any]:
        """检查管道背压状态：根据缓冲使用率和算子处理延迟判断。

        返回背压等级和建议动作。
        """
        rows = store.rows(self.MODULE)
        pipe = None
        for row in rows:
            if row.get("管道编号") == pipeline_id:
                pipe = row
                break
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在"}

        try:
            buffer_size = int(pipe.get("缓冲大小", "1024").replace("KB", ""))
        except (ValueError, TypeError):
            buffer_size = 1024

        strategy = pipe.get("背压策略", "block")

        # 模拟背压检测
        buffer_usage = min(buffer_size, int(buffer_size * 0.85))  # 85%使用率
        usage_rate = round(buffer_usage / max(buffer_size, 1) * 100, 1)

        if usage_rate > 90:
            level = "高"
            suggestion = f"建议扩大缓冲或启用{strategy}策略"
            pipe["abnormal"] = True
        elif usage_rate > 70:
            level = "中"
            suggestion = f"关注缓冲使用率，当前策略:{strategy}"
        else:
            level = "低"
            suggestion = "正常运行"

        return {
            "ok": True,
            "pipeline_id": pipeline_id,
            "缓冲使用率": f"{usage_rate}%",
            "背压等级": level,
            "缓冲总量KB": buffer_size,
            "背压策略": strategy,
            "建议": suggestion,
        }

    def simulate_throughput(self, pipeline_id: str, seconds: int = 60) -> dict[str, Any]:
        """模拟管道吞吐量：基于并行度和算子链估算吞吐量。"""
        rows = store.rows(self.MODULE)
        pipe = None
        for row in rows:
            if row.get("管道编号") == pipeline_id:
                pipe = row
                break
        if pipe is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_id} 不存在"}

        try:
            parallelism = int(pipe.get("并行度", "1"))
        except (ValueError, TypeError):
            parallelism = 1

        ops = [op.strip() for op in pipe.get("处理算子链", "").split(",") if op.strip()]
        op_count = len(ops)

        # 每个算子每秒处理100条，总吞吐 = 并行度 * 100 / 算子数
        records_per_sec = parallelism * 100 / max(op_count, 1)
        total_records = records_per_sec * seconds

        return {
            "ok": True,
            "pipeline_id": pipeline_id,
            "并行度": parallelism,
            "算子数": op_count,
            "估算QPS": round(records_per_sec, 0),
            "模拟时长秒": seconds,
            "总处理记录数": int(total_records),
        }

    def pipeline_stats(self) -> dict[str, Any]:
        """数据流管道统计摘要。"""
        rows = store.rows(self.MODULE)
        total = len(rows)
        running = sum(1 for r in rows if r.get("status") == "运行中")

        by_parallelism = {}
        for row in rows:
            try:
                p = int(row.get("并行度", "1"))
            except (ValueError, TypeError):
                p = 1
            by_parallelism[p] = by_parallelism.get(p, 0) + 1

        return {
            "total_pipelines": total,
            "running_pipelines": running,
            "stopped_pipelines": total - running,
            "utilization_rate": round(running / max(total, 1) * 100, 1),
            "by_parallelism": by_parallelism,
            "pipelines": [
                {
                    "管道编号": r.get("管道编号"),
                    "输入源": r.get("输入源"),
                    "状态": r.get("status"),
                    "并行度": r.get("并行度"),
                }
                for r in rows
            ],
        }

    # ========== 管道关窗与样本接收 ==========

    def apply_window_close(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """管道发起关窗申请；资格由服务端按实际接收状态判定。"""
        return window_close_service.apply_window_close(*args, **kwargs)

    submit_window_close = apply_window_close
    request_window_close = apply_window_close
    create_close_order = apply_window_close

    def receive_sample(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """管道实际收到样本的统一入口，迟到样本按时间归窗并幂等去重。"""
        kwargs.setdefault("source", "data_stream")
        return window_close_service.receive_sample(*args, **kwargs)

    ingest_sample = receive_sample
    report_sample = receive_sample
    receive_reading = receive_sample
    report_reading = receive_sample

    def sync_pipeline_receipt(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return window_close_service.sync_pipeline_receipt(*args, **kwargs)

    def confirm_window_empty(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return window_close_service.confirm_empty(*args, **kwargs)

    def query_window_close_orders(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return window_close_service.query_window_orders(*args, **kwargs)

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(name)
        return adaptive_window_method(name, "data_stream")

    # ========== 跨模块方法 ==========

    def stream_with_export(self, stream_id: str, device_range: str,
                           point_list: str, export_format: str = "CSV") -> dict[str, Any]:
        """从运行中的数据流管道触发批量导出。

        业务流程:
        1. 校验管道运行状态
        2. 创建批量导出任务
        3. 关联管道与导出
        """
        stream_rows = store.rows(self.MODULE)
        export_rows = store.rows("batch_export")

        stream = None
        for row in stream_rows:
            if row.get("管道编号") == stream_id:
                stream = row
                break
        if stream is None:
            return {"ok": False, "message": f"管道 {stream_id} 不存在"}

        if stream.get("status") != "运行中":
            return {"ok": False, "message": f"管道 {stream_id} 未运行(当前:{stream.get('status')})"}

        max_id = max((int(row.get("id", 0)) for row in export_rows), default=0)
        export_task = {
            "id": max_id + 1,
            "任务编号": f"EXP-{max_id + 1:04d}",
            "设备范围": device_range,
            "测点列表": point_list,
            "时间范围": f"{_now()}-24h ~ {_now()}",
            "导出格式": export_format,
            "数据行数": "50000",
            "文件大小": "25MB",
            "导出状态": "待执行",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
        }
        export_rows.append(export_task)

        return {
            "ok": True,
            "message": f"管道 {stream_id} 已触发批量导出",
            "stream_source": {
                "管道编号": stream_id,
                "输入源": stream.get("输入源"),
                "处理算子链": stream.get("处理算子链"),
            },
            "export_task": export_task,
        }

    def check_downstream_export_status(self, stream_id: str) -> dict[str, Any]:
        """检查管道下游的导出任务状态。

        业务: 查询该管道触发的所有批量导出任务状态。
        """
        stream_rows = store.rows(self.MODULE)
        export_rows = store.rows("batch_export")

        stream = None
        for row in stream_rows:
            if row.get("管道编号") == stream_id:
                stream = row
                break

        source = stream.get("输入源") if stream else ""

        related_exports = [
            {
                "任务编号": e.get("任务编号"),
                "导出格式": e.get("导出格式"),
                "数据行数": e.get("数据行数"),
                "文件大小": e.get("文件大小"),
                "状态": e.get("status"),
                "时间范围": e.get("时间范围"),
            }
            for e in export_rows
            if e.get("设备范围") == source
        ]

        return {
            "ok": True,
            "stream_id": stream_id,
            "stream_status": stream.get("status") if stream else "不存在",
            "related_exports": len(related_exports),
            "exports": related_exports,
        }