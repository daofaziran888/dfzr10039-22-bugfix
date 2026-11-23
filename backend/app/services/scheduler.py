"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.models import CrawlJobSpec
from app.services.base import BaseModuleService
from app.store import store


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "调度计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划编号", "关联作业", "Cron表达式", "时区设置", "依赖检查", "失败通知",
                   "并发控制", "下次执行", "认领窗口", "认领地址", "实际执行入口",
                   "重复原始入口", "去重回执", "实际采集数", "重复提交数"]
    REQUIRED_FIELDS = ["计划编号", "关联作业", "Cron表达式"]
    STATUS_ORDER = ["待激活", "执行中", "已完成", "已停用"]
    ACTION_RULES = {"激活计划": "执行中", "立即执行": "已完成", "停用计划": "已停用"}
    NEGATIVE_ACTIONS = ["停用计划"]

    def run_action(self, entry_id: int, action: str,
                   remark: str | None = None) -> tuple[dict[str, Any] | None, str]:
        """「立即执行」走同一条去重认领：窗口、地址和回执都登记回调度计划。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or action != "立即执行":
            return super().run_action(entry_id, action, remark=remark)

        from app.integration import dispatch_with_dedup, _find_job_row

        job_row = _find_job_row(str(entry.get("关联作业", "")))
        if job_row is None:
            return None, f"调度计划 {entry_id} 关联的采集作业不存在"
        job = CrawlJobSpec.from_seed_row(job_row)
        window = (remark or "").strip() or str(entry.get("并发控制", "") or "") or None
        _, outcomes = dispatch_with_dedup(job, window=window)
        entry, message = super().run_action(entry_id, action, remark=remark)
        if entry is not None:
            entry["实际采集数"] = sum(1 for o in outcomes if o.can_crawl)
            entry["重复提交数"] = sum(1 for o in outcomes if not o.can_crawl)
            message = f"{message}（窗口 {outcomes[0].window if outcomes else ''}）"
        return entry, message
