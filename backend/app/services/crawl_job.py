"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.models import CrawlJobSpec
from app.services.base import BaseModuleService
from app.store import store


class CrawlJobService(BaseModuleService):
    MODULE = "crawl_job"
    ENTITY = "采集作业"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["作业编号", "目标站点", "入口URL", "最大深度", "并发线程", "调度策略",
                   "完成页数", "失败页数", "认领窗口", "实际采集入口", "重复入口", "去重状态"]
    REQUIRED_FIELDS = ["作业编号", "目标站点", "入口URL"]
    STATUS_ORDER = ["待执行", "采集执行", "已完成", "部分失败"]
    ACTION_RULES = {"启动采集": "采集执行", "暂停作业": "已完成", "标记完成": "部分失败"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing
        # 新登记的作业把认领列表按空值补齐，页面刷新前后字段稳定。
        for field, default in (
            ("认领窗口", ""), ("实际采集入口", ""), ("重复入口", ""), ("去重状态", "待认领")):
            if entry.get(field) is None:
                entry[field] = default
        return entry, missing

    def _apply_claim_view(self, entry: dict[str, Any]) -> None:
        """把持久化认领回执回填到作业行，列表和详情刷新后看到同一份数据。"""
        from app.integration import job_claim_list

        claims = job_claim_list(str(entry.get("作业编号", "")))
        if not claims:
            for field, default in (
                ("认领窗口", ""), ("实际采集入口", ""),
                ("重复入口", ""), ("去重状态", "待认领")):
                if entry.get(field) is None:
                    entry[field] = default
            return
        entry["认领窗口"] = claims[-1]["认领窗口"]
        executed = list(dict.fromkeys(
            c["实际采集入口"] for c in claims if c["实际采集入口"]))
        # 汇总本作业在所有窗口里登记的重复入口；最终真正执行过的地址以执行为准，
        # 避免“实际采集入口”和“重复入口”是同一个地址。
        duplicates = [url for url in dict.fromkeys(
            u for c in claims for u in c["重复入口"].split(",") if u)
            if url not in executed]
        entry["实际采集入口"] = ",".join(executed)
        entry["重复入口"] = ",".join(duplicates)
        entry["去重状态"] = "已去重" if duplicates else "已认领"

    def list_entries(self, *, filters: dict[str, str] | None = None,
                     keyword: str | None = None, status: str | None = None,
                     page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        rows, total = super().list_entries(
            filters=filters, keyword=keyword, status=status, page=page, size=size)
        for row in rows:
            self._apply_claim_view(row)
        return rows, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """详情把持久化认领回执回填到作业上，和去重台账同源。"""
        entry = super().get_entry(entry_id)
        if entry is not None:
            self._apply_claim_view(entry)
        return entry

    def run_action(self, entry_id: int, action: str,
                   remark: str | None = None) -> tuple[dict[str, Any] | None, str]:
        """启动采集先做一次窗口认领；只有领到资格的地址才会真正执行。

        可选 remark 作为认领窗口，不传时按小时分桶。同一窗口重复启动不会产生
        第二次实际采集；换一个窗口 remark 就能继续处理同一批入口。
        """
        entry = store.find(self.MODULE, entry_id)
        window = ""
        if entry is not None and action == "启动采集":
            from app.integration import dispatch_with_dedup

            job = CrawlJobSpec.from_seed_row(entry)
            _, outcomes = dispatch_with_dedup(job, window=(remark or "").strip() or None)
            window = outcomes[0].window if outcomes else ""
        entry, message = super().run_action(entry_id, action, remark=remark)
        if entry is not None:
            self._apply_claim_view(entry)
        if action == "启动采集" and entry is not None and window:
            message = f"{message}（认领窗口 {window}）"
        return entry, message
