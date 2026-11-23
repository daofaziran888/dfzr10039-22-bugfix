"""URL去重领域服务：指纹台账 + 认领回执，重复次数和窗口资格都落台账。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class UrlDedupService(BaseModuleService):
    MODULE = "url_dedup"
    ENTITY = "去重指纹"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["指纹编号", "源URL", "指纹算法", "指纹哈希", "发现渠道", "首次发现",
                   "重复次数", "去重状态", "最近认领窗口", "实际采集入口", "回执编号"]
    REQUIRED_FIELDS = ["指纹编号", "源URL", "指纹算法"]
    STATUS_ORDER = ["已记录", "已去重", "允许重采", "永久排除"]
    ACTION_RULES = {"标记重采": "已去重", "永久排除": "永久排除"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记指纹时补齐规范URL和哈希，历史样式的表单数据也能直接进入台账。"""
        from app.integration import register_fingerprint

        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        entry = register_fingerprint(
            source_url=str(values.get("源URL", "")),
            algorithm=str(values.get("指纹算法", "sha256") or "sha256"),
            fingerprint_id=str(values.get("指纹编号", "")),
            first_seen=str(values.get("首次发现", "") or "") or None,
            discovery_channel=str(values.get("发现渠道", "") or "") or None,
            status=str(values.get("去重状态", "") or "") or None,
        )
        return entry, []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """按持久化回执补齐重复次数/回执编号，列表与详情刷新后一致。"""
        from app.integration import load_fingerprint_receipt

        rows, total = super().list_entries(
            filters=filters, keyword=keyword, status=status, page=page, size=size)
        for row in rows:
            receipt = load_fingerprint_receipt(
                str(row.get("指纹哈希") or row.get("源URL") or ""))
            if receipt is not None:
                row["重复次数"] = receipt["repeated"]
                row["首次发现"] = receipt["first_seen"] or row.get("首次发现", "")
                row["实际采集入口"] = receipt["claimed_url"]
                row["最近认领窗口"] = receipt["window"]
                row["回执编号"] = receipt["receipt_id"]
        return rows, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """详情按持久化台账返回，重复次数、回执编号与列表刷新后一致。"""
        entry = super().get_entry(entry_id)
        if entry is None:
            return None
        from app.integration import load_fingerprint_receipt

        receipt = load_fingerprint_receipt(str(entry.get("指纹哈希") or entry.get("源URL") or ""))
        if receipt is not None:
            entry["重复次数"] = receipt["repeated"]
            entry["首次发现"] = receipt["first_seen"] or entry.get("首次发现", "")
            entry["实际采集入口"] = receipt["claimed_url"]
            entry["最近认领窗口"] = receipt["window"]
            entry["回执编号"] = receipt["receipt_id"]
        return entry


class UrlDedupClaimService(BaseModuleService):
    """采集作业认领列表：每次窗口认领一条回执，记录执行和重复的原始入口。"""
    MODULE = "url_dedup_claim"
    ENTITY = "去重回执"
    OPERATION_TERM = "认领"
    LIST_FIELDS = ["回执编号", "指纹编号", "指纹哈希", "认领窗口", "作业编号", "目标站点",
                   "提交入口", "实际采集入口", "重复入口", "首次发现", "确认时间", "去重状态"]
    REQUIRED_FIELDS = ["回执编号", "指纹哈希", "认领窗口"]
    STATUS_ORDER = ["已记录", "已去重", "实际采集", "已采集", "窗口重复", "永久排除"]
    ACTION_RULES = {"标记已采集": "已采集", "确认去重": "已去重"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str,
                   remark: str | None = None) -> tuple[dict[str, Any] | None, str]:
        from app.integration import mark_claim_executed

        entry, message = super().run_action(entry_id, action, remark=remark)
        if entry is not None and action == "标记已采集":
            claim = mark_claim_executed(str(entry.get("回执编号", "")))
            if claim is not None:
                entry["实际采集入口"] = claim.executed_url
                entry["去重状态"] = claim.status
                entry["status"] = claim.status
                entry["pending"] = False
            message = f"{self.ENTITY}已标记实际采集"
        return entry, message
