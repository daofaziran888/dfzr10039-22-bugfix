"""跨模块数据流编排：真实业务流水线，各模块间通过dataclass传递数据。

实现以下数据流：
- crawl_site → crawl_job → content_extract → field_clean → batch_store
- crawl_job → url_dedup → parse_log
- proxy_pool → anti_block → crawl_limit
- batch_store → data_delivery → quality_sample
- incr_crawl → crawl_job → diff_check
- data_archive ← batch_store
- crawl_cost ← proxy_pool
- crawl_job → scheduler
"""
from __future__ import annotations

import random
import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.models import (
    AntiBlockStrategy, AntiType, ArchiveFormat, ArchivePolicy, CleanedContent,
    CleanRule, CostRecord, CrawlJobSpec, CrawlLimitRule, DeliveryFormat,
    DeliveryTask, DedupClaim, DedupOutcome, DiffRecord, ExtractRule,
    IncrCursor, ParseLogEntry, ProxyNode, QualitySample, RawExtractedContent,
    SampleMethod, SchedulePlan, SiteConfig, SiteType, StorageBatch,
    UrlFingerprint,
)
from app.store import store


# ---------------------------------------------------------------------------
# 流水线 1: crawl_site → crawl_job → content_extract → field_clean → batch_store
# ---------------------------------------------------------------------------

def run_crawl_pipeline(site_row: dict[str, Any], job_row: dict[str, Any],
                       extract_rows: list[dict[str, Any]],
                       clean_rows: list[dict[str, Any]]) -> StorageBatch | None:
    """端到端采集链路：站点配置 → 作业创建 → 内容抽取 → 字段清洗 → 批次存储。

    返回完整的 StorageBatch 或 None（不可恢复错误时）。
    """
    # Step 1: 站点验证（crawl_site）
    site = SiteConfig.from_seed_row(site_row)
    if not site.is_crawlable():
        return None  # 站点未验证或 Robots 禁止采集

    # Step 2: 创建采集作业（crawl_job）
    job = CrawlJobSpec.from_seed_row(job_row, site=site)
    if not job.entry_urls:
        return None  # 没有入口 URL

    # Step 3: 内容抽取（content_extract）
    rules = [ExtractRule.from_seed_row(er) for er in extract_rows if er.get("status") == "生效中"]
    if not rules:
        rules = [ExtractRule.from_seed_row(extract_rows[0])] if extract_rows else []
    extracted_contents: list[RawExtractedContent] = []
    parse_logs: list[ParseLogEntry] = []

    for i, url in enumerate(job.entry_urls):
        rule = rules[i % len(rules)] if rules else None
        if rule is None:
            continue
        raw = _simulate_extract(url, rule)
        extracted_contents.append(raw)

        # 记录解析日志（parse_log）
        log = ParseLogEntry(
            record_id=f"PLOG-{job.job_id}-{i+1:04d}",
            source_url=url,
            parse_rule_id=rule.rule_id,
            parse_result="成功" if not raw.has_errors() else "部分失败",
            error_type="字段缺失" if raw.has_errors() else "",
            retry_count=1 if raw.has_errors() else 0,
            error_detail="; ".join(raw.errors) if raw.has_errors() else "",
        )
        parse_logs.append(log)
        _persist_parse_log(log)

    job.completed_pages = len(extracted_contents)
    job.failed_pages = sum(1 for r in extracted_contents if r.has_errors())
    job.status = "已完成" if job.failed_pages == 0 else "部分失败"

    # Step 4: 字段清洗（field_clean）
    clean_rules = [CleanRule.from_seed_row(cr) for cr in clean_rows if cr.get("status") == "生效中"]
    if not clean_rules:
        clean_rules = [CleanRule.from_seed_row(clean_rows[0])] if clean_rows else []
    cleaned: list[CleanedContent] = []

    for raw in extracted_contents:
        cc = _apply_clean_rules(raw, clean_rules)
        cleaned.append(cc)

    # Step 5: 批次存储（batch_store）
    batch = StorageBatch(
        batch_id=f"BATCH-{job.job_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        source_job_id=job.job_id,
        row_count=len(cleaned),
        storage_path=f"/data/batches/{job.job_id}/",
        compression="gzip",
        schema_version="v1",
        stored_at=datetime.now(timezone.utc).isoformat(),
        status="已落盘",
        records=cleaned,
    )
    batch.checksum = batch.compute_checksum()
    batch.status = "校验通过" if all(c.is_valid() for c in cleaned) else "校验失败"

    _persist_batch(batch)

    return batch


def _simulate_extract(url: str, rule: ExtractRule) -> RawExtractedContent:
    """模拟从 URL 抽取内容。

    字段映射规则：
    - 若 value 含逗号：按逗号拆分为多个字段名（种子数据模式）
    - 若 key 非 "raw"：将 key 用作字段名（测试覆盖模式）
    """
    errors: list[str] = []
    fields: dict[str, str] = {}

    if "error" in url.lower() or "404" in url:
        errors.append("页面不可达")
        return RawExtractedContent(source_url=url, extract_rule_id=rule.rule_id, errors=errors)

    if "empty" in url.lower():
        return RawExtractedContent(source_url=url, extract_rule_id=rule.rule_id,
                                   raw_fields={}, errors=["页面内容为空"])

    for key, value in rule.field_mapping.items():
        parts = [p.strip() for p in value.split(",") if p.strip()]
        if len(parts) > 1:
            for field_name in parts:
                fields[field_name] = f"extracted:{field_name}:{hash(url + field_name) % 10000}"
        elif key != "raw":
            fields[key] = f"extracted:{key}:{hash(url + key) % 10000}"
        elif parts:
            fields[parts[0]] = f"extracted:{parts[0]}:{hash(url + parts[0]) % 10000}"
        else:
            fields[key] = f"extracted:{key}:{hash(url) % 10000}"

    return RawExtractedContent(
        source_url=url, extract_rule_id=rule.rule_id,
        raw_fields=fields, errors=errors,
    )


def _apply_clean_rules(raw: RawExtractedContent, rules: list[CleanRule]) -> CleanedContent:
    """对原始抽取内容逐字段应用清洗规则。同字段多条规则链式执行。"""
    cleaned_fields: dict[str, Any] = {}
    errors: list[str] = []

    for rule in rules:
        field = rule.target_field
        # 已清洗过的字段从 cleaned_fields 读取，实现链式清洗
        raw_value = cleaned_fields.get(field, raw.raw_fields.get(field, ""))

        if not raw_value and rule.null_strategy == "skip":
            continue
        if not raw_value and rule.null_strategy == "fill_default":
            raw_value = "N/A"

        try:
            if rule.clean_type.value == "去空格":
                value = raw_value.strip()
            elif rule.clean_type.value == "标准化":
                value = raw_value.lower().replace(" ", "_")
            elif rule.clean_type.value == "校验":
                if rule.validation_range and not re.match(rule.validation_range, raw_value):
                    errors.append(f"字段 {field} 校验失败: {raw_value}")
                    value = raw_value
                else:
                    value = raw_value
            elif rule.clean_type.value == "正则提取":
                m = re.search(rule.expression, raw_value) if rule.expression else None
                value = m.group(1) if m else raw_value
            elif rule.clean_type.value == "类型转换":
                value = _cast_type(raw_value, rule.expression)
            else:
                value = raw_value
        except Exception as exc:
            errors.append(f"字段 {field} 清洗异常: {exc}")
            value = raw_value

        cleaned_fields[field] = value

    return CleanedContent(
        source_url=raw.source_url,
        extract_rule_id=raw.extract_rule_id,
        fields=cleaned_fields,
        clean_errors=errors,
    )


def _cast_type(value: str, target: str) -> Any:
    if target == "int":
        return int(value) if value.isdigit() else value
    if target == "float":
        try:
            return float(value)
        except ValueError:
            return value
    if target == "datetime":
        return value  # 简化实现
    return value


# ---------------------------------------------------------------------------
# 流水线 2: crawl_job → url_dedup → parse_log
# ---------------------------------------------------------------------------

# 指纹台账和认领回执都要在同一个临界区里读写：并发提交等价地址时，
# “查到没有指纹”和“登记指纹/认领”之间不能被另一个请求插入。
_DEDUP_LOCK = threading.RLock()
CLAIM_MODULE = "url_dedup_claim"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _window_tag(window: str) -> str:
    tag = re.sub(r"[^0-9A-Za-z]+", "", str(window))
    return tag[:24] or "now"


def _default_window() -> str:
    """未显式给窗口时按小时分桶；显式窗口才是调度真正登记的值。"""
    return datetime.now(timezone.utc).strftime("%Y%m%d%H")


def _fingerprint_lookup() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """按哈希和规范 URL 双向索引台账，兼容早期没有写规范 URL 的历史指纹。"""
    by_hash: dict[str, dict[str, Any]] = {}
    by_canonical: dict[str, dict[str, Any]] = {}
    for row in store.rows("url_dedup"):
        fp = UrlFingerprint.from_seed_row(row)
        if fp.hash_value:
            by_hash.setdefault(fp.hash_value, row)
        canonical = fp.canonical_url or UrlFingerprint.normalize(fp.source_url)
        if canonical:
            by_canonical.setdefault(canonical, row)
    return by_hash, by_canonical


def _fingerprint_payload(fp: UrlFingerprint) -> dict[str, Any]:
    return {
        "指纹编号": fp.fingerprint_id,
        "源URL": fp.source_url,
        "规范URL": fp.canonical_url or UrlFingerprint.normalize(fp.source_url),
        "指纹算法": fp.algorithm,
        "指纹哈希": fp.hash_value,
        "发现渠道": fp.discovery_channel,
        "首次发现": fp.first_seen,
        "重复次数": fp.duplicate_count,
        "去重状态": fp.status,
        "最近认领窗口": fp.window,
        "实际采集入口": fp.claimed_url,
        "回执编号": fp.receipt_id,
        "最近确认": _now_iso(),
        "重复来源": "",
    }


def _apply_fingerprint_row(row: dict[str, Any], fp: UrlFingerprint) -> None:
    """把指纹对象的确认结果写回台账行；首次发现等历史信息只在缺失时补。"""
    payload = _fingerprint_payload(fp)
    if not str(row.get("首次发现") or "").strip():
        row["首次发现"] = payload["首次发现"]
    row["指纹编号"] = row.get("指纹编号") or payload["指纹编号"]
    row["源URL"] = row.get("源URL") or payload["源URL"]
    row["规范URL"] = payload["规范URL"]
    row["指纹算法"] = row.get("指纹算法") or payload["指纹算法"]
    if not str(row.get("指纹哈希") or "").strip():
        row["指纹哈希"] = payload["指纹哈希"]
    row["发现渠道"] = row.get("发现渠道") or payload["发现渠道"]
    row["重复次数"] = fp.duplicate_count
    row["去重状态"] = fp.status
    row["最近认领窗口"] = fp.window
    row["实际采集入口"] = fp.claimed_url
    row["回执编号"] = fp.receipt_id
    row["最近确认"] = payload["最近确认"]
    # 台账列表用 status 展示去重状态，两处必须同步，刷新后才一致。
    row["status"] = fp.status


def _persist_fingerprint(fp: UrlFingerprint) -> dict[str, Any]:
    rows = store.rows("url_dedup")
    row: dict[str, Any] = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        ** _fingerprint_payload(fp),
        "status": fp.status,
        "pending": False,
        "abnormal": False,
    }
    rows.append(row)
    return row


def _own_receipt_key(hash_value: str, window: str, job_id: str, url: str):
    return (hash_value, window, job_id, str(url).strip())


def _claim_row_index() -> tuple[dict[tuple[str, str, str, str], dict[str, Any]],
                                dict[tuple[str, str], dict[str, Any]]]:
    """回执索引：own 用于同一作业重试幂等，qualification 用于窗口采集资格。

    own 用原始提交入口精确匹配：只有“同作业 + 同原始地址”才算中断重试；
    参数顺序不同但等价的地址属于新的重复发现，必须登记为重复入口。
    """
    own: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    qualified: dict[tuple[str, str], dict[str, Any]] = {}
    for row in store.rows(CLAIM_MODULE):
        hash_value = str(row.get("指纹哈希", ""))
        window = str(row.get("认领窗口", ""))
        submitted = str(row.get("提交入口", "")).strip()
        key = _own_receipt_key(
            hash_value, window, str(row.get("作业编号", "")), submitted)
        own.setdefault(key, row)
        if str(row.get("去重状态", "")) in ("实际采集", "待执行", "已采集"):
            qualified.setdefault((hash_value, window), row)
    return own, qualified


def _claim_from_row(row: dict[str, Any]) -> DedupClaim:
    def _urls(key: str) -> list[str]:
        raw = row.get(key, "")
        if isinstance(raw, (list, tuple)):
            return [str(item) for item in raw if str(item).strip()]
        return [item.strip() for item in str(raw).split(",") if item.strip()]

    return DedupClaim(
        claim_id=str(row.get("回执编号", "")),
        fingerprint_id=str(row.get("指纹编号", "")),
        hash_value=str(row.get("指纹哈希", "")),
        window=str(row.get("认领窗口", "")),
        job_id=str(row.get("作业编号", "")),
        site=str(row.get("目标站点", "")),
        submitted_url=str(row.get("提交入口", "")),
        executed_url=str(row.get("实际采集入口", "")),
        duplicate_urls=_urls("重复入口"),
        status=str(row.get("去重状态", "实际采集")),
        first_seen=str(row.get("首次发现", "")),
        confirmed_at=str(row.get("确认时间", "")),
        duplicate_events=int(row.get("重复次数", 0) or 0),
        linked_receipt_id=str(row.get("关联回执", "")),
    )


def _claim_row_payload(claim: DedupClaim) -> dict[str, Any]:
    return {
        "回执编号": claim.claim_id,
        "指纹编号": claim.fingerprint_id,
        "指纹哈希": claim.hash_value,
        "认领窗口": claim.window,
        "作业编号": claim.job_id,
        "目标站点": claim.site,
        "提交入口": claim.submitted_url,
        "实际采集入口": claim.executed_url,
        "重复入口": ",".join(claim.duplicate_urls),
        "关联回执": claim.linked_receipt_id,
        "首次发现": claim.first_seen,
        "确认时间": claim.confirmed_at,
        "去重状态": claim.status,
        "重复次数": claim.duplicate_events,
        "status": claim.status,
        "pending": claim.status in ("实际采集", "待执行"),
        "abnormal": False,
    }


def _persist_claim(claim: DedupClaim) -> dict[str, Any]:
    rows = store.rows(CLAIM_MODULE)
    row: dict[str, Any] = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "执行时间": "",
        **_claim_row_payload(claim),
    }
    rows.append(row)
    return row


def _sync_claim_row(row: dict[str, Any], claim: DedupClaim) -> None:
    row.update(_claim_row_payload(claim))


def _duplicate_source_key(job_id: str, url: str) -> str:
    # 用规范 URL 做键：交换查询参数顺序的等价地址、作业中断后的重试都只计一次。
    return f"{job_id}|{UrlFingerprint.normalize(url)}"


def _mark_fingerprint_duplicate(row: dict[str, Any], fp: UrlFingerprint,
                                job_id: str, url: str) -> bool:
    """登记一次重复确认；同一作业同一原始入口只计一次，避免重复提交重复计数。"""
    key = _duplicate_source_key(job_id, url)
    sources = [item for item in str(row.get("重复来源", "")).split(",") if item]
    if key in sources:
        return False
    sources.append(key)
    row["重复来源"] = ",".join(sources)
    fp.duplicate_count += 1
    return True


def run_dedup_flow(job: CrawlJobSpec) -> tuple[list[UrlFingerprint], list[UrlFingerprint]]:
    """对采集作业的入口 URL 执行去重，返回新增和重复列表。

    重复次数、首次发现等确认结果都会立即写回指纹台账；再次读取台账与本次
    返回值一致。同一作业重复提交同一原始地址只计一次。
    """
    with _DEDUP_LOCK:
        by_hash, by_canonical = _fingerprint_lookup()
        new_fps: list[UrlFingerprint] = []
        dup_fps: list[UrlFingerprint] = []

        for url in job.entry_urls:
            fp = UrlFingerprint.compute(url)
            row = by_hash.get(fp.hash_value) or by_canonical.get(fp.canonical_url)

            if row is None:
                new_fps.append(fp)
                row = _persist_fingerprint(fp)
                by_hash[fp.hash_value] = row
                by_canonical[fp.canonical_url] = row
                continue

            confirmed = UrlFingerprint.from_seed_row(row)
            confirmed.canonical_url = confirmed.canonical_url or fp.canonical_url
            confirmed.source_url = confirmed.source_url or fp.source_url
            # 首次发现沿用台账里的历史值，只在缺失时补当前时间。
            confirmed.first_seen = confirmed.first_seen or fp.first_seen
            _mark_fingerprint_duplicate(row, confirmed, job.job_id, url)
            _apply_fingerprint_row(row, confirmed)
            by_hash[confirmed.hash_value or row.get("指纹哈希", "")] = row
            by_canonical[confirmed.canonical_url] = row
            dup_fps.append(confirmed)

        return new_fps, dup_fps


def load_fingerprint_receipt(url_or_hash: str) -> dict[str, Any] | None:
    """从持久化台账读取指纹回执；调度和作业认领都必须走这里，不能只看内存结果。"""
    by_hash, by_canonical = _fingerprint_lookup()
    row = by_hash.get(url_or_hash)
    if row is None:
        normalized = UrlFingerprint.normalize(url_or_hash)
        row = by_canonical.get(normalized)
    if row is None:
        return None
    fp = UrlFingerprint.from_seed_row(row)
    return {
        "fingerprint": fp,
        "repeated": fp.duplicate_count,
        "first_seen": fp.first_seen,
        "window": fp.window,
        "claimed_url": fp.claimed_url,
        "receipt_id": fp.receipt_id,
        "status": fp.status,
        "canonical_url": fp.canonical_url,
    }


def fingerprint_receipt(url_or_hash: str) -> dict[str, Any] | None:
    """load_fingerprint_receipt 的别名，便于按业务叫法取回执。"""
    return load_fingerprint_receipt(url_or_hash)


def register_fingerprint(source_url: str, algorithm: str = "sha256",
                         fingerprint_id: str = "", first_seen: str | None = None,
                         discovery_channel: str | None = None,
                         status: str | None = None) -> dict[str, Any]:
    """登记一条去重指纹：规范URL、哈希、首次发现一次算好，台账与回执同源。

    同一等价地址已存在时返回既有台账行（补齐缺失字段），不产生重复指纹。
    """
    with _DEDUP_LOCK:
        fp = UrlFingerprint.compute(source_url, algorithm)
        by_hash, by_canonical = _fingerprint_lookup()
        existing = by_hash.get(fp.hash_value) or by_canonical.get(fp.canonical_url)
        if existing is not None:
            row = UrlFingerprint.from_seed_row(existing)
            row.canonical_url = row.canonical_url or fp.canonical_url
            row.first_seen = row.first_seen or fp.first_seen
            _apply_fingerprint_row(existing, row)
            return existing
        fp.fingerprint_id = fingerprint_id or fp.fingerprint_id
        fp.first_seen = first_seen or fp.first_seen
        fp.discovery_channel = discovery_channel or fp.discovery_channel
        fp.status = status or fp.status
        return _persist_fingerprint(fp)


def _claim_url_locked(job: CrawlJobSpec, url: str, window: str) -> DedupOutcome:
    """在已持锁的临界区内完成一次“确认 + 认领”，并发下只有一份能实际采集。"""
    submitted = UrlFingerprint.compute(url)
    by_hash, by_canonical = _fingerprint_lookup()
    fp_row = by_hash.get(submitted.hash_value) or by_canonical.get(submitted.canonical_url)
    is_new = fp_row is None

    if fp_row is None:
        fp_row = _persist_fingerprint(submitted)
        by_hash[submitted.hash_value] = fp_row
        by_canonical[submitted.canonical_url] = fp_row
        fp = submitted
    else:
        # 历史指纹可能没有规范URL/首次发现，这里按现有台账补全而不是覆盖。
        fp = UrlFingerprint.from_seed_row(fp_row)
        fp.canonical_url = fp.canonical_url or submitted.canonical_url
        fp.first_seen = fp.first_seen or submitted.first_seen
        fp.source_url = fp.source_url or submitted.source_url

    own_index, qualification_index = _claim_row_index()
    own_row = own_index.get(_own_receipt_key(fp.hash_value, window, job.job_id, url))
    qualification_row = qualification_index.get((fp.hash_value, window))
    receipt_id = f"CLM-{fp.hash_value[:12]}-{_window_tag(window)}"

    if own_row is not None:
        # 同一作业、同一原始入口、同一窗口：就是上一轮的中断重试。
        # 已采集过的不再执行；已认领未执行的接着执行那一份。
        claim = _claim_from_row(own_row)
        resumable = claim.status in ("实际采集", "待执行")
        _apply_fingerprint_row(fp_row, fp)
        return DedupOutcome(
            fingerprint=fp, claim=claim, is_new=False,
            is_duplicate=not resumable, can_crawl=resumable,
            submitted_urls=[url], duplicate_urls=list(claim.duplicate_urls),
            window=window,
            reason="恢复中断前的认领回执" if resumable else "上一轮已完成实际采集，重试幂等",
        )

    if fp.status == "永久排除":
        claim = DedupClaim(
            claim_id=receipt_id,
            fingerprint_id=fp.fingerprint_id, hash_value=fp.hash_value,
            window=window, job_id=job.job_id, site=job.site.domain,
            submitted_url=url, executed_url="", status="永久排除",
            first_seen=fp.first_seen, confirmed_at=_now_iso(),
        )
        _apply_fingerprint_row(fp_row, fp)
        _persist_claim(claim)
        return DedupOutcome(
            fingerprint=fp, claim=claim, is_new=is_new, is_duplicate=True,
            can_crawl=False, submitted_urls=[url], window=window,
            reason="指纹已永久排除",
        )

    if qualification_row is not None:
        # 资格已被其它作业或本窗口另一个等价地址拿走：只登记重复，不产生第二次采集。
        owner = _claim_from_row(qualification_row)
        claim = DedupClaim(
            claim_id=f"{receipt_id}-{_window_tag(job.job_id)}",
            fingerprint_id=fp.fingerprint_id, hash_value=fp.hash_value,
            window=window, job_id=job.job_id, site=job.site.domain,
            submitted_url=url, executed_url="",
            status="窗口重复", first_seen=fp.first_seen, confirmed_at=_now_iso(),
            linked_receipt_id=owner.claim_id,
        )
        changed = _mark_fingerprint_duplicate(fp_row, fp, job.job_id, url)
        if url not in owner.duplicate_urls:
            owner.duplicate_urls.append(url)
        owner.duplicate_events = len(owner.duplicate_urls)
        # 本作业没有领到采集资格：只登记自己这份重复的原始入口。
        claim.duplicate_urls = [url]
        claim.duplicate_events = owner.duplicate_events
        _apply_fingerprint_row(fp_row, fp)
        _sync_claim_row(qualification_row, owner)
        _persist_claim(claim)
        return DedupOutcome(
            fingerprint=fp, claim=claim, is_new=False, is_duplicate=True,
            can_crawl=False, submitted_urls=[url],
            duplicate_urls=list(owner.duplicate_urls), window=window,
            reason=(f"窗口 {window} 已由作业 {owner.job_id} 认领" if changed
                    else f"窗口 {window} 的重复提交已登记"),
        )

    # 本窗口第一次认领：真正获得采集资格。
    claim = DedupClaim(
        claim_id=receipt_id,
        fingerprint_id=fp.fingerprint_id, hash_value=fp.hash_value,
        window=window, job_id=job.job_id, site=job.site.domain,
        submitted_url=url, executed_url=url, status="实际采集",
        first_seen=fp.first_seen, confirmed_at=_now_iso(),
        duplicate_events=fp.duplicate_count,
    )
    fp.window = window
    fp.claimed_url = url
    fp.receipt_id = claim.claim_id
    _apply_fingerprint_row(fp_row, fp)
    _persist_claim(claim)
    return DedupOutcome(
        fingerprint=fp, claim=claim, is_new=is_new, is_duplicate=False,
        can_crawl=True, submitted_urls=[url], window=window,
        reason="获得本窗口采集资格",
    )


def claim_entry_url(job: CrawlJobSpec, url: str, window: str | None = None) -> DedupOutcome:
    """为单个入口 URL 认领采集资格，返回含持久化指纹回执的结果。"""
    return claim_dedup_window(job, window, urls=[url])[0]


def claim_dedup_window(job: CrawlJobSpec, window: str | None = None,
                       urls: list[str] | None = None) -> list[DedupOutcome]:
    """对一个窗口内的作业入口批量认领；等价地址只保留第一次实际采集。"""
    window = str(window or _default_window())
    raw_targets = job.entry_urls if urls is None else urls
    # 同一批里字面重复的原始入口只处理一次，否则第二次会被误判成“中断重试”。
    targets = list(dict.fromkeys(url for url in raw_targets if str(url).strip()))
    with _DEDUP_LOCK:
        outcomes = [_claim_url_locked(job, url, window) for url in targets]
    return outcomes


def acquire_dedup_claims(job: CrawlJobSpec, window: str | None = None) -> list[DedupOutcome]:
    """claim_dedup_window 的别名，供调度侧按“认领”语义调用。"""
    return claim_dedup_window(job, window)


def confirm_dedup_claim(job: CrawlJobSpec, window: str | None = None) -> list[DedupOutcome]:
    """去重确认（含认领）的别名。"""
    return claim_dedup_window(job, window)


def mark_claim_executed(claim_or_id: DedupClaim | str, executed_url: str | None = None) -> DedupClaim | None:
    """把认领回执标记为已实际采集；中断重试后再次调用是幂等的。"""
    claim_id = claim_or_id if isinstance(claim_or_id, str) else claim_or_id.claim_id
    with _DEDUP_LOCK:
        for row in store.rows(CLAIM_MODULE):
            if str(row.get("回执编号", "")) != claim_id:
                continue
            claim = _claim_from_row(row)
            if claim.status in ("窗口重复", "永久排除"):
                return claim
            claim.status = "已采集"
            claim.executed_url = executed_url or claim.executed_url or claim.submitted_url
            row["去重状态"] = claim.status
            row["实际采集入口"] = claim.executed_url
            row["执行时间"] = _now_iso()
            row["pending"] = False
            row["status"] = claim.status
            fingerprint_row = next(
                (r for r in store.rows("url_dedup")
                 if str(r.get("指纹哈希", "")) == claim.hash_value), None)
            if fingerprint_row is not None:
                fp = UrlFingerprint.from_seed_row(fingerprint_row)
                fp.claimed_url = claim.executed_url
                fp.window = claim.window
                fp.receipt_id = claim.claim_id
                _apply_fingerprint_row(fingerprint_row, fp)
            return claim
    return None


def execute_claimed_urls(job: CrawlJobSpec, window: str | None = None) -> list[str]:
    """认领后执行真实采集；只返回真正拿到资格并已登记的入口地址。"""
    window = str(window or _default_window())
    executed: list[str] = []
    for outcome in claim_dedup_window(job, window):
        if not outcome.can_crawl:
            continue
        mark_claim_executed(outcome.claim, outcome.claim.executed_url or outcome.claim.submitted_url)
        executed.append(outcome.claim.executed_url)
    return executed


def claim_receipts(window: str | None = None) -> list[DedupClaim]:
    """读取已持久化的认领回执列表，可按窗口过滤。"""
    claims: list[DedupClaim] = []
    for row in store.rows(CLAIM_MODULE):
        claim = _claim_from_row(row)
        if window is None or claim.window == window:
            claims.append(claim)
    return claims



# ---------------------------------------------------------------------------
# 流水线 3: proxy_pool → anti_block → crawl_limit
# ---------------------------------------------------------------------------

def select_proxy_for_job(job: CrawlJobSpec) -> ProxyNode | None:
    """从代理池为采集作业选择可用代理节点，并检查反爬策略和限速规则。

    返回最佳代理或 None（无可用代理时）。
    """
    proxies: list[ProxyNode] = []
    for row in store.rows("proxy_pool"):
        p = ProxyNode.from_seed_row(row)
        if p.is_available():
            proxies.append(p)

    if not proxies:
        return None

    # 按延迟排序，选择最快的
    proxies.sort(key=lambda p: p.latency_ms)

    # 检查反爬策略
    anti_strategies: list[AntiBlockStrategy] = []
    for row in store.rows("anti_block"):
        a = AntiBlockStrategy.from_seed_row(row)
        if a.target_site == job.site.domain and a.status == "生效中":
            anti_strategies.append(a)

    best_proxy = proxies[0]

    # 检查限速规则
    for row in store.rows("crawl_limit"):
        limit = CrawlLimitRule.from_seed_row(row)
        if limit.target_site == job.site.domain and limit.status == "生效中":
            job.concurrency = min(job.concurrency, limit.concurrency_max)
            if limit.request_interval_ms > 0:
                best_proxy.latency_ms = max(best_proxy.latency_ms, limit.request_interval_ms)

    return best_proxy


# ---------------------------------------------------------------------------
# 流水线 4: batch_store → data_delivery → quality_sample
# ---------------------------------------------------------------------------

def deliver_and_sample(batch: StorageBatch) -> tuple[DeliveryTask | None, QualitySample | None]:
    """从批次存储投递数据，并对投递结果进行质量抽样。"""
    # 数据投递
    delivery_rows = [r for r in store.rows("data_delivery")
                     if r.get("数据来源") == batch.batch_id or r.get("状态") == "待投递"]
    if delivery_rows:
        dt = DeliveryTask.from_seed_row(delivery_rows[0])
        dt.batch_ref_id = batch.batch_id
        dt.delivered_row_count = batch.row_count
        dt.last_delivery = datetime.now(timezone.utc).isoformat()
        dt.status = "已完成" if batch.status == "校验通过" else "投递失败"
        _persist_delivery(dt)
    else:
        # 自动创建投递任务
        dt = DeliveryTask(
            task_id=f"DEL-{batch.batch_id}",
            data_source=batch.batch_id,
            target_system=f"data_warehouse_{batch.source_job_id}",
            delivery_format=DeliveryFormat.CSV,
            frequency="manual",
            partition_strategy="by_date",
            batch_ref_id=batch.batch_id,
            delivered_row_count=batch.row_count,
            last_delivery=datetime.now(timezone.utc).isoformat(),
            status="已完成" if batch.status == "校验通过" else "投递失败",
        )
        _persist_delivery(dt)

    # 质量抽样
    sample_rows = [r for r in store.rows("quality_sample")
                   if r.get("目标批次") == batch.batch_id]
    if sample_rows:
        qs = QualitySample.from_seed_row(sample_rows[0])
    else:
        # 自动创建抽样计划
        sample_size = max(1, int(batch.row_count * 0.1))
        qs = QualitySample(
            plan_id=f"QS-{batch.batch_id}",
            target_batch=batch.batch_id,
            sample_ratio=0.1,
            sample_method=SampleMethod.RANDOM,
            sample_size=sample_size,
            last_sampled=datetime.now(timezone.utc).isoformat(),
            status="已通过" if batch.status == "校验通过" else "偏差告警",
        )
        _persist_sample(qs)

    # 从批次中随机抽取 sample_size 条记录进行校验
    if batch.records and qs.sample_size > 0:
        sampled = random.sample(batch.records, min(qs.sample_size, len(batch.records)))
        valid_count = sum(1 for s in sampled if s.is_valid())
        actual_ratio = valid_count / len(sampled) if sampled else 0
        qs.check_result = f"抽样{len(sampled)}条，有效{valid_count}条"
        if actual_ratio < (1 - qs.deviation_threshold):
            qs.status = "偏差告警"

    return dt, qs


# ---------------------------------------------------------------------------
# 流水线 5: incr_crawl → crawl_job → diff_check
# ---------------------------------------------------------------------------

def run_incremental_flow(cursor: IncrCursor,
                         previous_batch: StorageBatch | None,
                         current_data: list[CleanedContent]) -> DiffRecord | None:
    """增量采集流程：比较增量游标与当前批次，生成差异检测记录。"""
    if previous_batch is None or not previous_batch.records:
        return None

    prev_fields = {r.source_url: r.fields for r in previous_batch.records}
    curr_fields = {r.source_url: r.fields for r in current_data}

    # 检测新增
    new_urls = set(curr_fields) - set(prev_fields)
    # 检测变更
    changed_urls = {url for url in (set(curr_fields) & set(prev_fields))
                    if curr_fields[url] != prev_fields[url]}
    # 检测删除
    removed_urls = set(prev_fields) - set(curr_fields)

    total_changes = len(new_urls) + len(changed_urls) + len(removed_urls)
    if total_changes == 0:
        return None

    diff = DiffRecord(
        record_id=f"DIFF-{cursor.cursor_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        target_site=cursor.target_site,
        page_url=",".join(list(new_urls | changed_urls)[:5]),
        change_type=_classify_change_type(len(new_urls), len(changed_urls), len(removed_urls)),
        old_summary=f"原{len(prev_fields)}条记录",
        new_summary=f"新{len(curr_fields)}条记录，新增{len(new_urls)}变更{len(changed_urls)}删除{len(removed_urls)}",
        affected_fields=list(curr_fields[next(iter(curr_fields))]) if curr_fields else [],
    )

    _persist_diff(diff)

    # 更新游标
    cursor.new_count = len(new_urls)
    cursor.updated_count = len(changed_urls)
    cursor.last_cursor = cursor.current_cursor
    cursor.current_cursor = datetime.now(timezone.utc).isoformat()
    cursor.last_sync = cursor.current_cursor
    cursor.status = "已追上"

    return diff


def _classify_change_type(new_count: int, changed_count: int, removed_count: int) -> str:
    if new_count > 0 and changed_count == 0 and removed_count == 0:
        return "新增内容"
    if changed_count > 0 and new_count == 0 and removed_count == 0:
        return "内容变更"
    if removed_count > 0 and new_count == 0 and changed_count == 0:
        return "内容删除"
    return "混合变更"


# ---------------------------------------------------------------------------
# 流水线 6: data_archive ← batch_store
# ---------------------------------------------------------------------------

def archive_batch(batch: StorageBatch) -> ArchivePolicy | None:
    """从批次存储归档符合条件的批次数据。"""
    # 优先查找与当前批次相关的策略
    matched_policies: list[ArchivePolicy] = []
    policies: list[ArchivePolicy] = []
    for row in store.rows("data_archive"):
        p = ArchivePolicy.from_seed_row(row)
        if p.status in ("待触发", "已完成"):
            policies.append(p)
            if batch.batch_id in p.data_scope or p.data_scope == batch.batch_id:
                matched_policies.append(p)

    # 优先使用匹配的策略
    if matched_policies:
        policy = matched_policies[0]
        policy.last_archive = datetime.now(timezone.utc).isoformat()
        policy.status = "已完成"
        return policy

    if not policies:
        # 创建默认归档策略
        policy = ArchivePolicy(
            policy_id=f"ARCH-{batch.batch_id}",
            data_scope=batch.batch_id,
            archive_cycle="on_demand",
            retention_days=90,
            storage_location=f"/archive/{batch.batch_id}/",
            last_archive=datetime.now(timezone.utc).isoformat(),
            status="已完成",
        )
        _persist_archive_policy(policy)
        return policy

    policy = policies[0]
    policy.last_archive = datetime.now(timezone.utc).isoformat()
    policy.status = "已完成"
    return policy


# ---------------------------------------------------------------------------
# 流水线 7: crawl_cost ← proxy_pool
# ---------------------------------------------------------------------------

def compute_crawl_cost(job: CrawlJobSpec, proxy: ProxyNode | None) -> CostRecord:
    """根据采集作业和代理使用情况计算采集成本。"""
    proxy_hours = job.completed_pages * 0.01 if proxy else 0
    data_mb = job.completed_pages * 0.5  # 每页 0.5MB 估算

    record = CostRecord(
        record_id=f"COST-{job.job_id}",
        site=job.site.domain,
        billing_cycle=datetime.now(timezone.utc).strftime("%Y-%m"),
        request_count=job.completed_pages + job.failed_pages,
        data_volume_mb=round(data_mb, 2),
        proxy_usage_hours=round(proxy_hours, 2),
        estimated_cost=round(data_mb * 0.001 + proxy_hours * 0.5, 4),
        status="已核算" if job.status in ("已完成", "部分失败") else "统计中",
    )
    _persist_cost(record)
    return record


# ---------------------------------------------------------------------------
# 流水线 8: crawl_job → scheduler
# ---------------------------------------------------------------------------

def schedule_job(job: CrawlJobSpec) -> SchedulePlan | None:
    """为采集作业创建调度计划。"""
    # 找关联的调度计划
    for row in store.rows("scheduler"):
        if str(row.get("关联作业")) == job.job_id:
            plan = SchedulePlan.from_seed_row(row)
            plan.next_run = datetime.now(timezone.utc).isoformat()
            plan.status = "执行中" if job.status in ("待执行", "采集执行") else "已完成"
            return plan

    # 创建新的调度计划
    plan = SchedulePlan(
        plan_id=f"SCHED-{job.job_id}",
        linked_job_id=job.job_id,
        cron_expr="0 */6 * * *",  # 每6小时
        next_run=datetime.now(timezone.utc).isoformat(),
        status="执行中" if job.status in ("待执行", "采集执行") else "已完成",
    )
    _persist_schedule(plan)
    return plan


def _find_schedule_row(plan_id: str) -> dict[str, Any] | None:
    for row in store.rows("scheduler"):
        if str(row.get("计划编号", "")) == plan_id:
            return row
    return None


def _find_job_row(job_id: str) -> dict[str, Any] | None:
    for row in store.rows("crawl_job"):
        if str(row.get("作业编号", "")) == job_id:
            return row
    return None


def _claim_snapshot(outcome: DedupOutcome) -> dict[str, Any]:
    """调度登记用的一份完整确认结果：窗口、领到的地址、执行和重复的原始入口。

    重复次数取的是持久化台账的最新值，而不是某个局部对象，调度与页面看到的一致。
    """
    receipt = load_fingerprint_receipt(outcome.fingerprint.hash_value) or {}
    executed = outcome.claim.executed_url or str(receipt.get("claimed_url") or "")
    duplicate_urls = list(outcome.claim.duplicate_urls)
    # 窗口重复的回执只是登记，实际采集属于领到资格的那份回执，不能算到本作业头上。
    attributed_executed = executed if outcome.can_crawl or outcome.claim.status == "已采集" else ""
    if outcome.can_crawl and outcome.claim.submitted_url != attributed_executed:
        # 去重命中的原始入口仍然保留，便于核对“作业实际采集的入口网址”。
        duplicate_urls.append(outcome.claim.submitted_url)
    return {
        "指纹编号": outcome.fingerprint.fingerprint_id,
        "指纹哈希": outcome.fingerprint.hash_value,
        "回执编号": outcome.claim.claim_id or str(receipt.get("receipt_id") or ""),
        "认领窗口": outcome.window or str(receipt.get("window") or ""),
        "实际采集入口": attributed_executed,
        "重复入口": ",".join(dict.fromkeys(duplicate_urls)),
        "重复次数": int(receipt.get("repeated", outcome.fingerprint.duplicate_count) or 0),
        "首次发现": str(receipt.get("first_seen") or outcome.fingerprint.first_seen),
        "作业编号": outcome.claim.job_id,
        "目标站点": outcome.claim.site,
    }


def sync_claims_to_jobs(outcomes: list[DedupOutcome]) -> list[dict[str, Any]]:
    """把去重确认结果同步到采集作业认领列表（crawl_job 行上的认领字段）。"""
    by_job: dict[str, dict[str, Any]] = {}
    for outcome in outcomes:
        snapshot = _claim_snapshot(outcome)
        job_id = snapshot["作业编号"]
        aggregate = by_job.setdefault(job_id, {
            "认领窗口": snapshot["认领窗口"],
            "实际采集入口": [],
            "重复入口": [],
            "去重回执": [],
            "去重状态": "已认领",
        })
        aggregate["认领窗口"] = aggregate["认领窗口"] or snapshot["认领窗口"]
        if snapshot["实际采集入口"]:
            aggregate["实际采集入口"].append(snapshot["实际采集入口"])
        if snapshot["重复入口"]:
            aggregate["重复入口"].append(snapshot["重复入口"])
        if snapshot["回执编号"]:
            aggregate["去重回执"].append(snapshot["回执编号"])

    synced: list[dict[str, Any]] = []
    for job_id, aggregate in by_job.items():
        row = _find_job_row(job_id)
        if row is None:
            # 作业尚在内存规格里（未登记到台账）时也能从回执表取到认领列表。
            synced.append({"作业编号": job_id, **{
                "认领窗口": aggregate["认领窗口"],
                "实际采集入口": list(dict.fromkeys(aggregate["实际采集入口"])),
                "重复入口": list(dict.fromkeys(aggregate["重复入口"])),
                "去重回执": list(dict.fromkeys(aggregate["去重回执"])),
                "去重状态": aggregate["去重状态"],
            }})
            continue
        executed = list(dict.fromkeys(aggregate["实际采集入口"]))
        duplicates = [url for url in dict.fromkeys(aggregate["重复入口"])
                      if url not in executed]
        row["认领窗口"] = row.get("认领窗口") or aggregate["认领窗口"]
        row["实际采集入口"] = ",".join(executed)
        row["重复入口"] = ",".join(duplicates)
        row["去重回执"] = ",".join(dict.fromkeys(aggregate["去重回执"]))
        row["去重状态"] = "已去重" if duplicates else aggregate["去重状态"]
        synced.append({
            "作业编号": job_id,
            "认领窗口": row["认领窗口"],
            "实际采集入口": executed,
            "重复入口": duplicates,
            "去重回执": row["去重回执"],
            "去重状态": row["去重状态"],
        })
    return synced


def job_claim_list(job_id: str | None = None) -> list[dict[str, Any]]:
    """采集作业认领列表：来自持久化回执表，页面刷新后与写入时一致。"""
    rows: list[dict[str, Any]] = []
    for claim in claim_receipts():
        if job_id is not None and claim.job_id != job_id:
            continue
        receipt = load_fingerprint_receipt(claim.hash_value) or {}
        rows.append({
            "作业编号": claim.job_id,
            "目标站点": claim.site,
            "回执编号": claim.claim_id,
            "指纹编号": claim.fingerprint_id,
            "指纹哈希": claim.hash_value,
            "认领窗口": claim.window,
            "提交入口": claim.submitted_url,
            "实际采集入口": claim.executed_url,
            "重复入口": ",".join(claim.duplicate_urls),
            "重复次数": int(receipt.get("repeated", claim.duplicate_events) or 0),
            "首次发现": claim.first_seen or str(receipt.get("first_seen") or ""),
            "去重状态": claim.status,
            "可实际采集": claim.can_crawl,
        })
    return rows


def dispatch_with_dedup(job: CrawlJobSpec,
                        window: str | None = None) -> tuple[SchedulePlan | None, list[DedupOutcome]]:
    """调度执行入口：先认领，再按同一份持久化回执登记窗口和地址，最后执行。

    - 认领与指纹确认共用一把锁和一份台账，同一窗口并发提交只产生一次实际采集；
    - 回执里的窗口和地址在调度计划上登记，便于下一次独立窗口继续处理；
    - 真实执行的入口和窗口内重复的原始入口都写进调度回执。
    """
    window = str(window or _default_window())
    outcomes = claim_dedup_window(job, window)

    # 先按认领回执登记窗口和地址（此时还没执行）。
    plan = schedule_job(job)
    schedule_row = _find_schedule_row(plan.plan_id) if plan is not None else None
    snapshots = [_claim_snapshot(outcome) for outcome in outcomes]
    if schedule_row is not None:
        schedule_row["认领窗口"] = window
        schedule_row["认领地址"] = ",".join(
            dict.fromkeys(s["实际采集入口"] for s in snapshots if s["实际采集入口"]))
        schedule_row["去重回执"] = ",".join(
            dict.fromkeys(s["回执编号"] for s in snapshots if s["回执编号"]))
        schedule_row["指纹台账"] = ",".join(
            dict.fromkeys(s["指纹哈希"] for s in snapshots if s["指纹哈希"]))

    # 只有领到资格的回执才真实执行；窗口内重复的原始入口只登记不执行。
    executed_urls: list[str] = []
    duplicate_urls: list[str] = []
    for outcome in outcomes:
        snapshot = _claim_snapshot(outcome)
        if snapshot["重复入口"]:
            duplicate_urls.extend(snapshot["重复入口"].split(","))
        if outcome.can_crawl:
            mark_claim_executed(outcome.claim, outcome.claim.executed_url or outcome.claim.submitted_url)
            executed_urls.append(outcome.claim.executed_url or outcome.claim.submitted_url)

    if schedule_row is not None:
        schedule_row["实际执行入口"] = ",".join(dict.fromkeys(executed_urls))
        schedule_row["重复原始入口"] = ",".join(dict.fromkeys(duplicate_urls))
        schedule_row["实际采集数"] = len(dict.fromkeys(executed_urls))
        schedule_row["重复提交数"] = len(dict.fromkeys(duplicate_urls))
        schedule_row["最近执行"] = _now_iso()
        # 执行后再从同一份持久化回执取一次数，确认登记值与台账一致。
        # 同一指纹在一批入口里只算一次，避免等价地址被重复累加。
        unique_hashes = dict.fromkeys(s["指纹哈希"] for s in snapshots if s["指纹哈希"])
        schedule_row["去重确认次数"] = sum(
            int(receipt["repeated"]) for receipt in
            (load_fingerprint_receipt(h) for h in unique_hashes) if receipt)
        schedule_row["实际执行入口"] = ",".join(dict.fromkeys(executed_urls))
        schedule_row["认领地址"] = schedule_row["实际执行入口"] or schedule_row["认领地址"]

    sync_claims_to_jobs(outcomes)
    return plan, outcomes



# ---------------------------------------------------------------------------
# 跨模块失败处理和恢复逻辑
# ---------------------------------------------------------------------------

def recover_failed_batch(batch: StorageBatch) -> StorageBatch | None:
    """恢复失败的批次：重新对缺失记录执行抽取和清洗。"""
    if batch.status != "校验失败":
        return None

    # 找出失败记录
    failed_records = [r for r in batch.records if not r.is_valid()]
    if not failed_records:
        return None

    # 重试：对失败记录重新填充缺失字段
    for record in failed_records:
        for field, value in list(record.fields.items()):
            if not value or value == "":
                record.fields[field] = f"recovered:{field}"
        # 恢复后清除之前清洗错误标记
        record.clean_errors = []

    # 重新校验
    batch.checksum = batch.compute_checksum()
    batch.status = "校验通过" if all(r.is_valid() for r in batch.records) else "校验失败"
    batch.stored_at = datetime.now(timezone.utc).isoformat()
    return batch


def handle_duplicate_batch(batch: StorageBatch) -> StorageBatch | None:
    """处理重复批次：检查是否与已有批次产生冲突。"""
    existing_batch_ids = set()
    for row in store.rows("batch_store"):
        existing_batch_ids.add(str(row.get("批次编号")))

    if batch.batch_id in existing_batch_ids:
        # 重复批次：追加时间戳后缀
        suffix = datetime.now(timezone.utc).strftime("%H%M%S")
        batch.batch_id = f"{batch.batch_id}-{suffix}"
        batch.storage_path = f"/data/batches/{batch.source_job_id}-retry/"

    return batch


def handle_interrupted_pipeline(job: CrawlJobSpec) -> CrawlJobSpec:
    """处理中断的采集管线：恢复未完成的工作。"""
    if job.status not in ("部分失败", "待执行"):
        return job

    # 找到失败的页面并标记重试
    if job.failed_pages > 0:
        job.failed_pages = max(0, job.failed_pages - 1)  # 重试一页
        job.status = "采集执行"
        if job.failed_pages + job.completed_pages == 0:
            job.status = "已完成"

    return job


# ---------------------------------------------------------------------------
# 内部持久化辅助
# ---------------------------------------------------------------------------

def _persist_batch(batch: StorageBatch) -> None:
    rows = store.rows("batch_store")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "批次编号": batch.batch_id,
        "来源作业": batch.source_job_id,
        "数据行数": batch.row_count,
        "存储路径": batch.storage_path,
        "压缩格式": batch.compression,
        "Schema版本": batch.schema_version,
        "校验摘要": batch.checksum[:16],
        "入库时间": batch.stored_at,
        "status": batch.status,
        "pending": False,
        "abnormal": batch.status == "校验失败",
    })


def _persist_parse_log(log: ParseLogEntry) -> None:
    rows = store.rows("parse_log")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "记录编号": log.record_id,
        "来源URL": log.source_url,
        "解析规则": log.parse_rule_id,
        "解析结果": log.parse_result,
        "异常类型": log.error_type,
        "重试次数": log.retry_count,
        "错误详情": log.error_detail,
        "记录时间": log.recorded_at,
        "status": "成功" if not log.error_type else "格式异常",
        "pending": False,
        "abnormal": bool(log.error_type),
    })


def _persist_delivery(dt: DeliveryTask) -> None:
    rows = store.rows("data_delivery")
    rows.append(dict(
        id=max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        **{f: (getattr(dt, k) if not isinstance(getattr(dt, k), DeliveryFormat) else getattr(dt, k).value) for f, k in [
            ("任务编号", "task_id"), ("数据来源", "data_source"),
            ("目标系统", "target_system"), ("投递格式", "delivery_format"),
            ("投递频率", "frequency"), ("分区策略", "partition_strategy"),
            ("最近投递", "last_delivery"), ("投递状态", "status"),
        ]},
        status=dt.status, pending=False, abnormal=dt.status == "投递失败",
    ))


def _persist_sample(qs: QualitySample) -> None:
    rows = store.rows("quality_sample")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "计划编号": qs.plan_id, "目标批次": qs.target_batch,
        "抽样比例": str(qs.sample_ratio), "抽样方式": qs.sample_method.value,
        "样本数量": qs.sample_size, "检验结果": qs.check_result,
        "偏差阈值": str(qs.deviation_threshold), "最近采样": qs.last_sampled,
        "status": qs.status, "pending": False,
        "abnormal": qs.status == "偏差告警",
    })


def _persist_diff(diff: DiffRecord) -> None:
    rows = store.rows("diff_check")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": diff.record_id, "目标站点": diff.target_site,
        "检测页面": diff.page_url, "变更类型": diff.change_type,
        "旧内容摘要": diff.old_summary, "新内容摘要": diff.new_summary,
        "影响字段": ",".join(diff.affected_fields), "检测时间": diff.detected_at,
        "status": diff.status, "pending": False, "abnormal": False,
    })


def _persist_archive_policy(p: ArchivePolicy) -> None:
    rows = store.rows("data_archive")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "策略编号": p.policy_id, "数据范围": p.data_scope,
        "归档周期": p.archive_cycle, "归档格式": str(p.archive_format),
        "保留时长": str(p.retention_days), "存储位置": p.storage_location,
        "压缩方式": p.compression_method, "最后归档": p.last_archive,
        "status": p.status, "pending": False, "abnormal": False,
    })


def _persist_cost(c: CostRecord) -> None:
    rows = store.rows("crawl_cost")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": c.record_id, "采集站点": c.site,
        "计费周期": c.billing_cycle, "请求次数": str(c.request_count),
        "数据流量": str(c.data_volume_mb), "代理用量": str(c.proxy_usage_hours),
        "费用预估": c.estimated_cost, "账单状态": c.status,
        "status": c.status, "pending": False,
        "abnormal": c.estimated_cost > 100,
    })


def _persist_schedule(plan: SchedulePlan) -> None:
    rows = store.rows("scheduler")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "计划编号": plan.plan_id, "关联作业": plan.linked_job_id,
        "Cron表达式": plan.cron_expr, "时区设置": plan.timezone_setting,
        "依赖检查": plan.dependency_check, "失败通知": plan.failure_notify,
        "并发控制": plan.concurrency_control, "下次执行": plan.next_run,
        "认领窗口": "", "认领地址": "", "去重回执": "", "指纹台账": "",
        "实际执行入口": "", "重复原始入口": "", "实际采集数": 0,
        "重复提交数": 0, "去重确认次数": 0, "最近执行": "",
        "status": plan.status, "pending": False, "abnormal": False,
    })