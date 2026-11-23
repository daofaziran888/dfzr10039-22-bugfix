"""URL 去重指纹回执与窗口认领的回归测试。

覆盖：
- 重复次数写回台账，重新读取/刷新后一致；
- 交换查询参数顺序的等价地址命中同一指纹；
- 同一窗口并发提交只产生一次实际采集，下一次独立窗口仍可处理；
- 去重确认同步到采集作业认领列表，调度从同一份持久化回执取数并登记；
- 历史指纹（缺字段）兼容、首次发现保留；
- 认领中断后重试只产生一次有效采集，新窗口记录不丢失。
"""
from __future__ import annotations

import threading
from uuid import uuid4

from app.integration import (
    claim_dedup_window,
    claim_receipts,
    dispatch_with_dedup,
    job_claim_list,
    load_fingerprint_receipt,
    mark_claim_executed,
    run_dedup_flow,
    sync_claims_to_jobs,
    _find_job_row,
    _find_schedule_row,
)
from app.models import CrawlJobSpec, SiteConfig, SiteType, UrlFingerprint
from app.store import store


def _job(job_id: str, urls: list[str]) -> CrawlJobSpec:
    return CrawlJobSpec(
        job_id=job_id,
        site=SiteConfig(
            site_id="S1", domain="test.com", site_type=SiteType.NEWS,
            robots_policy="allow", language="zh-CN", region="CN",
            access_method="direct", registered_at="2026-01-01",
        ),
        entry_urls=urls,
    )


def _ledger_rows(url: str) -> list[dict]:
    h = UrlFingerprint.compute(url).hash_value
    return [row for row in store.rows("url_dedup") if row.get("指纹哈希") == h]


def _unique_url(tag: str) -> str:
    return f"https://dedup-{uuid4().hex[:8]}.test/{tag}"


# ---------------------------------------------------------------------------
# 重复次数持久化
# ---------------------------------------------------------------------------

def test_duplicate_count_persisted_to_ledger() -> None:
    """重复次数必须在台账上可见：返回值和重新读取的台账一致。"""
    url = _unique_url("count")
    run_dedup_flow(_job("JOB-COUNT-1", [url]))

    new2, dup2 = run_dedup_flow(_job("JOB-COUNT-2", [url]))
    assert new2 == []
    assert dup2[0].duplicate_count == 1

    rows = _ledger_rows(url)
    assert len(rows) == 1
    assert rows[0]["重复次数"] == 1, "重复次数没有写回台账"

    receipt = load_fingerprint_receipt(url)
    assert receipt is not None
    assert receipt["repeated"] == 1
    assert receipt["first_seen"] == rows[0]["首次发现"]


def test_duplicate_count_query_param_order_counts_once() -> None:
    """参数顺序交换的等价地址只产生一个指纹，重复次数记在同一个台账行。"""
    base = _unique_url("order")
    u1 = f"{base}?a=1&b=2"
    u2 = f"{base}?b=2&a=1"

    run_dedup_flow(_job("JOB-ORDER-1", [u1]))
    _, dup = run_dedup_flow(_job("JOB-ORDER-2", [u2]))
    assert len(dup) == 1
    assert dup[0].duplicate_count == 1

    rows = _ledger_rows(u1)
    assert len(rows) == 1, "等价地址不应新增第二条指纹"
    assert rows[0]["重复次数"] == 1


def test_duplicate_count_stable_on_same_job_retry() -> None:
    """同一作业重复提交同一原始入口只计一次，不会随重试次数膨胀。"""
    url = _unique_url("retry-count")
    run_dedup_flow(_job("JOB-STABLE", [url]))
    run_dedup_flow(_job("JOB-STABLE", [url]))
    run_dedup_flow(_job("JOB-STABLE", [url]))
    assert _ledger_rows(url)[0]["重复次数"] == 1


def test_duplicate_counts_accumulate_across_jobs() -> None:
    """跨作业的每次首次发现都要累加。"""
    url = _unique_url("accum")
    run_dedup_flow(_job("JOB-ACC-0", [url]))
    run_dedup_flow(_job("JOB-ACC-1", [url]))
    run_dedup_flow(_job("JOB-ACC-2", [url]))
    assert _ledger_rows(url)[0]["重复次数"] == 2


# ---------------------------------------------------------------------------
# 窗口认领与并发
# ---------------------------------------------------------------------------

def test_concurrent_window_claims_only_one_crawl() -> None:
    """同一窗口并发提交等价地址：只有一个作业获得采集资格。"""
    base = _unique_url("race")
    u1, u2 = f"{base}?x=1&y=2", f"{base}?y=2&x=1"
    window = f"WIN-{uuid4().hex[:6]}"
    jobs = [_job("JOB-RACE-A", [u1]), _job("JOB-RACE-B", [u2])]

    results: list = []
    barrier = threading.Barrier(len(jobs))

    def worker(job: CrawlJobSpec) -> None:
        barrier.wait()
        results.append(claim_dedup_window(job, window))

    threads = [threading.Thread(target=worker, args=(j,)) for j in jobs]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    outcomes = [o for batch in results for o in batch]
    assert len([o for o in outcomes if o.can_crawl]) == 1, \
        f"同一窗口应只有一次实际采集，实际: {[(o.claim.job_id, o.can_crawl) for o in outcomes]}"
    assert len(_ledger_rows(u1)) == 1, "并发提交不应产生重复指纹行"

    qualified = [c for c in claim_receipts(window) if c.status in ("实际采集", "已采集")]
    assert len(qualified) == 1
    assert qualified[0].duplicate_urls, "窗口内第二个等价地址应登记为重复入口"


def test_next_independent_window_can_crawl() -> None:
    """同一地址在下一个独立窗口仍可处理。"""
    url = _unique_url("next-window")
    job = _job("JOB-WINDOW", [url])

    first = claim_dedup_window(job, "WIN-FIRST")
    assert first[0].can_crawl
    mark_claim_executed(first[0].claim)

    second = claim_dedup_window(job, "WIN-SECOND")
    assert second[0].can_crawl, "新窗口应重新获得采集资格"
    assert second[0].claim.claim_id != first[0].claim.claim_id


def test_claim_interrupted_then_retried_once() -> None:
    """认领后执行前中断：重试继续原回执，只产生一次有效采集。"""
    url = _unique_url("interrupt")
    job = _job("JOB-INTERRUPT", [url])
    window = "WIN-INTERRUPT"

    granted = claim_dedup_window(job, window)
    assert granted[0].can_crawl
    # 模拟中断：拿到认领但没有 mark_claim_executed。

    retried = claim_dedup_window(job, window)
    assert retried[0].can_crawl, "中断后同一作业应能继续执行原回执"
    assert retried[0].claim.claim_id == granted[0].claim.claim_id
    mark_claim_executed(retried[0].claim)

    after_execute = claim_dedup_window(job, window)
    assert not after_execute[0].can_crawl, "已执行后重试不得再次采集"
    assert _ledger_rows(url)[0]["重复次数"] == 0, "中断重试不应误记重复"

    new_window = claim_dedup_window(job, "WIN-INTERRUPT-NEXT")
    assert new_window[0].can_crawl, "新窗口的记录不能丢"


def test_claim_after_write_interruption_retry() -> None:
    """写入与认领中断后重试：上一轮只一次有效采集，新窗口记录不丢失。"""
    url = _unique_url("write-retry")
    job = _job("JOB-WRITE-RETRY", [url])
    window = "WIN-WRITE"

    first = claim_dedup_window(job, window)[0]
    assert first.can_crawl
    # 第一次执行模拟中途失败：只登记回执，不落执行时间。
    receipt = load_fingerprint_receipt(url)
    assert receipt["receipt_id"] == first.claim.claim_id

    mark_claim_executed(first.claim)
    executed = [c for c in claim_receipts(window) if c.status == "已采集"]
    assert len(executed) == 1

    # 再次提交同一窗口：不再执行，台账也不新增指纹。
    again = claim_dedup_window(job, window)[0]
    assert not again.can_crawl
    assert len(_ledger_rows(url)) == 1

    # 新窗口依然可以认领，且回执独立。
    next_window = claim_dedup_window(job, "WIN-WRITE-NEXT")[0]
    assert next_window.can_crawl
    assert next_window.window == "WIN-WRITE-NEXT"
    assert len([c for c in claim_receipts("WIN-WRITE-NEXT")]) == 1


def test_new_window_after_interrupt_records_fresh_receipt() -> None:
    """新窗口执行后，回执独立登记，台账只保留一条指纹且窗口为最新值。"""
    url = _unique_url("fresh-window")
    job = _job("JOB-FRESH", [url])

    first = claim_dedup_window(job, "WIN-FRESH-1")[0]
    mark_claim_executed(first.claim)
    second = claim_dedup_window(job, "WIN-FRESH-2")[0]
    assert second.can_crawl
    mark_claim_executed(second.claim)

    windows = {c.window for c in claim_receipts() if c.job_id == "JOB-FRESH"}
    assert windows == {"WIN-FRESH-1", "WIN-FRESH-2"}
    receipt = load_fingerprint_receipt(url)
    assert receipt["window"] == "WIN-FRESH-2"
    assert len(_ledger_rows(url)) == 1


# ---------------------------------------------------------------------------
# 调度与作业认领列表
# ---------------------------------------------------------------------------

def test_dispatch_registers_window_and_urls() -> None:
    """调度从同一份回执取数，登记窗口、实际执行和重复的原始入口。"""
    base = _unique_url("dispatch")
    u1, u2 = f"{base}?k=2&z=1", f"{base}?z=1&k=2"
    job_id = f"JOB-DISPATCH-{uuid4().hex[:6]}"
    store.rows("crawl_job").append({
        "id": 9000, "status": "待执行", "pending": True, "abnormal": False,
        "作业编号": job_id, "目标站点": "test.com",
        "入口URL": f"{u1},{u2}",
        "最大深度": "3", "并发线程": "4", "调度策略": "interval",
        "完成页数": "0", "失败页数": "0",
    })

    job = CrawlJobSpec.from_seed_row(_find_job_row(job_id))
    plan, outcomes = dispatch_with_dedup(job, "WIN-DISPATCH")
    assert plan is not None
    schedule_row = _find_schedule_row(plan.plan_id)
    assert schedule_row["认领窗口"] == "WIN-DISPATCH"
    assert schedule_row["实际执行入口"] == u1
    assert schedule_row["重复原始入口"] == u2
    assert schedule_row["实际采集数"] == 1
    assert schedule_row["重复提交数"] == 1
    assert schedule_row["去重回执"]

    job_row = _find_job_row(job_id)
    assert job_row["实际采集入口"] == u1
    assert job_row["重复入口"] == u2
    assert job_row["认领窗口"] == "WIN-DISPATCH"

    receipt = load_fingerprint_receipt(u2)
    assert receipt["repeated"] == 1
    assert receipt["claimed_url"] == u1
    assert receipt["window"] == "WIN-DISPATCH"


def test_claims_synced_to_job_claim_list() -> None:
    """去重确认结果同步到采集作业认领列表，刷新后仍可读到同一份回执。"""
    url = _unique_url("sync")
    job_id = f"JOB-SYNC-{uuid4().hex[:6]}"
    store.rows("crawl_job").append({
        "id": 9001, "status": "待执行", "pending": True, "abnormal": False,
        "作业编号": job_id, "目标站点": "test.com", "入口URL": url,
        "最大深度": "3", "并发线程": "4", "调度策略": "interval",
        "完成页数": "0", "失败页数": "0",
    })
    job = CrawlJobSpec.from_seed_row(_find_job_row(job_id))
    outcomes = claim_dedup_window(job, "WIN-SYNC")
    sync_claims_to_jobs(outcomes)
    mark_claim_executed(outcomes[0].claim)

    claims = job_claim_list(job_id)
    assert len(claims) == 1
    assert claims[0]["认领窗口"] == "WIN-SYNC"
    assert claims[0]["实际采集入口"] == url
    assert claims[0]["去重状态"] == "已采集"
    assert claims[0]["回执编号"] == outcomes[0].claim.claim_id


def test_scheduler_immediate_run_uses_receipt() -> None:
    """调度「立即执行」从同一份回执取数，登记窗口、执行和重复原始入口。"""
    from app.services.scheduler import SchedulerService

    base = _unique_url("sched-svc")
    u1, u2 = f"{base}?m=2&n=1", f"{base}?n=1&m=2"
    job_id = f"JOB-SCHED-{uuid4().hex[:6]}"
    plan_id = f"SP-{uuid4().hex[:6]}"
    store.rows("crawl_job").append({
        "id": 9002, "status": "待执行", "pending": True, "abnormal": False,
        "作业编号": job_id, "目标站点": "test.com", "入口URL": f"{u1},{u2}",
        "最大深度": "3", "并发线程": "4", "调度策略": "interval",
        "完成页数": "0", "失败页数": "0",
    })
    scheduler_rows = store.rows("scheduler")
    scheduler_rows.append({
        "id": 9003, "status": "待激活", "pending": True, "abnormal": False,
        "计划编号": plan_id, "关联作业": job_id, "Cron表达式": "0 */6 * * *",
        "时区设置": "Asia/Shanghai", "依赖检查": "", "失败通知": "",
        "并发控制": "WIN-SVC", "下次执行": "",
        "认领窗口": "", "认领地址": "", "去重回执": "", "指纹台账": "",
        "实际执行入口": "", "重复原始入口": "", "实际采集数": 0,
        "重复提交数": 0, "去重确认次数": 0, "最近执行": "",
    })

    entry, message = SchedulerService().run_action(
        9003, "立即执行", remark="WIN-SVC-2")
    assert entry is not None, message
    schedule_row = _find_schedule_row(plan_id)
    assert schedule_row["认领窗口"] == "WIN-SVC-2"
    assert schedule_row["实际执行入口"] == u1
    assert schedule_row["重复原始入口"] == u2
    assert schedule_row["实际采集数"] == 1
    assert schedule_row["重复提交数"] == 1
    receipt = load_fingerprint_receipt(u2)
    assert receipt["repeated"] == 1
    assert receipt["window"] == "WIN-SVC-2"


def test_partial_write_missing_receipt_recovers_once() -> None:
    """指纹已写入但认领回执没落盘时，重试补发一次资格，再次重试不重复采集。"""
    url = _unique_url("partial-write")
    hash_value = UrlFingerprint.compute(url).hash_value
    store.rows("url_dedup").append({
        "id": 9200, "指纹编号": f"FP-{hash_value[:12]}", "源URL": url,
        "指纹算法": "sha256", "指纹哈希": hash_value,
        "发现渠道": "crawl_job", "首次发现": "2026-03-03T00:00:00+00:00",
        "重复次数": 0, "去重状态": "已记录", "status": "已记录",
        "pending": False, "abnormal": False,
    })
    job = _job("JOB-PARTIAL", [url])
    window = "WIN-PARTIAL"

    first = claim_dedup_window(job, window)[0]
    assert first.can_crawl, "回执缺失时应补发实际采集资格"
    mark_claim_executed(first.claim)

    retried = claim_dedup_window(job, window)[0]
    assert not retried.can_crawl, "补齐回执后重试不得重复采集"
    assert len(_ledger_rows(url)) == 1
    assert len(claim_receipts(window)) == 1


# ---------------------------------------------------------------------------
# 历史指纹兼容
# ---------------------------------------------------------------------------

def test_legacy_fingerprint_receipt_compatible() -> None:
    """历史指纹只带源URL/哈希/重复次数时，仍能被认领且首次发现不被覆盖。"""
    url = f"{_unique_url('legacy')}?a=2&b=1"
    equivalent = url.replace("a=2&b=1", "b=1&a=2")
    computed = UrlFingerprint.compute(url)
    first_seen = "2026-01-01T00:00:00+00:00"
    legacy_row = {
        "id": 9100, "指纹编号": "FP-LEGACY-T", "源URL": url,
        "指纹算法": "sha256", "指纹哈希": computed.hash_value,
        "发现渠道": "crawl_job", "首次发现": first_seen,
        "重复次数": 4, "去重状态": "已记录", "status": "已记录",
        "pending": False, "abnormal": False,
    }
    store.rows("url_dedup").append(legacy_row)

    outcome = claim_dedup_window(_job("JOB-LEGACY", [equivalent]), "WIN-LEGACY")[0]
    assert outcome.can_crawl, "历史指纹应能被认领"
    assert outcome.fingerprint.first_seen == first_seen
    assert legacy_row["首次发现"] == first_seen, "首次发现不能被覆盖"
    assert legacy_row["重复次数"] == 4

    receipt = load_fingerprint_receipt(equivalent)
    assert receipt["repeated"] == 4
    assert receipt["first_seen"] == first_seen
    assert receipt["claimed_url"] == equivalent


def test_legacy_fingerprint_without_new_columns() -> None:
    """完全缺少规范URL等新列的旧台账行也能按规范URL命中。"""
    url = f"{_unique_url('bare')}?p=2&q=1"
    computed = UrlFingerprint.compute(url)
    store.rows("url_dedup").append({
        "id": 9101, "指纹编号": "FP-BARE", "源URL": url,
        "指纹算法": "sha256", "指纹哈希": computed.hash_value,
        "发现渠道": "crawl_job", "首次发现": "",
        "重复次数": "0", "去重状态": "已记录", "status": "已记录",
        "pending": False, "abnormal": False,
    })
    run_dedup_flow(_job("JOB-BARE", [url.replace("p=2&q=1", "q=1&p=2")]))
    row = store.rows("url_dedup")[-1]
    assert row["指纹编号"] == "FP-BARE"
    assert row["重复次数"] == 1
    assert row["首次发现"], "缺失的首次发现应按现算时间补齐"
