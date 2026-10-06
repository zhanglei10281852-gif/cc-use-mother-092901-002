"""领域对象到 JSON 可记录结构的转换。"""

from .engine import EventJudgment, SynthesisResult
from .service import IngestOutcome, ReplayComparison, SealRecord
from .snapshots import Snapshot


def _iso(value):
    return value.isoformat() if value is not None else None


def judgment_to_dict(judgment: EventJudgment) -> dict:
    return {
        "event_key": judgment.event_key,
        "episode_id": judgment.episode_id,
        "status": judgment.status.value,
        "location_code": judgment.location_code,
        "confidence": judgment.confidence,
        "active_since": _iso(judgment.active_since),
        "cleared_at": _iso(judgment.cleared_at),
        "fingerprint": judgment.fingerprint,
        "rules_version": judgment.rules_version,
        "computed_as_of": judgment.computed_as_of.isoformat(),
        "occurrence_spread_s": judgment.occurrence_spread_s,
        "lane_conflicts": judgment.lane_conflicts,
        "evidence": [
            {
                "report_id": item.report_id,
                "source": {"type": item.source_type, "id": item.source_id},
                "kind": item.kind,
                "observed_at": item.observed_at.isoformat(),
                "intrinsic_weight": item.intrinsic_weight,
                "effective_weight": item.effective_weight,
                "role": item.role,
                "stale": item.stale,
            }
            for item in judgment.evidence
        ],
        "excluded": [
            {"report_id": item.report_id, "reason": item.reason, "detail": item.detail}
            for item in judgment.excluded
        ],
    }


def synthesis_to_dict(result: SynthesisResult) -> dict:
    return {
        "as_of": result.as_of.isoformat(),
        "rules_version": result.rules_version,
        "journal_seq_max": result.seq_max,
        "judgments": {key: judgment_to_dict(j) for key, j in result.judgments.items()},
    }


def snapshot_to_dict(snapshot: Snapshot) -> dict:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "sequence": snapshot.sequence,
        "published_at": _iso(snapshot.published_at),
        "sealed": snapshot.sealed,
        "rules_version": snapshot.rules_version,
        "as_of": _iso(snapshot.as_of),
        "journal_seq_max": snapshot.journal_seq_max,
        "event_key": snapshot.event_key,
        "episode_id": snapshot.episode_id,
        "status": snapshot.status,
        "location_code": snapshot.location_code,
        "confidence": snapshot.confidence,
        "active_since": _iso(snapshot.active_since),
        "cleared_at": _iso(snapshot.cleared_at),
        "judgment_fingerprint": snapshot.judgment_fingerprint,
        "evidence_report_ids": list(snapshot.evidence_report_ids),
        "excluded_report_ids": list(snapshot.excluded_report_ids),
        "content_hash": snapshot.content_hash,
        "prev_snapshot_id": snapshot.prev_snapshot_id,
        "chain_hash": snapshot.chain_hash,
    }


def seal_to_dict(seal: SealRecord) -> dict:
    return {
        "event_key": seal.event_key,
        "episode_id": seal.episode_id,
        "sealed_at": _iso(seal.sealed_at),
        "journal_seq": seal.journal_seq,
        "snapshot_id": seal.snapshot_id,
    }


def ingest_outcome_to_dict(outcome: IngestOutcome) -> dict:
    entry = outcome.receipt.entry
    return {
        "accepted": True,
        "journal_seq": entry.seq,
        "duplicated": outcome.receipt.duplicated,
        "duplicate_of": entry.duplicate_of,
        "published": [snapshot_to_dict(s) for s in outcome.published],
    }


def replay_to_dict(replay: ReplayComparison) -> dict:
    return {
        "as_of": _iso(replay.as_of),
        "rules_version": replay.rules_version,
        "historical": {k: judgment_to_dict(j) for k, j in replay.historical.items()},
        "current": {k: judgment_to_dict(j) for k, j in replay.current.items()},
        "new_reports": list(replay.new_reports),
        "changes": replay.changes,
        "published_snapshots_untouched": replay.published_snapshots_untouched,
    }
