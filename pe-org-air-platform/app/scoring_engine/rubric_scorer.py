from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from app.scoring_engine.mapping_config import DIMENSIONS
from app.scoring_engine.evidence_mapper import DimensionFeature


@dataclass(frozen=True)
class DimensionScoreResult:
    dimension: str
    score: float          # 0–100
    confidence: float     # 0–1
    evidence_count: int
    top_keywords: List[str]
    reasons: List[str]


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _default_result(dim: str) -> DimensionScoreResult:
    # PDF spec behavior: if no evidence, do NOT penalize to 0/10 → default to 50
    return DimensionScoreResult(
        dimension=dim,
        score=50.0,
        confidence=0.50,
        evidence_count=0,
        top_keywords=[],
        reasons=["No evidence found → default score=50"],
    )


def _confidence_from_evidence(evidence_count: int, reliability_weighted: float) -> float:
    """
    Confidence increases with evidence volume and reliability.
    Bounded so it never hits 0 or 1.
    """
    vol = clamp(evidence_count / 80.0, 0.0, 1.0)        # 80+ evidence ≈ strong
    rel = clamp(reliability_weighted, 0.0, 1.0)        # already weighted per source
    conf = 0.40 + 0.35 * vol + 0.20 * rel
    return clamp(conf, 0.40, 0.95)


def _score_from_thresholds(
    f: DimensionFeature,
    *,
    low: float,
    mid: float,
    high: float,
    very_high: float,
) -> tuple[float, List[str]]:
    """
    5-level rubric based on weighted_signal:
    - defaults to 50 if no evidence
    - otherwise returns 25/50/75/90/100 based on thresholds
    """
    if f.evidence_count <= 0:
        return 50.0, ["No evidence found → default score=50"]

    s = float(f.weighted_signal)
    reasons: List[str] = [f"weighted_signal={s:.2f}, evidence_count={f.evidence_count}"]

    if s < low:
        reasons.append(f"weighted_signal < low({low}) → 25")
        return 25.0, reasons

    if s < mid:
        reasons.append(f"low({low}) ≤ weighted_signal < mid({mid}) → 50")
        return 50.0, reasons

    if s < high:
        reasons.append(f"mid({mid}) ≤ weighted_signal < high({high}) → 75")
        return 75.0, reasons

    if s < very_high:
        reasons.append(f"high({high}) ≤ weighted_signal < very_high({very_high}) → 90")
        return 90.0, reasons

    reasons.append(f"weighted_signal ≥ very_high({very_high}) → 100")
    return 100.0, reasons


# Dimension-specific thresholds (deterministic v1; tune later)
RUBRIC_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "data_infrastructure": {"low": 4.0, "mid": 10.0, "high": 18.0, "very_high": 28.0},
    "ai_governance": {"low": 3.0, "mid": 9.0, "high": 16.0, "very_high": 26.0},
    "technology_stack": {"low": 4.0, "mid": 11.0, "high": 19.0, "very_high": 30.0},
    "talent_skills": {"low": 5.0, "mid": 12.0, "high": 20.0, "very_high": 32.0},
    "leadership_vision": {"low": 3.0, "mid": 9.0, "high": 16.0, "very_high": 26.0},
    "use_case_portfolio": {"low": 4.0, "mid": 10.0, "high": 18.0, "very_high": 28.0},
    "culture_change": {"low": 3.0, "mid": 8.0, "high": 15.0, "very_high": 24.0},
}


def score_dimension_features(features: Dict[str, DimensionFeature]) -> List[DimensionScoreResult]:
    """
    Scores ALL 7 dimensions using weighted_signal + reliability.
    Guarantees a result for each dimension.
    """
    results: List[DimensionScoreResult] = []

    for dim in DIMENSIONS:
        f = features.get(dim)
        if not f:
            results.append(_default_result(dim))
            continue

        t = RUBRIC_THRESHOLDS[dim]
        score, reasons = _score_from_thresholds(
            f,
            low=t["low"],
            mid=t["mid"],
            high=t["high"],
            very_high=t["very_high"],
        )

        conf = _confidence_from_evidence(f.evidence_count, f.reliability_weighted)

        results.append(
            DimensionScoreResult(
                dimension=dim,
                score=clamp(score, 0.0, 100.0),
                confidence=clamp(conf, 0.0, 1.0),
                evidence_count=int(f.evidence_count),
                top_keywords=list(f.top_keywords),
                reasons=reasons,
            )
        )

    return results