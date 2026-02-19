from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List
from app.scoring_engine.evidence_mapper import DIMENSIONS, MappedEvidence
@dataclass(frozen=True)
class DimensionScoreResult:
    dimension: str
    score: float          # 0–100
    confidence: float     # 0–1
    evidence_count: int
    top_keywords: List[str]

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _score_from_count(n: int) -> float:
    """
    Rubric: more evidence -> higher score, with diminishing returns.
    Very explainable & deterministic.
    """
    if n <= 0:
        return 10.0
    if n <= 2:
        return 35.0
    if n <= 5:
        return 55.0
    if n <= 10:
        return 70.0
    if n <= 20:
        return 82.0
    return 90.0

def _confidence_from_count(n: int) -> float:
    """
    Confidence increases with more evidence.
    """
    if n <= 0:
        return 0.30
    if n <= 2:
        return 0.55
    if n <= 5:
        return 0.70
    if n <= 10:
        return 0.80
    if n <= 20:
        return 0.88
    return 0.92

def score_dimensions(mapped: List[MappedEvidence]) -> List[DimensionScoreResult]:
    by_dim: Dict[str, List[MappedEvidence]] = {d: [] for d in DIMENSIONS}
    for m in mapped:
        if m.dimension in by_dim:
            by_dim[m.dimension].append(m)
    results: List[DimensionScoreResult] = []
    for dim in DIMENSIONS:
        items = by_dim[dim]
        n = len(items)
        # Collect top keywords for explainability (most frequent)
        kw_freq: Dict[str, int] = {}
        for it in items:
            for kw in it.matched_keywords:
                kw_freq[kw] = kw_freq.get(kw, 0) + 1
        top_keywords = sorted(kw_freq.keys(), key=lambda k: kw_freq[k], reverse=True)[:8]
        score = _score_from_count(n)
        conf = _confidence_from_count(n)
        results.append(
            DimensionScoreResult(
                dimension=dim,
                score=clamp(score, 0.0, 100.0),
                confidence=clamp(conf, 0.0, 1.0),
                evidence_count=n,
                top_keywords=top_keywords,
            )
        )
    return results
 