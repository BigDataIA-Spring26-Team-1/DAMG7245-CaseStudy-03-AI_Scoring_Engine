from __future__ import annotations

from dataclasses import dataclass


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


@dataclass(frozen=True)
class CompositeResult:
    composite_score: float
    score_band: str
    base_vr_plus_synergy: float
    penalty_factor_used: float


def _score_band(score: float) -> str:
    if score >= 85.0:
        return "leader"
    if score >= 70.0:
        return "strong"
    if score >= 55.0:
        return "developing"
    if score >= 40.0:
        return "emerging"
    return "lagging"


def compute_composite(
    *,
    vr_score: float,
    synergy_bonus: float,
    penalty_factor: float,
) -> CompositeResult:
    """
    Composite score model:
      1) Additive synergy on top of VR
      2) Multiplicative talent concentration penalty
      3) Clamp to [0, 100]
    """
    base = _clamp(float(vr_score) + float(synergy_bonus), 0.0, 100.0)
    factor = _clamp(float(penalty_factor), 0.0, 1.0)
    composite = _clamp(base * factor, 0.0, 100.0)

    return CompositeResult(
        composite_score=round(composite, 2),
        score_band=_score_band(composite),
        base_vr_plus_synergy=round(base, 2),
        penalty_factor_used=round(factor, 4),
    )

