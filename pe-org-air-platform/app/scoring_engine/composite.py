from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class CompositeResult:
    composite_score: float
    score_band: str

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _assign_score_band(score: float) -> str:
    """
    Score bands defined in CS3 checklist:
    0-20   Nascent
    21-40  Developing
    41-60  Progressing
    61-80  Advanced
    81-100 Leading
    """
    if score <= 20:
        return "Nascent"
    if score <= 40:
        return "Developing"
    if score <= 60:
        return "Progressing"
    if score <= 80:
        return "Advanced"
    return "Leading"

def compute_composite(
    *,
    vr_score: float,
    synergy_bonus: float,
    penalty_factor: float,
) -> CompositeResult:
    """
    Composite formula (Lab 6):
        Org-AI-R = (VR + synergy_bonus) * penalty_factor
    - VR is already 0-100
    - synergy_bonus is capped ±15
    - penalty_factor in [0,1]
    """
    base = vr_score + synergy_bonus
    # ensure penalty_factor bounded
    penalty_factor = _clamp(penalty_factor, 0.0, 1.0)
    composite = base * penalty_factor
    composite = _clamp(composite, 0.0, 100.0)
    band = _assign_score_band(composite)
    return CompositeResult(
        composite_score=round(composite, 2),
        score_band=band,
    )