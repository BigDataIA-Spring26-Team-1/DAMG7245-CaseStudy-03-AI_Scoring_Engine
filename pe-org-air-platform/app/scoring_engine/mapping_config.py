from __future__ import annotations
 
from dataclasses import dataclass
from typing import Dict
 
DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]
 
 
@dataclass(frozen=True)
class SourceProfile:
    reliability: float
    dim_weights: Dict[str, float]
 
 
SOURCE_PROFILES: dict[str, SourceProfile] = {
    "10k": SourceProfile(
        reliability=0.90,
        dim_weights={
            "leadership_vision": 0.18,
            "ai_governance": 0.18,
            "data_infrastructure": 0.14,
            "technology_stack": 0.14,
            "use_case_portfolio": 0.16,
            "culture_change": 0.12,
            "talent_skills": 0.08,
        },
    ),
    "jobs": SourceProfile(
        reliability=0.75,
        dim_weights={
            "talent_skills": 0.40,
            "technology_stack": 0.20,
            "data_infrastructure": 0.15,
            "use_case_portfolio": 0.10,
            "ai_governance": 0.05,
            "leadership_vision": 0.05,
            "culture_change": 0.05,
        },
    ),
    "news": SourceProfile(
        reliability=0.60,
        dim_weights={
            "leadership_vision": 0.20,
            "use_case_portfolio": 0.25,
            "culture_change": 0.20,
            "ai_governance": 0.10,
            "technology_stack": 0.10,
            "data_infrastructure": 0.10,
            "talent_skills": 0.05,
        },
    ),
    "patents": SourceProfile(
        reliability=0.70,
        dim_weights={
            "use_case_portfolio": 0.30,
            "technology_stack": 0.20,
            "data_infrastructure": 0.15,
            "leadership_vision": 0.10,
            "talent_skills": 0.15,
            "ai_governance": 0.05,
            "culture_change": 0.05,
        },
    ),
    "tech": SourceProfile(
        reliability=0.65,
        dim_weights={
            "technology_stack": 0.45,
            "data_infrastructure": 0.25,
            "use_case_portfolio": 0.10,
            "talent_skills": 0.10,
            "ai_governance": 0.05,
            "leadership_vision": 0.03,
            "culture_change": 0.02,
        },
    ),
}
 
 
def normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    s = float(sum(weights.values()))
    if s <= 0:
        return {k: 0.0 for k in weights}
    return {k: float(v) / s for k, v in weights.items()}