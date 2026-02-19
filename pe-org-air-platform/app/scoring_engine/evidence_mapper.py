from __future__ import annotations
 
from dataclasses import dataclass
from typing import Dict, List, Tuple
 
 
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
class EvidenceItem:
    source: str                # "document_chunk" | "external_signal" | "company_summary"
    evidence_type: str         # e.g., "10-K", "jobs", "news"
    text: str
    url: str | None = None
    published_at: str | None = None
 
 
@dataclass(frozen=True)
class MappedEvidence:
    dimension: str
    matched_keywords: List[str]
    item: EvidenceItem
 
 
# Simple keyword taxonomy (deterministic + explainable)
DIMENSION_KEYWORDS: Dict[str, List[str]] = {
    "data_infrastructure": [
        "data lake", "data warehouse", "etl", "pipeline", "spark", "snowflake", "databricks",
        "governance of data", "data quality", "master data", "metadata", "lineage",
    ],
    "ai_governance": [
        "model risk", "responsible ai", "ai governance", "policy", "compliance", "privacy",
        "security", "bias", "audit", "controls", "risk management",
    ],
    "technology_stack": [
        "cloud", "aws", "azure", "gcp", "kubernetes", "mlops", "api", "microservice",
        "vector database", "llm", "cortex", "bedrock", "sagemaker",
    ],
    "talent_skills": [
        "data scientist", "machine learning engineer", "ml engineer", "data engineer",
        "ai engineer", "mlops", "analytics", "python", "sql",
    ],
    "leadership_vision": [
        "strategy", "roadmap", "executive", "ceo", "cio", "chief data", "chief ai",
        "investment", "transformation", "innovation",
    ],
    "use_case_portfolio": [
        "use case", "pilot", "production", "deployment", "predictive", "forecast",
        "recommendation", "fraud", "optimization", "automation", "genai",
    ],
    "culture_change": [
        "training", "change management", "culture", "adoption", "upskilling", "reskilling",
        "agile", "cross-functional", "center of excellence", "coe",
    ],
}
 
 
def _normalize(text: str) -> str:
    return (text or "").lower()
 
 
def map_evidence_to_dimensions(items: List[EvidenceItem]) -> List[MappedEvidence]:
    mapped: List[MappedEvidence] = []
 
    for item in items:
        t = _normalize(item.text)
 
        for dim, keywords in DIMENSION_KEYWORDS.items():
            hits = [kw for kw in keywords if kw in t]
            if hits:
                mapped.append(
                    MappedEvidence(
                        dimension=dim,
                        matched_keywords=hits[:8],  # cap for readability
                        item=item,
                    )
                )
    return mapped