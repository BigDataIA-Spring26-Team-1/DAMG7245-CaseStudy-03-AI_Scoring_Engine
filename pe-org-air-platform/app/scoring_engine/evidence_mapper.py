from __future__ import annotations
 
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
from app.scoring_engine.mapping_config import SOURCE_PROFILES, normalize_weights
 
 
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

@dataclass(frozen=True)
class DimensionFeature:
    dimension: str
    weighted_signal: float
    evidence_count: int
    reliability_weighted: float
    top_keywords: List[str]

def _infer_signal_bucket(item: EvidenceItem) -> str:
    """
    Normalize different evidence types into spec buckets.
    """
    t = (item.evidence_type or "").lower()
    if "10-k" in t or "10k" in t or "10 q" in t or "10-q" in t:
        return "10k"
    if "job" in t:
        return "jobs"
    if "news" in t:
        return "news"
    if "patent" in t:
        return "patents"
    if "tech" in t or "stack" in t:
        return "tech"
    return "news"  # safe default bucket

def build_source_payloads(mapped: List[MappedEvidence]) -> Dict[str, dict]:
    """
    Converts raw mapped evidence into per-source payloads:
    { source: {count: int, keywords: {kw: freq}} }
    """
    payloads: Dict[str, dict] = {}
    for m in mapped:
        src = _infer_signal_bucket(m.item)
        if src not in payloads:
            payloads[src] = {"count": 0, "keywords": {}}
        payloads[src]["count"] += 1
        for kw in m.matched_keywords:
            payloads[src]["keywords"][kw] = payloads[src]["keywords"].get(kw, 0) + 1
    return payloads

def map_sources_to_dimension_features(source_payloads: Dict[str, dict]) -> Dict[str, DimensionFeature]:
    """
    Weighted mapping matrix + reliability.
    Returns features per dimension that rubric scorer will consume.
    """
    acc: Dict[str, Dict[str, Any]] = {
        d: {"weighted_signal": 0.0, "evidence_count": 0, "reliability_weighted": 0.0, "keywords": {}}
        for d in DIMENSIONS
    }
    for source_name, payload in source_payloads.items():
        prof = SOURCE_PROFILES.get(source_name)
        if not prof:
            continue
        w = normalize_weights(prof.dim_weights)
        count = int(payload.get("count", 0))
        kws = payload.get("keywords", {}) or {}
        for dim, dim_w in w.items():
            contrib = float(count) * float(dim_w) * float(prof.reliability)
            acc[dim]["weighted_signal"] += contrib
            acc[dim]["evidence_count"] += count
            acc[dim]["reliability_weighted"] += float(dim_w) * float(prof.reliability)
            for k, v in kws.items():
                acc[dim]["keywords"][k] = acc[dim]["keywords"].get(k, 0) + int(v)
    out: Dict[str, DimensionFeature] = {}
    for dim in DIMENSIONS:
        topk = sorted(acc[dim]["keywords"].items(), key=lambda x: x[1], reverse=True)[:5]
        out[dim] = DimensionFeature(
            dimension=dim,
            weighted_signal=float(acc[dim]["weighted_signal"]),
            evidence_count=int(acc[dim]["evidence_count"]),
            reliability_weighted=float(acc[dim]["reliability_weighted"]),
            top_keywords=[k for k, _ in topk],
        )
    return out