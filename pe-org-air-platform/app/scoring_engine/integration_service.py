from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict

import httpx

from app.pipelines.board_analyzer import BoardCompositionAnalyzer
from app.pipelines.glassdoor_collector import GlassdoorCultureCollector
from app.scoring_engine.evidence_mapper import EvidenceScore, SignalSource
from app.scoring_engine.position_factor import PositionFactorCalculator
from app.scoring_engine.rubric_scorer import RubricScorer
from app.scoring_engine.talent_concentration import TalentConcentrationCalculator


class ScoringIntegrationService:
    """
    Full pipeline from CS1/CS2 data to Org-AI-R score.

    This implementation keeps external calls explicit and deterministic, and
    returns a rich result dict suitable for persistence/auditing.
    """

    def __init__(self, cs1_api_url: str = "http://localhost:8000", cs2_api_url: str = "http://localhost:8001"):
        self.cs1_url = cs1_api_url.rstrip("/")
        self.cs2_url = cs2_api_url.rstrip("/")

        self.rubric_scorer = RubricScorer()
        self.tc_calculator = TalentConcentrationCalculator()
        self.pf_calculator = PositionFactorCalculator()
        self.glassdoor_collector = GlassdoorCultureCollector()
        self.board_analyzer = BoardCompositionAnalyzer()

        self.http = httpx.Client(timeout=30.0)

    def score_company(self, ticker: str) -> Dict[str, Any]:
        company = self._fetch_company(ticker)
        cs2_evidence = self._fetch_cs2_evidence(company["id"])

        glassdoor_signal = self._collect_glassdoor(company["id"], ticker)
        board_signal = self._collect_board(company["id"], ticker)

        evidence_scores = self._build_evidence_scores(cs2_evidence, glassdoor_signal, board_signal)

        evidence_by_dimension = {
            "data_infrastructure": cs2_evidence.get("data_infrastructure_text", ""),
            "ai_governance": cs2_evidence.get("ai_governance_text", ""),
            "technology_stack": cs2_evidence.get("technology_stack_text", ""),
            "talent": cs2_evidence.get("talent_text", ""),
            "leadership": cs2_evidence.get("leadership_text", ""),
            "use_case_portfolio": cs2_evidence.get("use_case_text", ""),
            "culture": cs2_evidence.get("culture_text", ""),
        }

        metrics_by_dimension = cs2_evidence.get("metrics_by_dimension", {})
        rubric_results = self.rubric_scorer.score_all_dimensions(evidence_by_dimension, metrics_by_dimension)

        job_analysis = self.tc_calculator.analyze_job_postings(cs2_evidence.get("job_postings", []))
        tc = self.tc_calculator.calculate_tc(
            job_analysis,
            glassdoor_individual_mentions=glassdoor_signal.get("individual_mentions", 0),
            glassdoor_review_count=glassdoor_signal.get("review_count", 1),
        )

        vr_score = float(sum(float(v.score) for v in rubric_results.values()) / max(1, len(rubric_results)))
        pf = self.pf_calculator.calculate_position_factor(
            vr_score=vr_score,
            sector=company.get("sector", ""),
            market_cap_percentile=float(company.get("market_cap_percentile", 0.5)),
        )

        hr_base = float(company.get("hr_base", 50.0))
        hr_score = max(0.0, min(100.0, hr_base * (1 + 0.15 * float(pf))))

        alignment = self._calculate_alignment(vr_score, hr_score)
        synergy_score = (vr_score * hr_score / 100.0) * alignment * 1.0

        alpha = Decimal("0.60")
        beta = Decimal("0.12")
        final_score = float((1 - beta) * (alpha * Decimal(str(vr_score)) + (1 - alpha) * Decimal(str(hr_score))) + beta * Decimal(str(synergy_score)))

        total_evidence = sum(es.evidence_count for es in evidence_scores)
        conf = min(0.95, 0.40 + total_evidence / 200.0)
        band = 10.0

        result = {
            "company_id": company["id"],
            "ticker": ticker,
            "sector": company.get("sector"),
            "final_score": round(final_score, 2),
            "vr_score": round(vr_score, 2),
            "hr_score": round(hr_score, 2),
            "synergy_score": round(synergy_score, 2),
            "ci_lower": round(max(0.0, final_score - band), 2),
            "ci_upper": round(min(100.0, final_score + band), 2),
            "talent_concentration": float(tc),
            "position_factor": float(pf),
            "dimension_scores": {k: float(v.score) for k, v in rubric_results.items()},
            "evidence_count": total_evidence,
            "confidence": round(conf, 3),
        }

        self._persist_assessment(result)
        return result

    def _fetch_company(self, ticker: str) -> Dict[str, Any]:
        resp = self.http.get(f"{self.cs1_url}/api/v1/companies", params={"ticker": ticker})
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", data if isinstance(data, list) else [])
        if not items:
            raise ValueError(f"Company not found for ticker={ticker}")
        return items[0]

    def _fetch_cs2_evidence(self, company_id: str) -> Dict[str, Any]:
        resp = self.http.get(f"{self.cs2_url}/api/v1/companies/{company_id}/signals")
        resp.raise_for_status()
        signals = resp.json()
        if not isinstance(signals, list):
            return {}
        return {
            "signals": signals,
            "job_postings": [s for s in signals if str(s.get("signal_type", "")).lower() == "jobs"],
            "metrics_by_dimension": {},
        }

    def _collect_glassdoor(self, company_id: str, ticker: str) -> Dict[str, Any]:
        reviews = self.glassdoor_collector.fetch_reviews(ticker=ticker, limit=100)
        sig = self.glassdoor_collector.analyze_reviews(company_id=company_id, ticker=ticker, reviews=reviews)
        return {
            "raw_score": float(sig.overall_score),
            "confidence": float(sig.confidence),
            "evidence_count": sig.review_count,
            "review_count": sig.review_count,
            "individual_mentions": 0,
        }

    def _collect_board(self, company_id: str, ticker: str) -> Dict[str, Any]:
        members, committees = [], []
        sig = self.board_analyzer.analyze_board(
            company_id=company_id,
            ticker=ticker,
            members=members,
            committees=committees,
            strategy_text="",
        )
        return {
            "raw_score": float(sig.governance_score),
            "confidence": float(sig.confidence),
            "evidence_count": len(sig.ai_experts) + len(sig.relevant_committees),
        }

    def _build_evidence_scores(self, cs2_evidence: Dict[str, Any], glassdoor: Dict[str, Any], board: Dict[str, Any]) -> list[EvidenceScore]:
        out: list[EvidenceScore] = []

        for sig in cs2_evidence.get("signals", []):
            st = str(sig.get("signal_type", "")).lower()
            source = SignalSource.TECHNOLOGY_HIRING if "job" in st else SignalSource.INNOVATION_ACTIVITY
            out.append(
                EvidenceScore(
                    source=source,
                    raw_score=Decimal(str(sig.get("score", 50.0))),
                    confidence=Decimal(str(sig.get("confidence", 0.7))),
                    evidence_count=int(sig.get("evidence_count", 1)),
                    metadata=dict(sig),
                )
            )

        out.append(
            EvidenceScore(
                source=SignalSource.GLASSDOOR_REVIEWS,
                raw_score=Decimal(str(glassdoor.get("raw_score", 50.0))),
                confidence=Decimal(str(glassdoor.get("confidence", 0.6))),
                evidence_count=int(glassdoor.get("evidence_count", 0)),
                metadata=dict(glassdoor),
            )
        )

        out.append(
            EvidenceScore(
                source=SignalSource.BOARD_COMPOSITION,
                raw_score=Decimal(str(board.get("raw_score", 50.0))),
                confidence=Decimal(str(board.get("confidence", 0.6))),
                evidence_count=int(board.get("evidence_count", 0)),
                metadata=dict(board),
            )
        )

        return out

    @staticmethod
    def _calculate_alignment(vr_score: float, hr_score: float) -> float:
        gap = abs(vr_score - hr_score)
        return max(0.5, min(1.0, 1.0 - gap / 100.0))

    def _persist_assessment(self, result: Dict[str, Any]) -> None:
        payload = {
            "company_id": result["company_id"],
            "assessment_type": "cs3_scoring",
            "assessment_date": "2026-02-20",
            "vr_score": result["vr_score"],
            "confidence_lower": result["ci_lower"],
            "confidence_upper": result["ci_upper"],
        }
        resp = self.http.post(f"{self.cs1_url}/api/v1/assessments", json=payload)
        resp.raise_for_status()
