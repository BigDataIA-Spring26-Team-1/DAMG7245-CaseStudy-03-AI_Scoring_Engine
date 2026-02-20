from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import List, Optional


@dataclass
class GlassdoorReview:
    review_id: str
    rating: float
    title: str
    pros: str
    cons: str
    advice_to_management: Optional[str]
    is_current_employee: bool
    job_title: str
    review_date: datetime


@dataclass
class CultureSignal:
    company_id: str
    ticker: str
    innovation_score: Decimal
    data_driven_score: Decimal
    change_readiness_score: Decimal
    ai_awareness_score: Decimal
    overall_score: Decimal
    review_count: int
    avg_rating: Decimal
    current_employee_ratio: Decimal
    confidence: Decimal
    positive_keywords_found: List[str] = field(default_factory=list)
    negative_keywords_found: List[str] = field(default_factory=list)


class GlassdoorCultureCollector:
    INNOVATION_POSITIVE = [
        "innovative", "cutting-edge", "forward-thinking", "encourages new ideas",
        "experimental", "creative freedom", "startup mentality", "move fast", "disruptive",
    ]
    INNOVATION_NEGATIVE = [
        "bureaucratic", "slow to change", "resistant", "outdated", "stuck in old ways",
        "red tape", "politics", "siloed", "hierarchical",
    ]
    DATA_DRIVEN_KEYWORDS = [
        "data-driven", "metrics", "evidence-based", "analytical", "kpis", "dashboards",
        "data culture", "measurement", "quantitative",
    ]
    AI_AWARENESS_KEYWORDS = [
        "ai", "artificial intelligence", "machine learning", "automation", "data science",
        "ml", "algorithms", "predictive", "neural network",
    ]
    CHANGE_POSITIVE = ["agile", "adaptive", "fast-paced", "embraces change", "continuous improvement", "growth mindset"]
    CHANGE_NEGATIVE = ["rigid", "traditional", "slow", "risk-averse", "change resistant", "old school"]

    @staticmethod
    def _clamp(x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    def analyze_reviews(self, company_id: str, ticker: str, reviews: List[GlassdoorReview]) -> CultureSignal:
        if not reviews:
            return CultureSignal(
                company_id=company_id,
                ticker=ticker,
                innovation_score=Decimal("50.00"),
                data_driven_score=Decimal("50.00"),
                change_readiness_score=Decimal("50.00"),
                ai_awareness_score=Decimal("50.00"),
                overall_score=Decimal("50.00"),
                review_count=0,
                avg_rating=Decimal("0.00"),
                current_employee_ratio=Decimal("0.00"),
                confidence=Decimal("0.30"),
            )

        now = datetime.now(timezone.utc)
        innovation_pos = innovation_neg = 0.0
        data_mentions = ai_mentions = 0.0
        change_pos = change_neg = 0.0
        total_weight = 0.0

        positive_hits: set[str] = set()
        negative_hits: set[str] = set()

        ratings: List[float] = []
        current_employees = 0

        for review in reviews:
            text = f"{review.title} {review.pros} {review.cons} {review.advice_to_management or ''}".lower()
            review_dt = review.review_date
            if review_dt.tzinfo is None:
                review_dt = review_dt.replace(tzinfo=timezone.utc)

            days_old = (now - review_dt).days
            recency_weight = 1.0 if days_old < 730 else 0.5
            employee_weight = 1.2 if review.is_current_employee else 1.0
            weight = recency_weight * employee_weight
            total_weight += weight

            ratings.append(float(review.rating))
            if review.is_current_employee:
                current_employees += 1

            for kw in self.INNOVATION_POSITIVE:
                if kw in text:
                    innovation_pos += weight
                    positive_hits.add(kw)
            for kw in self.INNOVATION_NEGATIVE:
                if kw in text:
                    innovation_neg += weight
                    negative_hits.add(kw)
            for kw in self.DATA_DRIVEN_KEYWORDS:
                if kw in text:
                    data_mentions += weight
                    positive_hits.add(kw)
            for kw in self.AI_AWARENESS_KEYWORDS:
                if kw in text:
                    ai_mentions += weight
                    positive_hits.add(kw)
            for kw in self.CHANGE_POSITIVE:
                if kw in text:
                    change_pos += weight
                    positive_hits.add(kw)
            for kw in self.CHANGE_NEGATIVE:
                if kw in text:
                    change_neg += weight
                    negative_hits.add(kw)

        denom = max(1.0, total_weight)

        innovation = self._clamp(((innovation_pos - innovation_neg) / denom) * 50.0 + 50.0, 0.0, 100.0)
        data_driven = self._clamp((data_mentions / denom) * 100.0, 0.0, 100.0)
        ai_awareness = self._clamp((ai_mentions / denom) * 100.0, 0.0, 100.0)
        change_readiness = self._clamp(((change_pos - change_neg) / denom) * 50.0 + 50.0, 0.0, 100.0)

        overall = 0.30 * innovation + 0.25 * data_driven + 0.25 * ai_awareness + 0.20 * change_readiness
        confidence = self._clamp(0.40 + min(len(reviews), 100) / 100.0 * 0.45, 0.40, 0.95)

        return CultureSignal(
            company_id=company_id,
            ticker=ticker,
            innovation_score=Decimal(str(round(innovation, 2))),
            data_driven_score=Decimal(str(round(data_driven, 2))),
            change_readiness_score=Decimal(str(round(change_readiness, 2))),
            ai_awareness_score=Decimal(str(round(ai_awareness, 2))),
            overall_score=Decimal(str(round(overall, 2))),
            review_count=len(reviews),
            avg_rating=Decimal(str(round(sum(ratings) / max(1, len(ratings)), 2))),
            current_employee_ratio=Decimal(str(round(current_employees / max(1, len(reviews)), 3))),
            confidence=Decimal(str(round(confidence, 3))),
            positive_keywords_found=sorted(positive_hits),
            negative_keywords_found=sorted(negative_hits),
        )

    def fetch_reviews(self, ticker: str, limit: int = 100) -> List[GlassdoorReview]:
        path = Path("data") / "glassdoor" / f"{ticker.lower()}.json"
        if not path.exists():
            return []

        rows = json.loads(path.read_text(encoding="utf-8"))
        out: List[GlassdoorReview] = []
        for row in rows[:limit]:
            review_date = datetime.fromisoformat(str(row.get("review_date")).replace("Z", "+00:00"))
            out.append(
                GlassdoorReview(
                    review_id=str(row.get("review_id", "")),
                    rating=float(row.get("rating", 0.0)),
                    title=str(row.get("title", "")),
                    pros=str(row.get("pros", "")),
                    cons=str(row.get("cons", "")),
                    advice_to_management=row.get("advice_to_management"),
                    is_current_employee=bool(row.get("is_current_employee", False)),
                    job_title=str(row.get("job_title", "")),
                    review_date=review_date,
                )
            )
        return out
