# scripts/run_scoring_engine.py
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import uuid4
from datetime import datetime, UTC

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.snowflake import get_snowflake_connection

from app.scoring_engine.sector_config import get_company_sector, load_sector_profile
from app.scoring_engine.vr_model import fetch_dimension_inputs, compute_vr_score, DimensionInput
from app.scoring_engine.hr_baselines import compute_hr_factor, apply_hr_adjustment_to_talent
from app.scoring_engine.synergy import load_synergy_rules, compute_synergy
from app.scoring_engine.talent_penalty import compute_talent_penalty
from app.scoring_engine.composite import compute_composite


def _now_ts() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def get_latest_assessment_id(cur, company_id: str) -> str:
    cur.execute(
        """
        SELECT id
        FROM assessments
        WHERE company_id = %s
        ORDER BY assessment_date DESC, created_at DESC
        LIMIT 1
        """,
        (company_id,),
    )
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"No assessments found for company_id={company_id}")
    return str(row[0])


def insert_scoring_run(cur, companies_scored: list[str], model_version: str, params: dict) -> str:
    """
    Use INSERT..SELECT (not VALUES) so PARSE_JSON works reliably for VARIANT columns.
    """
    run_id = str(uuid4())
    cur.execute(
        """
        INSERT INTO scoring_runs (id, run_timestamp, companies_scored, model_version, parameters_json, status)
        SELECT
          %s,
          CURRENT_TIMESTAMP(),
          PARSE_JSON(%s),
          %s,
          PARSE_JSON(%s),
          %s
        """,
        (
            run_id,
            json.dumps(companies_scored),
            model_version,
            json.dumps(params),
            "running",
        ),
    )
    return run_id


def update_scoring_run_status(cur, run_id: str, status: str) -> None:
    cur.execute(
        """
        UPDATE scoring_runs
        SET status = %s
        WHERE id = %s
        """,
        (status, run_id),
    )


def audit_log(cur, run_id: str, company_id: str, step: str, input_obj: dict, output_obj: dict) -> None:
    """
    Use INSERT..SELECT so PARSE_JSON works reliably for VARIANT columns.
    """
    cur.execute(
        """
        INSERT INTO scoring_audit_log (id, scoring_run_id, company_id, step_name, input_json, output_json)
        SELECT
          %s,
          %s,
          %s,
          %s,
          PARSE_JSON(%s),
          PARSE_JSON(%s)
        """,
        (
            str(uuid4()),
            run_id,
            company_id,
            step,
            json.dumps(input_obj),
            json.dumps(output_obj),
        ),
    )


def upsert_org_air_score(
    cur,
    *,
    company_id: str,
    assessment_id: str,
    scoring_run_id: str,
    vr_score: float,
    synergy_bonus: float,
    talent_penalty: float,
    sem_lower: float | None,
    sem_upper: float | None,
    composite_score: float,
    score_band: str,
    breakdown_json: dict,
) -> None:
    """
    Idempotent upsert for (company_id, scoring_run_id).

    NOTE: Keep JSON parsing inside MERGE as PARSE_JSON(%s). If your Snowflake environment
    complains here, we can refactor to a USING subquery that pre-parses JSON.
    """
    score_id = str(uuid4())
    cur.execute(
        """
        MERGE INTO org_air_scores t
        USING (
          SELECT %s AS company_id, %s AS scoring_run_id
        ) s
        ON t.company_id = s.company_id AND t.scoring_run_id = s.scoring_run_id
        WHEN MATCHED THEN UPDATE SET
          assessment_id = %s,
          vr_score = %s,
          synergy_bonus = %s,
          talent_penalty = %s,
          sem_lower = %s,
          sem_upper = %s,
          composite_score = %s,
          score_band = %s,
          dimension_breakdown_json = PARSE_JSON(%s),
          scored_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          id, company_id, assessment_id, vr_score, synergy_bonus, talent_penalty, sem_lower, sem_upper,
          composite_score, score_band, dimension_breakdown_json, scoring_run_id, scored_at, created_at
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, PARSE_JSON(%s), %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """,
        (
            company_id,
            scoring_run_id,
            assessment_id,
            vr_score,
            synergy_bonus,
            talent_penalty,
            sem_lower,
            sem_upper,
            composite_score,
            score_band,
            json.dumps(breakdown_json),
            score_id,
            company_id,
            assessment_id,
            vr_score,
            synergy_bonus,
            talent_penalty,
            sem_lower,
            sem_upper,
            composite_score,
            score_band,
            json.dumps(breakdown_json),
            scoring_run_id,
        ),
    )


def score_one_company(cur, *, company_id: str, version: str, run_id: str) -> None:
    assessment_id = get_latest_assessment_id(cur, company_id)

    # Sector profile (weights + sector name for auditability)
    sector = get_company_sector(cur, company_id)
    profile = load_sector_profile(cur, sector, version=version)

    # Dimension inputs from Snowflake
    dims = fetch_dimension_inputs(cur, assessment_id)

    # HR adjustment (applies only to talent_skills)
    hr = compute_hr_factor(cur, company_id=company_id, sector_name=sector, version=version)
    adjusted_dims: list[DimensionInput] = []
    for d in dims:
        adjusted_score = apply_hr_adjustment_to_talent(
            dimension=d.dimension,
            raw_score=d.raw_score,
            hr_factor=hr.hr_factor,
        )
        adjusted_dims.append(
            DimensionInput(
                dimension=d.dimension,
                raw_score=adjusted_score,
                confidence=d.confidence,
                evidence_count=d.evidence_count,
            )
        )

    audit_log(
        cur,
        run_id,
        company_id,
        "hr_baseline",
        {"sector": sector, "version": version},
        {
            "baseline_value": hr.baseline_value,
            "jobs_signal_cnt": hr.jobs_signal_count,
            "hr_factor": hr.hr_factor,
            "method": hr.method,
            "window_days": hr.window_days,
        },
    )

    # Synergy
    rules = load_synergy_rules(cur, version=version)
    scores_by_dim = {d.dimension: d.raw_score for d in adjusted_dims}
    syn = compute_synergy(scores_by_dim, rules, cap_abs=15.0)

    audit_log(
        cur,
        run_id,
        company_id,
        "synergy",
        {"rules_loaded": len(rules), "cap_abs": 15.0},
        {
            "synergy_bonus": syn.synergy_bonus,
            "hits": [
                {
                    "dim_a": h.dim_a,
                    "dim_b": h.dim_b,
                    "type": h.synergy_type,
                    "threshold": h.threshold,
                    "magnitude": h.magnitude,
                    "activated": h.activated,
                    "reason": h.reason,
                }
                for h in syn.hits
            ],
        },
    )

    # Talent penalty (HHI)
    pen = compute_talent_penalty(cur, company_id=company_id, version=version)

    audit_log(
        cur,
        run_id,
        company_id,
        "talent_penalty",
        {"version": version},
        {
            "sample_size": pen.sample_size,
            "min_sample_met": pen.min_sample_met,
            "hhi_value": pen.hhi_value,
            "penalty_factor": pen.penalty_factor,
            "function_counts": pen.function_counts,
        },
    )

    # VR
    vr, vr_breakdown = compute_vr_score(adjusted_dims, profile.weights)

    audit_log(
        cur,
        run_id,
        company_id,
        "vr_model",
        {"sector": sector, "version": version},
        {"vr_score": vr, "dimension_breakdown": vr_breakdown},
    )

    # Composite (SEM not implemented yet => sem bounds NULL for now)
    comp = compute_composite(
        vr_score=vr,
        synergy_bonus=syn.synergy_bonus,
        penalty_factor=pen.penalty_factor,
    )

    breakdown_json = {
        "sector": sector,
        "version": version,
        "hr": {
            "baseline_value": hr.baseline_value,
            "jobs_signal_cnt": hr.jobs_signal_count,
            "hr_factor": hr.hr_factor,
            "method": hr.method,
            "window_days": hr.window_days,
        },
        "synergy": {
            "rules_loaded": len(rules),
            "cap_abs": 15.0,
            "synergy_bonus": syn.synergy_bonus,
            "hits": [
                {
                    "dim_a": h.dim_a,
                    "dim_b": h.dim_b,
                    "type": h.synergy_type,
                    "threshold": h.threshold,
                    "magnitude": h.magnitude,
                    "activated": h.activated,
                    "reason": h.reason,
                }
                for h in syn.hits
            ],
        },
        "talent_penalty": {
            "sample_size": pen.sample_size,
            "min_sample_met": pen.min_sample_met,
            "hhi_value": pen.hhi_value,
            "penalty_factor": pen.penalty_factor,
            "function_counts": pen.function_counts,
        },
        "vr": {
            "vr_score": vr,
            "dimension_breakdown": vr_breakdown,
        },
        "composite": {
            "base_vr_plus_synergy": float(vr + syn.synergy_bonus),
            "penalty_factor": pen.penalty_factor,
            "composite_score": comp.composite_score,
            "score_band": comp.score_band,
        },
        "generated_at_utc": _now_ts(),
    }

    # Store penalty as "magnitude" (0.0 means no penalty)
    penalty_magnitude = float(1.0 - pen.penalty_factor)

    upsert_org_air_score(
        cur,
        company_id=company_id,
        assessment_id=assessment_id,
        scoring_run_id=run_id,
        vr_score=vr,
        synergy_bonus=syn.synergy_bonus,
        talent_penalty=penalty_magnitude,
        sem_lower=None,
        sem_upper=None,
        composite_score=comp.composite_score,
        score_band=comp.score_band,
        breakdown_json=breakdown_json,
    )

    audit_log(
        cur,
        run_id,
        company_id,
        "final_write",
        {"target_table": "org_air_scores"},
        {"status": "upserted", "composite_score": comp.composite_score, "score_band": comp.score_band},
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--company-id", required=True)
    parser.add_argument("--version", default="v1.0")
    parser.add_argument("--model-version", default="cs3-scoring-v1")
    args = parser.parse_args()

    conn = get_snowflake_connection()
    cur = conn.cursor()
    run_id = None

    try:
        run_id = insert_scoring_run(
            cur,
            companies_scored=[args.company_id],
            model_version=args.model_version,
            params={"version": args.version},
        )

        score_one_company(cur, company_id=args.company_id, version=args.version, run_id=run_id)

        update_scoring_run_status(cur, run_id, "success")
        conn.commit()

        print("✅ Scoring run completed")
        print(f"run_id: {run_id}")
        return 0

    except Exception:
        if run_id:
            try:
                update_scoring_run_status(cur, run_id, "failed")
                conn.commit()
            except Exception:
                pass
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
