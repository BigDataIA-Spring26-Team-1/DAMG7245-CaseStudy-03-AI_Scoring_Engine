from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.snowflake import get_snowflake_connection
from app.scoring_engine.sector_config import get_company_sector, load_sector_profile
from app.scoring_engine.vr_model import fetch_dimension_inputs, compute_vr_score


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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--company-id", required=True)
    parser.add_argument("--version", default="v1.0")
    args = parser.parse_args()

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        assessment_id = get_latest_assessment_id(cur, args.company_id)

        sector = get_company_sector(cur, args.company_id)
        profile = load_sector_profile(cur, sector, version=args.version)

        dims = fetch_dimension_inputs(cur, assessment_id)
        vr, breakdown = compute_vr_score(dims, profile.weights)

        print("\n==== VR RESULT ====")
        print(f"company_id:    {args.company_id}")
        print(f"assessment_id: {assessment_id}")
        print(f"sector:        {sector}")
        print(f"version:       {args.version}")
        print(f"VR (0-100):    {vr:.2f}")
        print("\n---- Dimension Breakdown ----")
        # Keep it readable in terminal:
        for b, d in zip(breakdown, dims):
            print(
                f"{d.dimension:18s} raw={b['raw_score']:6.2f} "
                f"w={b['sector_weight']:.3f} conf={b['confidence']:.2f} "
                f"used={b['confidence_used']:.2f} contrib={b['weighted_score']:.2f}"
            )
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
