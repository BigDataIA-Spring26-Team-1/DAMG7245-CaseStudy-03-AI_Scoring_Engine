from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.snowflake import get_snowflake_connection
 
 
def split_sql_statements(sql: str) -> list[str]:
    """
    Very simple SQL splitter: splits on semicolons.
    Works for our schema.sql because it contains plain DDL.
    """
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def strip_leading_line_comments(sql: str) -> str:
    lines = sql.splitlines()
    i = 0
    while i < len(lines) and lines[i].lstrip().startswith("--"):
        i += 1
    return "\n".join(lines[i:]).strip()


def apply_uniqueness_constraints(cur) -> int:
    # Best-effort for environments that already had these tables.
    constraints = [
        "ALTER TABLE companies ADD CONSTRAINT uq_companies_ticker UNIQUE (ticker)",
        "ALTER TABLE documents ADD CONSTRAINT uq_documents_content_hash UNIQUE (content_hash)",
        "ALTER TABLE company_signal_summaries ADD CONSTRAINT uq_company_signal_summary_company_day UNIQUE (company_id, as_of_date)",
    ]
    applied = 0
    for stmt in constraints:
        try:
            cur.execute(stmt)
            applied += 1
        except Exception:
            # Ignore if already present or blocked by historical duplicate rows.
            continue
    return applied
 
 
def main() -> int:
    schema_path = ROOT / "app" / "database" / "schema.sql"
    if not schema_path.exists():
        raise SystemExit(f"schema.sql not found at: {schema_path.resolve()}")
 
    sql_text = schema_path.read_text(encoding="utf-8", errors="ignore")
    statements = split_sql_statements(sql_text)
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        applied = 0
        for i, stmt in enumerate(statements, start=1):
            s = strip_leading_line_comments(stmt)
            if not s:
                continue
            try:
                cur.execute(s)
                applied += 1
            except Exception:
                print(f"\n Failed on statement #{i}:\n{s}\n")
                raise

        constraints_applied = apply_uniqueness_constraints(cur)
        if constraints_applied:
            print(f"Applied {constraints_applied} uniqueness constraint statement(s)")

        print(f"Applied {applied} SQL statements from {schema_path}")
        return 0
    finally:
        cur.close()
        conn.close()
 
 
if __name__ == "__main__":
    raise SystemExit(main())
