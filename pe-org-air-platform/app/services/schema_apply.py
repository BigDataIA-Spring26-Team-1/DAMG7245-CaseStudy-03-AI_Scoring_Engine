from __future__ import annotations

import logging
from pathlib import Path

from app.services.snowflake import get_snowflake_connection

logger = logging.getLogger("uvicorn.error")


def _apply_uniqueness_constraints(cur) -> None:
    # Best-effort for existing environments created before constraints were added.
    constraints = [
        "ALTER TABLE companies ADD CONSTRAINT uq_companies_ticker UNIQUE (ticker)",
        "ALTER TABLE documents ADD CONSTRAINT uq_documents_content_hash UNIQUE (content_hash)",
        "ALTER TABLE company_signal_summaries ADD CONSTRAINT uq_company_signal_summary_company_day UNIQUE (company_id, as_of_date)",
    ]
    for stmt in constraints:
        try:
            cur.execute(stmt)
        except Exception as exc:
            # Already exists / duplicates present; leave as-is to avoid breaking schema apply.
            logger.warning("schema_constraint_apply_skipped stmt=%s err=%s", stmt, exc)
            continue


def _split_sql_statements(sql: str) -> list[str]:
    """
    Splits a SQL script into individual statements.
    Assumes statements are separated by semicolons.
    """
    statements = []
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            statements.append(stmt)
    return statements


def apply_schema() -> None:
    schema_path = Path(__file__).resolve().parents[1] / "database" / "schema.sql"

    sql = schema_path.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql)

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        for stmt in statements:
            cur.execute(stmt)
        _apply_uniqueness_constraints(cur)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    apply_schema()
    print("Snowflake schema applied successfully")
