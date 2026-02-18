from app.config import settings
 
_snowflake_import_error: Exception | None = None
try:
    import snowflake.connector as snowflake_connector
except Exception as exc:
    snowflake_connector = None
    _snowflake_import_error = exc
 
 
def get_snowflake_connection():
    if snowflake_connector is None:
        raise RuntimeError("snowflake-connector-python is not installed or failed to import") from _snowflake_import_error

    if not settings.snowflake_account or not settings.snowflake_user or not settings.snowflake_password:
        raise RuntimeError("Snowflake credentials missing (SNOWFLAKE_ACCOUNT/USER/PASSWORD)")
 
    return snowflake_connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
        role=settings.snowflake_role,
    )
 
 
def ping_snowflake() -> tuple[bool, str]:
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            cur.fetchone()
            return True, "ok"
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
