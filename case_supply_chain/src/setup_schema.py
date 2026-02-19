import pathlib
from sqlalchemy import text
from common.db_config import engine

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
SCHEMA_FILE = BASE_DIR / "sql" / "schema.sql"

def run_schema():
    with engine.begin() as conn:
        sql = SCHEMA_FILE.read_text()
        conn.execute(text(sql))

if __name__ == "__main__":
    run_schema()
    print("Schema applied successfully.")