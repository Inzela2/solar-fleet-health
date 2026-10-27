import io
from pathlib import Path
import psycopg
import pandas as pd
from .config import DSN

SCHEMA = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"


def connect(dsn: str = DSN) -> psycopg.Connection:
    return psycopg.connect(dsn)


def init_schema(conn) -> None:
    conn.execute(SCHEMA.read_text())
    conn.commit()


def copy_frame(conn, table: str, df: pd.DataFrame, columns: list[str]) -> None:
    """Bulk-load with COPY, far faster than row-by-row inserts for millions of readings."""
    buf = io.StringIO()
    df[columns].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with conn.cursor().copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
        while chunk := buf.read(1 << 20):
            cp.write(chunk)
