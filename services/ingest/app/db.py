import json
import math
import time
from typing import List, Tuple
import psycopg2
import psycopg2.extras

from .config import DB_HOST, DB_NAME, DB_PORT, DB_USER, DB_PASSWORD, HISTORY_DATA_URL
from .transform import normalize_record, remap_metric_name


def _is_nullish(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        import pandas as pd

        if value is pd.NA or value is pd.NaT:
            return True
    except Exception:
        pass
    return False


def _sanitize_meta(value):
    if _is_nullish(value):
        return None
    if isinstance(value, dict):
        return {k: _sanitize_meta(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_meta(v) for v in value]
    return value

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def check_db_emptiness() -> bool:
    """
    Check if the measurements table is empty.
    Returns True if empty, False otherwise.
    """
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM measurements")
            count = cur.fetchone()[0]
            return count == 0   

def bootstrap_history() -> bool:
    """
    Download initial history and insert into DB if not already present.
    """
    import urllib.request
    import zipfile
    import io
    import pandas as pd
    # check if DB is empty
    if not check_db_emptiness():
        return False  # already has data
    
    
    # download temporary zip file, extract XLS, parse rows, insert into DB
    with urllib.request.urlopen(HISTORY_DATA_URL) as resp:
        zip_bytes = resp.read()
            
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        # find the first xls/xlsx file
        xls_filename = None
        for name in z.namelist():
            if name.lower().endswith(('.xls', '.xlsx')):
                xls_filename = name
                break
        if not xls_filename:
            raise ValueError("No XLS/XLSX file found in history zip")

        # read file bytes and pass to pandas
        content_bytes = z.read(xls_filename)
        # choose engine based on extension (.xls uses xlrd, .xlsx uses openpyxl)
        ext = xls_filename.lower().rsplit('.', 1)[-1]
        if ext == 'xls':
            # pandas requires the 'xlrd' package to read .xls files.
            # Ensure 'xlrd' is installed in the environment (requirements.txt may need updating).
            engine = 'xlrd'
        else:
            engine = 'openpyxl'
        try:
            df = pd.read_excel(io.BytesIO(content_bytes), engine=engine, index_col=False, header=0)
        except Exception:
            # Some providers label a text/TSV file with .xls extension.
            # Fall back to parsing as tab-separated text using latin-1 encoding.
            text = content_bytes.decode('latin-1')
            df = pd.read_csv(io.StringIO(text), sep='\t', index_col=False, header=0, low_memory=False)
            print(f"Bootstrap history - number of rows fetched: {len(df)}")

        # drop rows where "Nature" is null
        df = df[df['Nature'].notnull()]

        # normalize/rename columns using remap_metric_name
        new_columns = [remap_metric_name(col) for col in df.columns]
        df.columns = new_columns

        # remove rows where 
        # create date_heure column from date and heure columns if present
        if 'date' in df.columns and 'heure' in df.columns:
            df['date_heure'] = df.apply(lambda row: f"{row['date']}T{row['heure']}:00+00:00", axis=1)
            
    # iterate rows and normalize/insert
    print(f"Bootstrap history - normalizing rows...")
    all_rows: List[Tuple] = []
    for index, rec in df.iterrows():
        # drop null values to match previous behavior
        clean = {k: v for k, v in rec.items() if v is not None}
        rows = normalize_record(clean)
        all_rows.extend(rows)
    
    print(f"Bootstrap history - total normalized rows to insert: {len(all_rows)}")
    
    print(f"Bootstrap history - inserting rows into DB...")
    n = insert_measurements(all_rows, is_bootstrap=True)
    print(f"Bootstrap history - inserted {n} rows into DB.")
    return True
    
def insert_measurements(rows: List[Tuple], is_bootstrap: bool = False, retry_seconds: int = 1, max_retries: int = 5) -> int:
    """
    Batch insert rows into measurements.

    rows: list of (ts, source, metric, value, meta_dict)
    Returns number of rows inserted/upserted.
    """
    if not rows:
        return 0

    if is_bootstrap:
        insert_sql = "INSERT INTO measurements (ts, source, metric, value, meta) VALUES %s"
    else:
        insert_sql = (
            "INSERT INTO measurements (ts, source, metric, value, meta) VALUES %s "
            "ON CONFLICT (ts, source, metric) DO UPDATE SET value = EXCLUDED.value, meta = EXCLUDED.meta, updated_at = now()"
        )

    attempts = 0
    while True:
        try:
            conn = get_conn()
            with conn:
                with conn.cursor() as cur:
                    # prepare values with JSON serialization
                    def prepare(r):
                        ts, source, metric, value, meta = r
                        clean_value = None if _is_nullish(value) else value
                        clean_meta = _sanitize_meta(meta) if meta is not None else None
                        return (
                            ts,
                            source,
                            metric,
                            clean_value,
                            json.dumps(clean_meta) if clean_meta is not None else None,
                        )

                    vals = [prepare(r) for r in rows]
                    if is_bootstrap:
                        psycopg2.extras.execute_values(
                            cur, insert_sql, vals, template=None, page_size=100
                        )
                        return len(rows)

                    psycopg2.extras.execute_values(
                        cur, insert_sql, vals, template=None, page_size=100
                    )
                    return len(rows)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            attempts += 1
            if attempts >= max_retries:
                raise
            time.sleep(retry_seconds * attempts)
